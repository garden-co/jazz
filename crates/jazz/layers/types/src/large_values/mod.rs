//! Canonical indirect representation for large logical scalar values.

#[cfg(any(test, feature = "testing"))]
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

mod json_syntax;
use json_syntax::StreamingJsonValidator;
use thiserror::Error;

pub mod chunks;
pub mod epoch;
mod inputs;
pub use inputs::{AttemptError, ChunkInputs, LoadedChunk};

use crate::records::{EnumCase, EnumSchema, EnumValue, RecordDescriptor, Value, ValueType};
use chunks::ChunkRequest;

const FORMAT_V1: u8 = 1;
pub const FORMAT_VERSION: u8 = FORMAT_V1;

/// The authoritative immutable-large-value codecs that this binary can read.
///
/// The descriptor selects one of these codecs before any descriptor-guided
/// traversal starts.  Keep an explicit case per persisted format: accepting a
/// later format through the v1 codec (or vice versa) would turn a format bump
/// into an accidental, lossy migration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LargeValueFormat {
    V1,
}

impl LargeValueFormat {
    fn from_version(version: u8) -> Result<Self, Error> {
        match version {
            FORMAT_V1 => Ok(Self::V1),
            _ => Err(Error::UnsupportedFormat(version)),
        }
    }

    fn version(self) -> u8 {
        match self {
            Self::V1 => FORMAT_V1,
        }
    }

    /// The descriptor, its nested edit records, and the schema-known stored
    /// scalar arm are one versioned physical contract.  Decode the descriptor
    /// version before interpreting any of those nested fields; a future codec
    /// gets an explicit implementation here rather than inheriting V1's
    /// tail/record assumptions.
    fn validate_descriptor_shape(self, value: &LargeValueRef) -> Result<(), Error> {
        match self {
            Self::V1 => validate_descriptor_shape_v1(value),
        }
    }

    fn validate_descriptor(self, value: &LargeValueRef) -> Result<(), Error> {
        match self {
            Self::V1 => validate_descriptor_v1(value),
        }
    }

    fn decode_descriptor_payload(
        self,
        kind: LargeValueKind,
        payload: &[u8],
    ) -> Result<LargeValueRef, Error> {
        match self {
            Self::V1 => decode_descriptor_payload_v1(kind, payload),
        }
    }

    fn encode_node(self, node: &ChunkNode) -> Result<Vec<u8>, Error> {
        match self {
            Self::V1 => encode_node_v1(node),
        }
    }

    fn decode_node(self, encoded: &[u8]) -> Result<ChunkNode, Error> {
        let node = match self {
            Self::V1 => decode_canonical_node_v1(encoded)?,
        };
        if node_format(&node) != self.version() {
            return Err(Error::UnsupportedFormat(node_format(&node)));
        }
        Ok(node)
    }
}
/// Logical scalar size above which ordinary writes use indirect storage.
pub const INLINE_VALUE_MAX_BYTES: usize = 64 * 1024;
/// Frozen V1 content-defined leaf profile. These values, masks, and the gear
/// mixer below are format data: changing any of them requires a new immutable
/// format, not a tuning-only release.
pub const V1_FASTCDC_LEAF_MIN_BYTES: usize = 16 * 1024;
pub const V1_FASTCDC_LEAF_TARGET_BYTES: usize = 64 * 1024;
pub const V1_FASTCDC_LEAF_MAX_BYTES: usize = 256 * 1024;
pub const V1_FASTCDC_LEAF_SHORT_MASK: u64 = 0x0000_0000_0001_ffff;
pub const V1_FASTCDC_LEAF_LONG_MASK: u64 = 0x0000_0000_0000_7fff;
/// Compatibility aliases for callers that need only the active writer
/// profile. Persisted descriptors always select the versioned constants.
pub const LEAF_MIN_BYTES: usize = V1_FASTCDC_LEAF_MIN_BYTES;
pub const LEAF_TARGET_BYTES: usize = V1_FASTCDC_LEAF_TARGET_BYTES;
pub const LEAF_MAX_BYTES: usize = V1_FASTCDC_LEAF_MAX_BYTES;
/// Hard allocation/CPU boundary for one encoded immutable node. Leaves dominate
/// the format; the additional envelope allowance is comfortably larger than a
/// maximum-fanout branch.
pub const MAX_ENCODED_NODE_BYTES: usize = LEAF_MAX_BYTES + 16 * 1024;
pub const V1_GROUPING_BRANCH_MIN_CHILDREN: usize = 4;
pub const V1_GROUPING_BRANCH_TARGET_CHILDREN: usize = 16;
pub const V1_GROUPING_BRANCH_MAX_CHILDREN: usize = 64;
pub const V1_GROUPING_BRANCH_SHORT_MASK: u64 = 0x0000_0000_0000_001f;
pub const V1_GROUPING_BRANCH_LONG_MASK: u64 = 0x0000_0000_0000_0007;
pub const BRANCH_MIN_CHILDREN: usize = V1_GROUPING_BRANCH_MIN_CHILDREN;
pub const BRANCH_TARGET_CHILDREN: usize = V1_GROUPING_BRANCH_TARGET_CHILDREN;
pub const BRANCH_MAX_CHILDREN: usize = V1_GROUPING_BRANCH_MAX_CHILDREN;
pub const MAX_TREE_DEPTH: usize = 32;
/// Maximum number of logical tree-edge occurrences one synchronous scalar
/// operation may expand. Physical graph walks deduplicate shared nodes, but a
/// shared node can occur at many logical positions. This bound prevents a
/// small authenticated shared DAG from turning one attempt into
/// disproportionate repeated traversal.
pub const MAX_LOGICAL_TRAVERSAL_STEPS: usize = 128 * 1024;
/// Maximum number of distinct immutable nodes a graph-oriented operation may
/// retain while authenticating one descriptor. This matches the logical-work
/// budget while still admitting multi-gigabyte canonical values: even at the
/// minimum leaf size, a normally shaped tree can represent well over 1 GiB.
///
/// Physical traversals must remember every discovered node to deduplicate DAG
/// edges and validate repeated references consistently, so this is also their
/// memory boundary.
pub const MAX_PHYSICAL_TRAVERSAL_NODES: usize = MAX_LOGICAL_TRAVERSAL_STEPS;
/// JSON syntax validation retains one frame per open array/object. Keep this
/// separate from the immutable-tree bound: a small logical value can otherwise
/// use arbitrarily deep JSON nesting to grow validator memory.
/// This is a safe memory ceiling, not a promise to admit every depth below it:
/// the default serde_json::Value parser admits only 127 simultaneous containers.
pub const MAX_JSON_NESTING_DEPTH: usize = 128;
/// The most edits a descriptor's tail holds: an epoch's (SPEC §19.3).
pub const MAX_EDIT_COUNT: usize = epoch::EPOCH_MAX_EDITS;
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ContentHash(pub [u8; 32]);

impl ContentHash {
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LargeValueKind {
    Bytes,
    String,
    Json,
}

/// Constructs the descriptor-only physical scalar type used by Jazz's storage
/// lowering. The resulting type is intentionally impossible to name or
/// construct through the public `records::ValueType` API.
///
/// This is an engine boundary rather than a schema feature: public schemas
/// must continue to use their logical `String`/`Bytes` types.
pub fn physical_storage_value_type(kind: LargeValueKind) -> ValueType {
    ValueType::stored_scalar(kind)
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeRef {
    pub object_hash: ContentHash,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeMetrics {
    pub byte_length: u64,
    pub utf16_length: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchChild {
    pub node_ref: NodeRef,
    pub metrics: NodeMetrics,
    /// The subtree's structural hash over its logical content.
    pub logical_hash: ContentHash,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChunkNode {
    Leaf {
        format: u8,
        kind: LargeValueKind,
        bytes: Vec<u8>,
    },
    Branch {
        format: u8,
        kind: LargeValueKind,
        children: Vec<BranchChild>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplaceEdit {
    pub offset: u64,
    pub delete_length: u64,
    pub insert_bytes: Vec<u8>,
    /// Text-coordinate form of the same replacement. These are zero for byte
    /// values and let UTF-16 reads map through the bounded tail without
    /// scanning the logical prefix to rediscover every edit coordinate.
    pub utf16_offset: u64,
    pub delete_utf16_length: u64,
    /// Derived from insert_bytes on decode; not stored in the edit record.
    pub insert_utf16_length: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LargeValueRef {
    pub kind: LargeValueKind,
    pub format_version: u8,
    pub logical_hash: ContentHash,
    pub root: NodeRef,
    pub byte_length: u64,
    pub utf16_length: Option<u64>,
    pub edit_tail: Vec<ReplaceEdit>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagedChunk {
    pub node_ref: NodeRef,
    pub encoded: Vec<u8>,
}

/// A read-only preparation returned by the chunk graph builder.
///
/// ```compile_fail
/// use jazz_types::large_values::{LargeValueRef, PreparedLargeValue, StagedChunk};
/// fn forge(value_ref: LargeValueRef, staged_chunks: Vec<StagedChunk>) {
///     let _ = PreparedLargeValue { value_ref, staged_chunks };
/// }
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct PreparedLargeValue {
    pub value_ref: LargeValueRef,
    pub staged_chunks: Vec<StagedChunk>,
}

/// Opaque identity for a persisted staging root.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StagedLargeValueId(pub [u8; 16]);

/// Incoming-upload accounting returned to Jazz for rate and eviction policy.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagedLargeValueAccounting {
    pub encoded_bytes: u64,
    pub node_count: u64,
}

/// Opaque staged root plus the descriptor a later authorized row write may
/// publish. Jazz never receives or manages the underlying chunk set.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagedLargeValue {
    pub id: StagedLargeValueId,
    pub value_ref: LargeValueRef,
    pub accounting: StagedLargeValueAccounting,
    /// Persisted mechanical creation time used by host staging policy.
    pub created_at_ms: u64,
}

/// Persisted metadata for a push upload that has not yet produced a root
/// receipt. Chunk identities stay storage-owned and are used only for safe
/// expiry/reclamation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingLargeValueUpload {
    pub id: StagedLargeValueId,
    /// The exact descriptor that this upload has been admitted to finalize.
    /// A failed or interrupted finalization may be retried only with this
    /// descriptor; chunk accounting is never a transferable publication
    /// capability.
    #[serde(default)]
    pub descriptor: Option<LargeValueRef>,
    /// Idempotency key for the receipt after finalization has been admitted.
    /// Kept with the descriptor so a crash between receipt registration and
    /// pending-upload release cannot double-count root references on retry.
    #[serde(default)]
    pub receipt_id: Option<StagedLargeValueId>,
    pub accounting: StagedLargeValueAccounting,
    pub created_at_ms: u64,
    pub chunks: Vec<NodeRef>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LargeValueUploadProgress {
    Missing(Vec<NodeRef>),
    Staged(StagedLargeValue),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StreamingPrepareStats {
    pub input_bytes: u64,
    pub peak_leaf_buffer_bytes: usize,
    pub peak_frontier_nodes: usize,
    pub staged_chunk_count: u64,
}

#[derive(Clone, Debug)]
pub struct LargeValueCursor {
    value: LargeValueRef,
    offset: u64,
    window_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StreamingExecutionStats {
    pub logical_bytes_consumed: u64,
    pub windows_consumed: u64,
    pub cooperative_yields: u64,
}

/// Resumable fixed-memory state for the reference streaming operator.
///
/// The state owns the exact source descriptor, so resuming it against a
/// replacement value is impossible. A caller may publish `finish()` only after
/// every logical window has been consumed.
#[derive(Clone, Debug)]
pub struct StreamingChecksum {
    cursor: LargeValueCursor,
    hasher: blake3::Hasher,
    max_bytes_per_turn: usize,
    bytes_this_turn: usize,
    stats: StreamingExecutionStats,
}

impl StreamingChecksum {
    pub fn new(
        value: LargeValueRef,
        window_bytes: usize,
        max_bytes_per_turn: usize,
    ) -> Result<Self, Error> {
        if max_bytes_per_turn == 0 {
            return Err(Error::MalformedScalar);
        }
        Ok(Self {
            cursor: LargeValueCursor::new(value, window_bytes.min(max_bytes_per_turn))?,
            hasher: blake3::Hasher::new(),
            max_bytes_per_turn,
            bytes_this_turn: 0,
            stats: StreamingExecutionStats::default(),
        })
    }

    pub fn cursor(&self) -> &LargeValueCursor {
        &self.cursor
    }

    pub fn stats(&self) -> StreamingExecutionStats {
        self.stats
    }

    /// Commit one complete cursor window to private operator state. Returns
    /// `true` when the caller must cooperatively yield before consuming more.
    pub fn consume_window(&mut self, bytes: &[u8]) -> Result<bool, Error> {
        let Some(range) = self.cursor.next_range() else {
            return Err(Error::MalformedScalar);
        };
        if u64::try_from(bytes.len()).map_err(|_| Error::MetricOverflow)? != range.end - range.start
        {
            return Err(Error::MalformedScalar);
        }
        self.hasher.update(bytes);
        self.cursor.advance_to(range.end);
        self.stats.logical_bytes_consumed = self
            .stats
            .logical_bytes_consumed
            .checked_add(u64::try_from(bytes.len()).map_err(|_| Error::MetricOverflow)?)
            .ok_or(Error::MetricOverflow)?;
        self.stats.windows_consumed = self
            .stats
            .windows_consumed
            .checked_add(1)
            .ok_or(Error::MetricOverflow)?;
        self.bytes_this_turn = self
            .bytes_this_turn
            .checked_add(bytes.len())
            .ok_or(Error::MetricOverflow)?;
        Ok(self.bytes_this_turn >= self.max_bytes_per_turn && self.cursor.remaining_bytes() != 0)
    }

    pub fn record_yield(&mut self) -> Result<(), Error> {
        self.stats.cooperative_yields = self
            .stats
            .cooperative_yields
            .checked_add(1)
            .ok_or(Error::MetricOverflow)?;
        self.bytes_this_turn = 0;
        Ok(())
    }

    pub fn finish(self) -> Result<(ContentHash, StreamingExecutionStats), Error> {
        if self.cursor.remaining_bytes() != 0 {
            return Err(Error::RequiresEvaluation);
        }
        Ok((ContentHash(*self.hasher.finalize().as_bytes()), self.stats))
    }
}

impl LargeValueCursor {
    pub fn new(value: LargeValueRef, window_bytes: usize) -> Result<Self, Error> {
        validate_descriptor(&value)?;
        let window_bytes = u64::try_from(window_bytes).map_err(|_| Error::MetricOverflow)?;
        if window_bytes == 0 {
            return Err(Error::MalformedScalar);
        }
        Ok(Self {
            value,
            offset: 0,
            window_bytes,
        })
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }
    pub fn remaining_bytes(&self) -> u64 {
        self.value.byte_length - self.offset
    }
    pub fn value(&self) -> &LargeValueRef {
        &self.value
    }

    pub fn next_range(&self) -> Option<std::ops::Range<u64>> {
        if self.offset == self.value.byte_length {
            return None;
        }
        Some(
            self.offset
                ..self
                    .offset
                    .saturating_add(self.window_bytes)
                    .min(self.value.byte_length),
        )
    }

    pub fn advance_to(&mut self, offset: u64) {
        self.offset = offset;
    }
}

/// Incremental write-admission validation for an already-chunked logical
/// value. The caller feeds consecutive final-logical windows, so this retains
/// only an incomplete UTF-8 sequence and bounded JSON semantic state, never the value.
pub struct LogicalValueValidator {
    kind: LargeValueKind,
    utf8_tail: Vec<u8>,
    utf16_length: u64,
    json: Option<StreamingJsonValidator>,
}

/// Validate every supplied immutable node before an upload batch reaches a
/// durable chunk backend. Upload batches are protocol-bounded, so retaining
/// these small identity sets does not change the streaming memory bound.
pub fn validate_staged_chunk_batch(
    kind: LargeValueKind,
    chunks: &[StagedChunk],
) -> Result<(), Error> {
    for chunk in chunks {
        // `decode_node` also checks the encoded-size and object-hash bounds.
        // Validate the full semantic envelope here, rather than discovering a
        // malformed later member after an earlier one is already durable.
        decode_node(kind, chunk.node_ref.object_hash, &chunk.encoded)?;
    }
    Ok(())
}

impl LogicalValueValidator {
    pub fn new(value: &LargeValueRef) -> Result<Self, Error> {
        validate_descriptor(value)?;
        Ok(Self {
            kind: value.kind,
            utf8_tail: Vec::new(),
            utf16_length: 0,
            json: (value.kind == LargeValueKind::Json).then(StreamingJsonValidator::new),
        })
    }

    pub fn push(&mut self, bytes: &[u8]) -> Result<(), Error> {
        if self.kind == LargeValueKind::Bytes {
            return Ok(());
        }

        self.utf8_tail.extend_from_slice(bytes);
        match std::str::from_utf8(&self.utf8_tail) {
            Ok(text) => {
                self.utf16_length = self
                    .utf16_length
                    .checked_add(utf16_length_of(text)?)
                    .ok_or(Error::MetricOverflow)?;
                if let Some(json) = &mut self.json {
                    json.push(text.as_bytes())
                        .map_err(|()| Error::InvalidJson)?;
                }
                self.utf8_tail.clear();
            }
            Err(error) => {
                let valid = error.valid_up_to();
                if error.error_len().is_some() {
                    return Err(Error::InvalidUtf8);
                }
                let valid_text = std::str::from_utf8(&self.utf8_tail[..valid])
                    .expect("UTF-8 parser reported a valid prefix");
                self.utf16_length = self
                    .utf16_length
                    .checked_add(utf16_length_of(valid_text)?)
                    .ok_or(Error::MetricOverflow)?;
                if let Some(json) = &mut self.json {
                    json.push(valid_text.as_bytes())
                        .map_err(|()| Error::InvalidJson)?;
                }
                self.utf8_tail.drain(..valid);
            }
        }
        Ok(())
    }

    pub fn finish(self, value: &LargeValueRef) -> Result<(), Error> {
        if !self.utf8_tail.is_empty() {
            return Err(Error::InvalidUtf8);
        }
        if let Some(json) = self.json {
            json.finish().map_err(|()| Error::InvalidJson)?;
        }
        if value.kind != LargeValueKind::Bytes && value.utf16_length != Some(self.utf16_length) {
            return Err(Error::DescriptorMismatch);
        }
        Ok(())
    }
}

/// Construct the canonical tree from a reader without retaining the logical
/// value or emitted chunks. `stage` receives immutable nodes as soon as their
/// content boundary is final. This is a pure construction adapter: if `stage`
/// persists anything, its caller owns rollback or expiring retention. Database
/// APIs use the persisted pending-upload lifecycle instead of this callback.
/// A failed text/JSON validation never returns a publishable descriptor.
pub fn prepare_streaming<R: std::io::Read>(
    kind: LargeValueKind,
    mut reader: R,
    stage: impl FnMut(StagedChunk) -> Result<(), Error>,
) -> Result<(LargeValueRef, StreamingPrepareStats), Error> {
    let mut builder = PushStreamingPreparation::new(kind, stage);
    let mut read_buffer = vec![0_u8; LEAF_MIN_BYTES];
    loop {
        let count = reader
            .read(&mut read_buffer)
            .map_err(|_| Error::MalformedScalar)?;
        if count == 0 {
            break;
        }
        builder.push(&read_buffer[..count])?;
    }
    builder.finish()
}

/// Resumable, bounded-memory construction of a canonical large-value tree.
/// Hosts may persist chunks from `stage` between calls to [`Self::push`].
pub struct PushStreamingPreparation<S>
where
    S: FnMut(StagedChunk) -> Result<(), Error>,
{
    builder: StreamingTreeBuilder<S>,
    json: Option<StreamingJsonValidator>,
}

impl<S> PushStreamingPreparation<S>
where
    S: FnMut(StagedChunk) -> Result<(), Error>,
{
    pub fn new(kind: LargeValueKind, stage: S) -> Self {
        Self {
            builder: StreamingTreeBuilder::new(kind, stage),
            json: (kind == LargeValueKind::Json).then(StreamingJsonValidator::new),
        }
    }

    pub fn push(&mut self, bytes: &[u8]) -> Result<(), Error> {
        if let Some(json) = &mut self.json {
            json.push(bytes).map_err(|()| Error::InvalidJson)?;
        }
        self.builder.feed(bytes)
    }

    pub fn finish(self) -> Result<(LargeValueRef, StreamingPrepareStats), Error> {
        if let Some(json) = self.json {
            json.finish().map_err(|()| Error::InvalidJson)?;
        }
        self.builder.finish()
    }
}

struct StreamingLevel {
    pending: Vec<BuiltNode>,
    hash: u64,
    total: usize,
}

struct StreamingTreeBuilder<S> {
    kind: LargeValueKind,
    stage: S,
    leaf: Vec<u8>,
    leaf_scan: usize,
    leaf_hash: u64,
    levels: Vec<StreamingLevel>,
    stats: StreamingPrepareStats,
}

impl<S> StreamingTreeBuilder<S>
where
    S: FnMut(StagedChunk) -> Result<(), Error>,
{
    fn new(kind: LargeValueKind, stage: S) -> Self {
        Self {
            kind,
            stage,
            leaf: Vec::new(),
            leaf_scan: 0,
            leaf_hash: 0,
            levels: Vec::new(),
            stats: StreamingPrepareStats::default(),
        }
    }

    fn feed(&mut self, bytes: &[u8]) -> Result<(), Error> {
        self.stats.input_bytes = self
            .stats
            .input_bytes
            .checked_add(bytes.len() as u64)
            .ok_or(Error::MetricOverflow)?;
        self.leaf.extend_from_slice(bytes);
        self.stats.peak_leaf_buffer_bytes = self.stats.peak_leaf_buffer_bytes.max(self.leaf.len());
        loop {
            let mut boundary = None;
            while self.leaf_scan < self.leaf.len().min(LEAF_MAX_BYTES) {
                self.leaf_hash = roll(self.leaf_hash, self.leaf[self.leaf_scan]);
                self.leaf_scan += 1;
                let length = self.leaf_scan;
                if leaf_cut(self.leaf_hash, length) || length == LEAF_MAX_BYTES {
                    boundary = Some(length);
                    break;
                }
            }
            let Some(mut end) = boundary else {
                break;
            };
            if self.kind != LargeValueKind::Bytes {
                match std::str::from_utf8(&self.leaf[..end]) {
                    Ok(_) => {}
                    Err(error) if error.error_len().is_none() => end = error.valid_up_to(),
                    Err(_) => return Err(Error::InvalidUtf8),
                }
                if end == 0 {
                    break;
                }
            }
            let leaf = self.leaf.drain(..end).collect::<Vec<_>>();
            self.leaf_scan = 0;
            self.leaf_hash = 0;
            self.emit_leaf(leaf)?;
        }
        Ok(())
    }

    fn emit_leaf(&mut self, bytes: Vec<u8>) -> Result<(), Error> {
        let metrics = metrics(self.kind, &bytes)?;
        let node = self.stage_node_to(leaf_node(self.kind, bytes), metrics)?;
        self.add_node(0, node)
    }

    fn stage_node_to(&mut self, node: ChunkNode, metrics: NodeMetrics) -> Result<BuiltNode, Error> {
        if node_kind(&node) != self.kind {
            return Err(Error::DescriptorMismatch);
        }
        if node_metrics(self.kind, &node)? != metrics {
            return Err(Error::DescriptorMismatch);
        }
        let (built, chunk) = seal_node(&node, metrics, |_| true)?;
        if let Some(chunk) = chunk {
            (self.stage)(chunk)?;
            self.stats.staged_chunk_count = self
                .stats
                .staged_chunk_count
                .checked_add(1)
                .ok_or(Error::MetricOverflow)?;
        }
        Ok(built)
    }

    fn add_node(&mut self, mut level: usize, mut node: BuiltNode) -> Result<(), Error> {
        loop {
            if level >= MAX_TREE_DEPTH {
                return Err(Error::MalformedNode);
            }
            while self.levels.len() <= level {
                self.levels.push(StreamingLevel {
                    pending: Vec::new(),
                    hash: 0,
                    total: 0,
                });
            }
            let (count, boundary) = {
                let state = &mut self.levels[level];
                state.hash = roll_child(state.hash, self.kind, &node);
                state.pending.push(node);
                state.total += 1;
                let count = state.pending.len();
                (count, branch_cut(state.hash, count))
            };
            self.stats.peak_frontier_nodes = self
                .stats
                .peak_frontier_nodes
                .max(self.levels.iter().map(|level| level.pending.len()).sum());
            if !boundary && count < BRANCH_MAX_CHILDREN {
                return Ok(());
            }
            node = self.flush_group(level)?;
            level += 1;
        }
    }

    fn flush_group(&mut self, level: usize) -> Result<BuiltNode, Error> {
        let group = std::mem::take(&mut self.levels[level].pending);
        self.levels[level].hash = 0;
        let (chunk_node, metrics) = branch_over(self.kind, &group)?;
        self.stage_node_to(chunk_node, metrics)
    }

    fn finish(mut self) -> Result<(LargeValueRef, StreamingPrepareStats), Error> {
        if !self.leaf.is_empty() || self.levels.is_empty() {
            let leaf = std::mem::take(&mut self.leaf);
            self.emit_leaf(leaf)?;
        }
        let mut level = 0;
        loop {
            if level >= self.levels.len() {
                return Err(Error::MalformedNode);
            }
            if self.levels[level].total == 1 {
                let root = self.levels[level]
                    .pending
                    .pop()
                    .ok_or(Error::MalformedNode)?;
                if self.kind == LargeValueKind::Json {
                    // JSON is validated on writes. Streaming JSON validation is
                    // supplied below by the reader-facing facade; this builder
                    // never marks an unvalidated descriptor publishable there.
                }
                return Ok((root.into_value_ref(self.kind), self.stats));
            }
            if !self.levels[level].pending.is_empty() {
                let node = self.flush_group(level)?;
                self.add_node(level + 1, node)?;
            }
            level += 1;
        }
    }
}

/// Bounded private state for replaying a tail into successive localized tree
/// splices. Completed splices remain resident across request suspension.
#[derive(Clone, Debug)]
pub struct ConsolidationContinuation {
    source: LargeValueRef,
    current: Option<LargeValueRef>,
    next_edit: usize,
    staged_chunks: Vec<StagedChunk>,
}

impl ConsolidationContinuation {
    pub fn new(source: LargeValueRef) -> Result<Self, Error> {
        validate_descriptor_shape(&source)?;
        Ok(Self {
            source,
            current: None,
            next_edit: 0,
            staged_chunks: Vec::new(),
        })
    }

    pub fn step(
        &mut self,
        inputs: &mut ChunkInputs,
    ) -> Result<Option<PreparedLargeValue>, AttemptError> {
        if self.current.is_none() {
            let node = load_authenticated_node_attempt(
                self.source.format_version,
                self.source.kind,
                &self.source.root,
                self.source.logical_hash,
                inputs,
            )?;
            let metrics = node_metrics(self.source.kind, &node)?;
            self.current = Some(LargeValueRef {
                kind: self.source.kind,
                format_version: self.source.format_version,
                logical_hash: self.source.logical_hash,
                root: self.source.root.clone(),
                byte_length: metrics.byte_length,
                utf16_length: metrics.utf16_length,
                edit_tail: Vec::new(),
            });
        }
        while self.next_edit < self.source.edit_tail.len() {
            let edit = self.source.edit_tail[self.next_edit].clone();
            let current = self.current.as_ref().expect("initialized current base");
            let TailEditOutcome::Updated(with_tail) = replace_tail_with_bounds_attempt(
                current,
                edit.offset,
                edit.delete_length,
                edit.insert_bytes,
                inputs,
                false,
            )?
            else {
                return Err(Error::MalformedScalar.into());
            };
            let prepared = consolidate_single_edit_attempt(&with_tail, inputs)?;
            for chunk in &prepared.staged_chunks {
                inputs.install_chunk(
                    ChunkRequest {
                        object_hash: chunk.node_ref.object_hash.0,
                    },
                    bytes::Bytes::copy_from_slice(&chunk.encoded),
                );
            }
            self.staged_chunks.extend(prepared.staged_chunks);
            self.current = Some(prepared.value_ref);
            self.next_edit += 1;
        }
        let current = self.current.clone().ok_or(Error::MalformedScalar)?;
        if current.byte_length != self.source.byte_length
            || current.utf16_length != self.source.utf16_length
        {
            return Err(Error::DescriptorMismatch.into());
        }
        Ok(Some(PreparedLargeValue {
            value_ref: current,
            staged_chunks: std::mem::take(&mut self.staged_chunks),
        }))
    }
}

/// A descriptor with one more tail edit, and whether that tail has reached
/// its bounds so the value must be consolidated before it grows further.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TailOutcome {
    Updated(LargeValueRef),
    ConsolidationRequired(LargeValueRef),
}

pub type TailAppendOutcome = TailOutcome;
pub type TailEditOutcome = TailOutcome;

/// Append `edit` to `updated`, the edited copy of `value`. Consolidation is
/// required once the tail no longer fits one epoch (SPEC §19.3): too many
/// edits, too many encoded bytes, or an insertion over `EPOCH_INSERT_BYTES`.
fn push_tail_edit(
    value: &LargeValueRef,
    mut updated: LargeValueRef,
    edit: ReplaceEdit,
    enforce_tail_bounds: bool,
) -> Result<TailOutcome, Error> {
    updated.edit_tail.push(edit);
    let fits = !enforce_tail_bounds || tail_fits_epoch(&updated)?;
    debug_assert!(value.edit_tail.len() + 1 == updated.edit_tail.len());
    Ok(if fits {
        TailOutcome::Updated(updated)
    } else {
        TailOutcome::ConsolidationRequired(updated)
    })
}

/// The byte and UTF-16 lengths of `value`'s tree: its tail undone.
pub fn tail_base_lengths(value: &LargeValueRef) -> Result<(u64, Option<u64>), Error> {
    let mut byte_length = value.byte_length;
    let mut utf16_length = value.utf16_length;
    for edit in value.edit_tail.iter().rev() {
        let inserted = u64::try_from(edit.insert_bytes.len()).map_err(|_| Error::MetricOverflow)?;
        byte_length = byte_length
            .checked_sub(inserted)
            .and_then(|length| length.checked_add(edit.delete_length))
            .ok_or(Error::MalformedScalar)?;
        if let Some(length) = &mut utf16_length {
            *length = length
                .checked_sub(edit.insert_utf16_length)
                .and_then(|length| length.checked_add(edit.delete_utf16_length))
                .ok_or(Error::MalformedScalar)?;
        }
    }
    Ok((byte_length, utf16_length))
}

/// Whether `value`'s tail fits one epoch.
pub fn tail_fits_epoch(value: &LargeValueRef) -> Result<bool, Error> {
    let (length, utf16_length) = tail_base_lengths(value)?;
    epoch::fits_epoch(value.kind, length, utf16_length, &value.edit_tail)
}

fn utf16_length_of(text: &str) -> Result<u64, Error> {
    u64::try_from(text.encode_utf16().count()).map_err(|_| Error::MetricOverflow)
}

/// Physical arm stored inside a logical bytes/string/JSON cell.
///
/// This is deliberately an engine-owned *normal record enum*: `Primitive`
/// carries the declared primitive in a raw backing field and `Chunked` carries
/// its descriptor and tail as ordinary records, arrays, and primitives. The
/// raw backing fields terminate the envelope recursion; they are not public
/// schema or operator types. Their shape is parameterized by the immutable
/// declared column kind. Independently addressed tree nodes authenticate that
/// kind themselves; the containing scalar does not duplicate it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoredScalar {
    Primitive(Vec<u8>),
    Chunked(LargeValueRef),
}

/// Append bytes without reading the immutable base tree. The descriptor's
/// logical hash identifies that base; the canonical tail is the remaining
/// part of final logical identity until consolidation produces a new base.
pub fn append_tail(
    value: &LargeValueRef,
    insert_bytes: Vec<u8>,
) -> Result<TailAppendOutcome, Error> {
    validate_descriptor(value)?;
    match value.kind {
        LargeValueKind::Bytes => {}
        LargeValueKind::String => {
            std::str::from_utf8(&insert_bytes).map_err(|_| Error::InvalidUtf8)?;
        }
        // Appending a fragment cannot validate the resulting JSON without
        // inspecting existing source. JSON mutations use replacement
        // preparation, which may suspend on the affected source ranges.
        LargeValueKind::Json => return Err(Error::InvalidJson),
    }
    let inserted_length = u64::try_from(insert_bytes.len()).map_err(|_| Error::MetricOverflow)?;
    let mut updated = value.clone();
    updated.byte_length = updated
        .byte_length
        .checked_add(inserted_length)
        .ok_or(Error::MetricOverflow)?;
    if let Some(utf16_length) = &mut updated.utf16_length {
        let inserted_utf16 =
            utf16_length_of(std::str::from_utf8(&insert_bytes).map_err(|_| Error::InvalidUtf8)?)?;
        *utf16_length = utf16_length
            .checked_add(inserted_utf16)
            .ok_or(Error::MetricOverflow)?;
    }
    let edit = ReplaceEdit {
        offset: value.byte_length,
        delete_length: 0,
        insert_bytes,
        utf16_offset: value.utf16_length.unwrap_or(0),
        delete_utf16_length: 0,
        insert_utf16_length: updated
            .utf16_length
            .zip(value.utf16_length)
            .and_then(|(next, previous)| next.checked_sub(previous))
            .unwrap_or(0),
    };
    push_tail_edit(value, updated, edit, true)
}

/// Add one canonical replacement to the bounded tail. Bytes edits need no
/// source reads. Text edits request the deleted logical range so UTF-16
/// metrics remain exact; missing chunks suspend through the ordinary evaluator
/// request set. JSON currently admits only complete validated replacement.
pub fn replace_tail_attempt(
    value: &LargeValueRef,
    offset: u64,
    delete_length: u64,
    insert_bytes: Vec<u8>,
    inputs: &mut ChunkInputs,
) -> Result<TailEditOutcome, AttemptError> {
    replace_tail_with_bounds_attempt(value, offset, delete_length, insert_bytes, inputs, true)
}

fn replace_tail_with_bounds_attempt(
    value: &LargeValueRef,
    offset: u64,
    delete_length: u64,
    insert_bytes: Vec<u8>,
    inputs: &mut ChunkInputs,
    enforce_tail_bounds: bool,
) -> Result<TailEditOutcome, AttemptError> {
    validate_descriptor(value)?;
    let end = offset
        .checked_add(delete_length)
        .ok_or(Error::MetricOverflow)?;
    if end > value.byte_length {
        return Err(Error::MalformedScalar.into());
    }
    let inserted_length = u64::try_from(insert_bytes.len()).map_err(|_| Error::MetricOverflow)?;
    let next_length = value
        .byte_length
        .checked_sub(delete_length)
        .and_then(|length| length.checked_add(inserted_length))
        .ok_or(Error::MetricOverflow)?;
    let (next_utf16, utf16_offset, delete_utf16_length, insert_utf16_length) = match value.kind {
        LargeValueKind::Bytes => (None, 0, 0, 0),
        LargeValueKind::String => {
            let inserted = std::str::from_utf8(&insert_bytes).map_err(|_| Error::InvalidUtf8)?;
            for boundary in [offset, end] {
                if boundary < value.byte_length {
                    let byte = byte_range_attempt(value, boundary..boundary + 1, inputs)?;
                    if byte
                        .first()
                        .is_some_and(|byte| byte & 0b1100_0000 == 0b1000_0000)
                    {
                        return Err(Error::InvalidUtf8.into());
                    }
                }
            }
            let deleted = byte_range_attempt(value, offset..end, inputs)?;
            let deleted = std::str::from_utf8(&deleted).map_err(|_| Error::InvalidUtf8)?;
            let utf16_offset = utf16_length_for_byte_range_attempt(value, 0..offset, inputs)?;
            let delete_utf16_length = utf16_length_of(deleted)?;
            let insert_utf16_length = utf16_length_of(inserted)?;
            let current = value.utf16_length.ok_or(Error::MalformedScalar)?;
            (
                Some(
                    current
                        .checked_sub(delete_utf16_length)
                        .and_then(|length| length.checked_add(insert_utf16_length))
                        .ok_or(Error::MetricOverflow)?,
                ),
                utf16_offset,
                delete_utf16_length,
                insert_utf16_length,
            )
        }
        LargeValueKind::Json => {
            if offset != 0 || delete_length != value.byte_length {
                return Err(Error::InvalidJson.into());
            }
            validate_logical(LargeValueKind::Json, &insert_bytes)?;
            let insert_utf16_length = utf16_length_of(
                std::str::from_utf8(&insert_bytes).map_err(|_| Error::InvalidUtf8)?,
            )?;
            (
                Some(insert_utf16_length),
                0,
                value.utf16_length.ok_or(Error::MalformedScalar)?,
                insert_utf16_length,
            )
        }
    };
    let mut updated = value.clone();
    updated.byte_length = next_length;
    updated.utf16_length = next_utf16;
    let edit = ReplaceEdit {
        offset,
        delete_length,
        insert_bytes,
        utf16_offset,
        delete_utf16_length,
        insert_utf16_length,
    };
    Ok(push_tail_edit(value, updated, edit, enforce_tail_bounds)?)
}

/// Consolidate an append-only tail by walking only the old tree's right
/// spine. At each level the final content-defined group is rebuilt; sibling
/// subtrees outside that group retain their exact authenticated `NodeRef`.
pub fn consolidate_appends_attempt(
    value: &LargeValueRef,
    inputs: &mut ChunkInputs,
) -> Result<PreparedLargeValue, AttemptError> {
    validate_descriptor_shape(value)?;
    let base_byte_length = base_length(value.byte_length, &value.edit_tail, RangeUnit::Bytes)?;
    let mut cursor = base_byte_length;
    let mut appended = Vec::new();
    for edit in &value.edit_tail {
        if edit.delete_length != 0 || edit.offset != cursor {
            return Err(Error::MalformedScalar.into());
        }
        cursor = cursor
            .checked_add(u64::try_from(edit.insert_bytes.len()).map_err(|_| Error::MetricOverflow)?)
            .ok_or(Error::MetricOverflow)?;
        appended.extend_from_slice(&edit.insert_bytes);
    }
    if cursor != value.byte_length {
        return Err(Error::DescriptorMismatch.into());
    }

    let root = (value.root.clone(), value.logical_hash);
    let last_leaf = descend_attempt(
        value,
        root,
        Vec::new(),
        MAX_TREE_DEPTH,
        inputs,
        |children| children.len().checked_sub(1).ok_or(Error::MalformedNode),
    )?;
    let mut existing = std::collections::BTreeSet::new();
    for node_ref in last_leaf.path.iter().map(|frame| &frame.node_ref) {
        existing.insert(node_ref.object_hash);
    }
    existing.insert(last_leaf.node_ref.object_hash);
    let depth = last_leaf.path.len();

    let mut logical_suffix = last_leaf.bytes;
    logical_suffix.extend_from_slice(&appended);
    let mut stager = reusing_stager(value.kind, &existing);
    let mut replacement = stager.leaves(&logical_suffix)?;

    for frame in last_leaf.path.into_iter().rev() {
        let mut level = frame.children;
        level.pop();
        let mut rebuilt = level.into_iter().map(BuiltNode::from).collect::<Vec<_>>();
        rebuilt.append(&mut replacement);
        replacement = stager.branches(&rebuilt)?;
    }
    finish_rebuilt_tree(value, replacement, depth, stager)
}

/// Consolidate one replacement against an otherwise tail-free base. The
/// algorithm walks to the first affected leaf, advances until FastCDC
/// boundaries resynchronize, and rebuilds only the zipper between those two
/// leaves. This is the general middle-of-tree locality primitive; batching
/// several tail entries applies the same splice from right to left.
pub fn consolidate_single_edit_attempt(
    value: &LargeValueRef,
    inputs: &mut ChunkInputs,
) -> Result<PreparedLargeValue, AttemptError> {
    validate_descriptor_shape(value)?;
    let [edit] = value.edit_tail.as_slice() else {
        return Err(Error::MalformedScalar.into());
    };
    let base_len = base_length(value.byte_length, &value.edit_tail, RangeUnit::Bytes)?;
    let edit_end = edit
        .offset
        .checked_add(edit.delete_length)
        .ok_or(Error::MetricOverflow)?;
    if edit_end > base_len {
        return Err(Error::MalformedScalar.into());
    }
    if edit.offset == base_len {
        return consolidate_appends_attempt(value, inputs);
    }

    let mut leaves = vec![locate_leaf_attempt(value, edit.offset, base_len, inputs)?];
    let segment = loop {
        let last = leaves.last().unwrap();
        let covered_end = last
            .start
            .checked_add(last.bytes.len() as u64)
            .ok_or(Error::MetricOverflow)?;
        if covered_end >= edit_end {
            let segment_start = leaves[0].start;
            let mut segment = leaves
                .iter()
                .flat_map(|leaf| leaf.bytes.iter().copied())
                .collect::<Vec<_>>();
            let local_start =
                usize::try_from(edit.offset - segment_start).map_err(|_| Error::MetricOverflow)?;
            let local_end =
                usize::try_from(edit_end - segment_start).map_err(|_| Error::MetricOverflow)?;
            segment.splice(local_start..local_end, edit.insert_bytes.iter().copied());
            let ranges = leaf_ranges(value.kind, &segment)?;
            let resynchronized = ranges
                .last()
                .is_some_and(|range| range.start > 0 && segment[range.clone()] == last.bytes);
            if resynchronized || covered_end == base_len {
                break segment;
            }
        }
        leaves.push(next_leaf_attempt(value, last, inputs)?);
    };

    let left_path = leaves.first().unwrap().path.clone();
    let right_path = leaves.last().unwrap().path.clone();
    if left_path.len() != right_path.len() {
        return Err(Error::InvalidTree.into());
    }
    let mut existing = std::collections::BTreeSet::new();
    for leaf in &leaves {
        existing.insert(leaf.node_ref.object_hash);
        for frame in &leaf.path {
            existing.insert(frame.node_ref.object_hash);
        }
    }

    if value.byte_length == 0 {
        if !segment.is_empty() {
            return Err(Error::DescriptorMismatch.into());
        }
        return Ok(prepare(value.kind, &segment)?);
    }

    let mut stager = reusing_stager(value.kind, &existing);
    let mut replacement = if segment.is_empty() {
        Vec::new()
    } else {
        stager.leaves(&segment)?
    };

    for depth in (0..left_path.len()).rev() {
        let left = &left_path[depth];
        let right = &right_path[depth];
        let mut level = left.children[..left.selected]
            .iter()
            .cloned()
            .map(BuiltNode::from)
            .collect::<Vec<_>>();
        level.append(&mut replacement);
        level.extend(
            right.children[right.selected + 1..]
                .iter()
                .cloned()
                .map(BuiltNode::from),
        );
        let higher_path_has_siblings = left_path[..depth]
            .iter()
            .zip(&right_path[..depth])
            .any(|(left, right)| left.selected > 0 || right.selected + 1 < right.children.len());
        replacement = if level.len() == 1 && !higher_path_has_siblings {
            level
        } else {
            stager.branches(&level)?
        };
    }
    finish_rebuilt_tree(value, replacement, left_path.len(), stager)
}

/// Stage parent levels over `level` (rebuilt at `depth`) until one root
/// remains, then check that root still describes `value`'s final contents.
fn finish_rebuilt_tree(
    value: &LargeValueRef,
    level: Vec<BuiltNode>,
    depth: usize,
    mut stager: TreeStager<impl FnMut(ContentHash) -> bool>,
) -> Result<PreparedLargeValue, AttemptError> {
    let root = stager.root(level, depth, Error::InvalidTree)?;
    if root.metrics.byte_length != value.byte_length
        || root.metrics.utf16_length != value.utf16_length
    {
        return Err(Error::DescriptorMismatch.into());
    }
    Ok(PreparedLargeValue {
        value_ref: root.into_value_ref(value.kind),
        staged_chunks: stager.staged,
    })
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum Error {
    #[error("large string is not valid UTF-8")]
    InvalidUtf8,
    #[error("large JSON is not valid JSON")]
    InvalidJson,
    #[error("unsupported large-value format version {0}")]
    UnsupportedFormat(u8),
    #[error("chunk object hash does not match its authenticated reference")]
    ObjectHashMismatch,
    #[error("malformed large-value node")]
    MalformedNode,
    #[error("large-value metric overflow")]
    MetricOverflow,
    #[error("large-value descriptor metrics or logical hash are dishonest")]
    DescriptorMismatch,
    #[error("large-value tree contains a cycle or exceeds its depth bound")]
    InvalidTree,
    #[error("large-value logical traversal exceeded its deterministic work limit")]
    TraversalWorkLimitExceeded,
    #[error("large-value physical traversal exceeded its distinct-node limit")]
    PhysicalTraversalNodeLimitExceeded,
    #[error("malformed physical scalar encoding")]
    MalformedScalar,
    #[error("indirect scalar requires interruptible evaluation")]
    RequiresEvaluation,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ReachabilityError {
    #[error(transparent)]
    LargeValue(#[from] Error),
    #[error(transparent)]
    Chunk(#[from] crate::large_values::chunks::ChunkError),
}

#[derive(Clone)]
struct PhysicalTraversalEntry {
    node_ref: NodeRef,
    /// Exact ancestors on this occurrence's path. Shared descendants are
    /// legal; encountering an ancestor again is a cycle even when that node
    /// was already authenticated through another path.
    ancestors: Vec<NodeRef>,
}

#[derive(Clone, Copy)]
struct PhysicalNodeState {
    expected_logical_hash: ContentHash,
    expected_metrics: Option<NodeMetrics>,
    actual_metrics: Option<NodeMetrics>,
}

/// A physical graph traversal visits each immutable node once while retaining
/// enough per-edge evidence to reject inconsistent shared references and
/// cycles. Logical readers deliberately do not use this deduplication: every
/// edge occurrence contributes bytes to the scalar and is instead protected by
/// [`MAX_LOGICAL_TRAVERSAL_STEPS`].
struct PhysicalTraversal {
    root: NodeRef,
    pending: Vec<PhysicalTraversalEntry>,
    nodes: BTreeMap<NodeRef, PhysicalNodeState>,
    edges: BTreeMap<NodeRef, Vec<NodeRef>>,
    node_budget: PhysicalTraversalNodeBudget,
}

impl PhysicalTraversal {
    /// Walk `value`'s physical tree. Root metrics are only known up front
    /// when there is no edit tail; otherwise they describe the base tree.
    fn for_value(value: &LargeValueRef) -> Self {
        let root_metrics = value.edit_tail.is_empty().then_some(NodeMetrics {
            byte_length: value.byte_length,
            utf16_length: value.utf16_length,
        });
        Self::new(value.root.clone(), root_metrics, value.logical_hash)
    }

    fn new(
        root: NodeRef,
        expected_metrics: Option<NodeMetrics>,
        expected_logical_hash: ContentHash,
    ) -> Self {
        Self::new_with_node_limit(
            root,
            expected_metrics,
            expected_logical_hash,
            MAX_PHYSICAL_TRAVERSAL_NODES,
        )
        .expect("the configured physical traversal node limit is non-zero")
    }

    fn new_with_node_limit(
        root: NodeRef,
        expected_metrics: Option<NodeMetrics>,
        expected_logical_hash: ContentHash,
        max_nodes: usize,
    ) -> Result<Self, Error> {
        let mut node_budget = PhysicalTraversalNodeBudget::with_limit(max_nodes);
        node_budget.consume()?;
        Ok(Self {
            root: root.clone(),
            pending: vec![PhysicalTraversalEntry {
                node_ref: root.clone(),
                ancestors: Vec::new(),
            }],
            nodes: BTreeMap::from([(
                root,
                PhysicalNodeState {
                    expected_logical_hash,
                    expected_metrics,
                    actual_metrics: None,
                },
            )]),
            edges: BTreeMap::new(),
            node_budget,
        })
    }

    fn pop(&mut self) -> Option<PhysicalTraversalEntry> {
        self.pending.pop()
    }

    fn validate_node(
        &mut self,
        kind: LargeValueKind,
        node_ref: &NodeRef,
        node: &ChunkNode,
    ) -> Result<(), Error> {
        let state = self.nodes.get_mut(node_ref).ok_or(Error::InvalidTree)?;
        if node_logical_hash(node) != state.expected_logical_hash {
            return Err(Error::DescriptorMismatch);
        }
        let actual_metrics = node_metrics(kind, node)?;
        if state
            .expected_metrics
            .is_some_and(|expected| expected != actual_metrics)
        {
            return Err(Error::DescriptorMismatch);
        }
        state.actual_metrics = Some(actual_metrics);
        self.edges.insert(
            node_ref.clone(),
            match node {
                ChunkNode::Leaf { .. } => Vec::new(),
                ChunkNode::Branch { children, .. } => children
                    .iter()
                    .map(|child| child.node_ref.clone())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect(),
            },
        );
        Ok(())
    }

    /// The logical hash and metrics every discovered edge into `node_ref`
    /// agrees on (see [`Self::discover_child`]).
    fn edge_claim(&self, node_ref: &NodeRef) -> Result<(ContentHash, NodeMetrics), Error> {
        let state = self.nodes.get(node_ref).ok_or(Error::InvalidTree)?;
        Ok((
            state.expected_logical_hash,
            state.expected_metrics.ok_or(Error::InvalidTree)?,
        ))
    }

    /// Treat a node proven elsewhere (see [`validate_derived_upload`]) as a
    /// terminal of this traversal: neither it nor its descendants are read.
    fn seal_reused(&mut self, node_ref: &NodeRef) {
        self.edges.insert(node_ref.clone(), Vec::new());
    }

    /// The longest root path to every node of the validated graph. Call only
    /// after [`Self::finish`] succeeded: the graph is then acyclic and at most
    /// `MAX_TREE_DEPTH` deep, which also bounds this recursion.
    fn max_depths(&self) -> BTreeMap<NodeRef, usize> {
        fn post_order<'a>(
            node_ref: &'a NodeRef,
            edges: &'a BTreeMap<NodeRef, Vec<NodeRef>>,
            seen: &mut BTreeSet<&'a NodeRef>,
            order: &mut Vec<&'a NodeRef>,
        ) {
            if !seen.insert(node_ref) {
                return;
            }
            for child in edges.get(node_ref).into_iter().flatten() {
                post_order(child, edges, seen, order);
            }
            order.push(node_ref);
        }
        let mut order = Vec::new();
        post_order(&self.root, &self.edges, &mut BTreeSet::new(), &mut order);
        let mut depths = BTreeMap::from([(self.root.clone(), 0_usize)]);
        for node_ref in order.into_iter().rev() {
            let depth = depths.get(node_ref).copied().unwrap_or_default();
            for child in self.edges.get(node_ref).into_iter().flatten() {
                let entry = depths.entry(child.clone()).or_default();
                *entry = (*entry).max(depth + 1);
            }
        }
        depths
    }

    /// Verify the complete authenticated physical graph without enumerating
    /// its potentially exponential logical paths. Memoized height evaluation
    /// rejects both cycles and paths beyond the depth ceiling in O(V + E).
    fn finish(&self) -> Result<(), Error> {
        fn height(
            node_ref: &NodeRef,
            edges: &BTreeMap<NodeRef, Vec<NodeRef>>,
            visiting: &mut BTreeSet<NodeRef>,
            heights: &mut BTreeMap<NodeRef, usize>,
        ) -> Result<usize, Error> {
            if let Some(height) = heights.get(node_ref) {
                return Ok(*height);
            }
            if !visiting.insert(node_ref.clone()) {
                return Err(Error::InvalidTree);
            }
            let children = edges.get(node_ref).ok_or(Error::InvalidTree)?;
            let mut result = 0_usize;
            for child in children {
                result = result.max(
                    height(child, edges, visiting, heights)?
                        .checked_add(1)
                        .ok_or(Error::InvalidTree)?,
                );
                if result > MAX_TREE_DEPTH {
                    return Err(Error::InvalidTree);
                }
            }
            visiting.remove(node_ref);
            heights.insert(node_ref.clone(), result);
            Ok(result)
        }

        height(
            &self.root,
            &self.edges,
            &mut BTreeSet::new(),
            &mut BTreeMap::new(),
        )?;
        Ok(())
    }

    fn discover_child(
        &mut self,
        parent: &PhysicalTraversalEntry,
        child: BranchChild,
    ) -> Result<(), Error> {
        let mut ancestors = parent.ancestors.clone();
        ancestors.push(parent.node_ref.clone());
        if ancestors.len() > MAX_TREE_DEPTH || ancestors.contains(&child.node_ref) {
            return Err(Error::InvalidTree);
        }
        match self.nodes.entry(child.node_ref.clone()) {
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                let state = entry.get_mut();
                if state.expected_logical_hash != child.logical_hash
                    || state
                        .expected_metrics
                        .is_some_and(|metrics| metrics != child.metrics)
                    || state
                        .actual_metrics
                        .is_some_and(|metrics| metrics != child.metrics)
                {
                    return Err(Error::DescriptorMismatch);
                }
                state.expected_metrics = Some(child.metrics);
            }
            std::collections::btree_map::Entry::Vacant(entry) => {
                self.node_budget.consume()?;
                entry.insert(PhysicalNodeState {
                    expected_logical_hash: child.logical_hash,
                    expected_metrics: Some(child.metrics),
                    actual_metrics: None,
                });
                self.pending.push(PhysicalTraversalEntry {
                    node_ref: child.node_ref,
                    ancestors,
                });
            }
        }
        Ok(())
    }

    /// Authenticate `encoded` as `entry`'s node of `value`, then queue the
    /// node's children for discovery.
    fn admit_encoded(
        &mut self,
        value: &LargeValueRef,
        entry: &PhysicalTraversalEntry,
        encoded: &[u8],
    ) -> Result<(), Error> {
        let node_ref = &entry.node_ref;
        let node = decode_node_for_format(
            value.format_version,
            value.kind,
            node_ref.object_hash,
            encoded,
        )?;
        self.validate_node(value.kind, node_ref, &node)?;
        if let ChunkNode::Branch { children, .. } = node {
            self.discover_children(entry, children)?;
        }
        Ok(())
    }

    fn discover_children(
        &mut self,
        parent: &PhysicalTraversalEntry,
        children: Vec<BranchChild>,
    ) -> Result<(), Error> {
        for child in children.into_iter().rev() {
            self.discover_child(parent, child)?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub struct PhysicalTraversalNodeBudget {
    remaining: usize,
}

impl Default for PhysicalTraversalNodeBudget {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalTraversalNodeBudget {
    pub fn new() -> Self {
        Self::with_limit(MAX_PHYSICAL_TRAVERSAL_NODES)
    }

    fn with_limit(limit: usize) -> Self {
        Self { remaining: limit }
    }

    pub fn consume(&mut self) -> Result<(), Error> {
        self.remaining = self
            .remaining
            .checked_sub(1)
            .ok_or(Error::PhysicalTraversalNodeLimitExceeded)?;
        Ok(())
    }

    pub fn consume_many(&mut self, count: usize) -> Result<(), Error> {
        self.remaining = self
            .remaining
            .checked_sub(count)
            .ok_or(Error::PhysicalTraversalNodeLimitExceeded)?;
        Ok(())
    }
}

/// Where one node of a logical walk sits: its start and length in the walk's
/// unit, and the metrics its parent claims for it (`None` only for a root
/// whose descriptor carries an edit tail).
struct WalkSpan {
    start: u64,
    len: u64,
    metrics: Option<NodeMetrics>,
}

impl WalkSpan {
    fn end(&self) -> Result<u64, Error> {
        self.start
            .checked_add(self.len)
            .ok_or(Error::MetricOverflow)
    }
}

/// One kind of logical (occurrence-counting) read of a value's base tree.
/// [`walk_logical`] owns the frontier, the step budget, the depth bound and
/// the blocked bookkeeping; a visitor says which nodes to read and what to
/// do with them.
trait LogicalVisitor {
    /// Whether to load this node; `false` prunes it (out of range, or
    /// already accounted from its parent's metrics).
    fn wants(&mut self, _span: &WalkSpan) -> Result<bool, Error> {
        Ok(true)
    }
    fn leaf(&mut self, span: &WalkSpan, bytes: Vec<u8>) -> Result<(), Error>;
    /// Check a branch before its children are charged to the budget.
    fn branch(&mut self, _span: &WalkSpan, _children: &[BranchChild]) -> Result<(), Error> {
        Ok(())
    }
    /// A child's length in the walk's unit.
    fn child_len(&self, metrics: &NodeMetrics) -> Result<u64, Error>;
    /// Whether a branch's children must exactly cover the branch's span.
    const EXACT_COVER: bool = false;
}

/// Walk the base tree of `value` depth-first, leaves in logical order. A node
/// read blocked on a later input round is skipped, and the walk reports the
/// blocked outcome only after every other node has been tried, so one round
/// requests every missing chunk the walk can discover.
fn walk_logical<V: LogicalVisitor>(
    value: &LargeValueRef,
    root: WalkSpan,
    visitor: &mut V,
    inputs: &mut ChunkInputs,
) -> Result<(), AttemptError> {
    let mut pending = vec![(value.root.clone(), value.logical_hash, root, 0_usize)];
    let mut remaining = MAX_LOGICAL_TRAVERSAL_STEPS;
    let mut charge = |count: usize| -> Result<(), Error> {
        remaining = remaining
            .checked_sub(count)
            .ok_or(Error::TraversalWorkLimitExceeded)?;
        Ok(())
    };
    let mut blocked = false;
    while let Some((node_ref, expected_hash, span, depth)) = pending.pop() {
        charge(1)?;
        if depth > MAX_TREE_DEPTH {
            return Err(Error::InvalidTree.into());
        }
        if !visitor.wants(&span)? {
            continue;
        }
        let node = match load_authenticated_node_attempt(
            value.format_version,
            value.kind,
            &node_ref,
            expected_hash,
            inputs,
        ) {
            Err(AttemptError::Blocked) => {
                blocked = true;
                continue;
            }
            node => node?,
        };
        match node {
            ChunkNode::Leaf { bytes, .. } => visitor.leaf(&span, bytes)?,
            ChunkNode::Branch { children, .. } => {
                visitor.branch(&span, &children)?;
                charge(children.len())?;
                let first = pending.len();
                let mut child_start = span.start;
                for child in children {
                    let len = visitor.child_len(&child.metrics)?;
                    let child_span = WalkSpan {
                        start: child_start,
                        len,
                        metrics: Some(child.metrics),
                    };
                    child_start = child_span.end()?;
                    pending.push((child.node_ref, child.logical_hash, child_span, depth + 1));
                }
                if V::EXACT_COVER && child_start != span.end()? {
                    return Err(Error::DescriptorMismatch.into());
                }
                pending[first..].reverse();
            }
        }
    }
    if blocked {
        return Err(AttemptError::Blocked);
    }
    Ok(())
}

/// Authenticate and visit every immutable node reachable from one descriptor.
///
/// The traversal is depth-first and retains only a bounded branch frontier;
/// Jazz can therefore feed each visited node hash into its own mark set
/// without learning or reimplementing the node encoding.
pub async fn visit_reachable_chunks(
    value: &LargeValueRef,
    provider: &dyn crate::large_values::chunks::ChunkProvider,
    mut visit: impl FnMut(&ChunkRequest),
) -> Result<u64, ReachabilityError> {
    check_format(value.format_version)?;
    let mut traversal = PhysicalTraversal::for_value(value);
    let mut visited = 0_u64;
    while let Some(entry) = traversal.pop() {
        let node_ref = &entry.node_ref;
        let request = ChunkRequest {
            object_hash: node_ref.object_hash.0,
        };
        let encoded = provider.get(request.clone()).await?;
        let node = decode_node_for_format(
            value.format_version,
            value.kind,
            node_ref.object_hash,
            &encoded,
        )?;
        traversal.validate_node(value.kind, node_ref, &node)?;
        visit(&request);
        visited = visited.checked_add(1).ok_or(Error::MetricOverflow)?;
        if let ChunkNode::Branch { children, .. } = node {
            traversal.discover_children(&entry, children)?;
        }
    }
    traversal.finish()?;
    Ok(visited)
}

/// Authenticate the locally present prefix of a descriptor's tree and return
/// its current missing frontier. Children are discovered only from verified
/// branch nodes, so an empty result is a proof of graph closure.
pub async fn missing_upload_frontier(
    value: &LargeValueRef,
    reader: &dyn crate::large_values::chunks::ChunkReader,
    limit: usize,
) -> Result<Vec<NodeRef>, ReachabilityError> {
    check_format(value.format_version)?;
    let mut traversal = PhysicalTraversal::for_value(value);
    let mut missing = Vec::new();
    while let Some(entry) = traversal.pop() {
        let node_ref = &entry.node_ref;
        let encoded = match reader.get(node_ref.object_hash).await {
            Ok(encoded) => encoded,
            Err(crate::large_values::chunks::ChunkStorageError::Unavailable) => {
                missing.push(entry.node_ref);
                if missing.len() >= limit.max(1) {
                    break;
                }
                continue;
            }
            Err(error) => return Err(crate::large_values::chunks::ChunkError::from(error).into()),
        };
        traversal.admit_encoded(value, &entry, &encoded)?;
    }
    if missing.is_empty() {
        traversal.finish()?;
    }
    Ok(missing)
}

/// Verify the complete local tree of an upload immediately before issuing its
/// staging receipt.  In addition to authenticating every reachable node, this
/// proves that every node belongs to this particular pending-upload journal.
/// That ownership check prevents one upload's accounting from being used to
/// publish a descriptor assembled from unrelated already-present chunks.
pub async fn validate_finalized_upload(
    value: &LargeValueRef,
    reader: &dyn crate::large_values::chunks::ChunkReader,
    uploaded_chunks: &std::collections::BTreeSet<NodeRef>,
    descriptor_was_bound_before_completion: bool,
) -> Result<(), ReachabilityError> {
    validate_descriptor(value)?;
    let mut traversal = PhysicalTraversal::for_value(value);
    while let Some(entry) = traversal.pop() {
        let node_ref = &entry.node_ref;
        // Descriptor-keyed peer uploads may legitimately discover that an
        // identical descriptor was completed by another connection. Once its
        // pending record was bound to that exact descriptor at `begin`, local
        // immutable nodes are safe to reuse. Unbound/raw uploads, in contrast,
        // must prove every reachable node was journaled by this upload.
        if !descriptor_was_bound_before_completion && !uploaded_chunks.contains(node_ref) {
            return Err(Error::DescriptorMismatch.into());
        }
        let encoded = read_for_validation(reader, node_ref).await?;
        traversal.admit_encoded(value, &entry, &encoded)?;
    }
    traversal.finish()?;
    Ok(())
}

/// Validate a descriptor that storage itself derived from `base` by a local
/// append, splice or consolidation, before its staging receipt is issued.
///
/// Precondition: `base` is a published descriptor, and therefore (by the
/// publication invariant) a fully valid value. A derivation stages its new
/// nodes in `staged_chunks` and reuses base nodes unchanged, so re-walking the
/// reused part would make every local edit O(value). Instead:
///
/// 1. every newly staged node reachable from `value` is authenticated and
///    checked exactly as [`validate_finalized_upload`] does;
/// 2. descent stops at each reachable node that was not staged by this
///    derivation, recording the edge claim (logical hash and metrics) its
///    parent makes for it, without fetching it;
/// 3. every recorded claim must equal an edge of the base tree, found by
///    walking only base branches that were not themselves reused, and
///    stopping as soon as every claim is proven.
///
/// Step 3 proves each reused node is a member of the base tree with its true
/// metrics, at a base depth no shallower than its deepest derived position,
/// so its whole subtree, including the depth bound, is covered by the base's
/// validity. Unlike
/// the raw-upload validator the root's metrics are checked even while an
/// edit tail is present. The tail itself is validated separately by replaying
/// it (see [`validate_edit_tail_attempt`]).
///
/// Returns `Ok(false)` when reuse cannot be proven within the traversal
/// budget; the caller must then fall back to full validation.
pub async fn validate_derived_upload(
    value: &LargeValueRef,
    base: &LargeValueRef,
    reader: &dyn crate::large_values::chunks::ChunkReader,
    staged_chunks: &std::collections::BTreeSet<NodeRef>,
) -> Result<bool, ReachabilityError> {
    let tree = tail_base_descriptor(value)?;
    let base_tree = tail_base_descriptor(base)?;
    if value.kind != base.kind || value.format_version != base.format_version {
        return Err(Error::DescriptorMismatch.into());
    }
    let tree_metrics = |tree: &LargeValueRef| NodeMetrics {
        byte_length: tree.byte_length,
        utf16_length: tree.utf16_length,
    };
    // The caller's `base` descriptor is only an unauthenticated edge into a
    // retained root: that root was validated for some descriptor, not
    // necessarily this one. Authenticate the edge against the root node
    // itself (one node read), so every base claim below derives from content-
    // addressed nodes and a forged base length, hash or kind fails closed.
    let encoded_base_root = read_for_validation(reader, &base.root).await?;
    let base_root = decode_node_for_format(
        base.format_version,
        base.kind,
        base.root.object_hash,
        &encoded_base_root,
    )?;
    if node_logical_hash(&base_root) != base_tree.logical_hash
        || node_metrics(base.kind, &base_root)? != tree_metrics(&base_tree)
    {
        return Err(Error::DescriptorMismatch.into());
    }
    let mut traversal = PhysicalTraversal::new(
        value.root.clone(),
        Some(tree_metrics(&tree)),
        value.logical_hash,
    );
    let mut claims = BTreeMap::new();
    while let Some(entry) = traversal.pop() {
        let node_ref = &entry.node_ref;
        if !staged_chunks.contains(node_ref) {
            let claim = traversal.edge_claim(node_ref)?;
            claims.insert(node_ref.clone(), claim);
            traversal.seal_reused(node_ref);
            continue;
        }
        let encoded = read_for_validation(reader, node_ref).await?;
        traversal.admit_encoded(value, &entry, &encoded)?;
    }
    traversal.finish()?;
    let depths = traversal.max_depths();

    // The base descriptor is the edge into the base root, at depth zero.
    let mut pending = Vec::new();
    match prove_reused_edge(
        &mut claims,
        &depths,
        &base.root,
        (base.logical_hash, tree_metrics(&base_tree)),
        0,
    )? {
        ReuseProof::Proven => {}
        ReuseProof::NotReused => pending.push((base.root.clone(), 0)),
        ReuseProof::Unprovable => return Ok(false),
    }
    let mut visited = BTreeSet::new();
    while !claims.is_empty() {
        let Some((node_ref, depth)) = pending.pop() else {
            return Ok(false);
        };
        if !visited.insert(node_ref.clone()) {
            continue;
        }
        if visited.len() > MAX_PHYSICAL_TRAVERSAL_NODES {
            return Ok(false);
        }
        let encoded = read_for_validation(reader, &node_ref).await?;
        let node = decode_node_for_format(
            base.format_version,
            base.kind,
            node_ref.object_hash,
            &encoded,
        )?;
        if let ChunkNode::Branch { children, .. } = node {
            for child in children.into_iter().rev() {
                // A proven child is reused intact, so its subtree needs no
                // descent. Any other child was replaced or dropped by the
                // derivation; its edges may prove deeper reused nodes. A
                // reused node reachable only below a reused one (a reshaped
                // tree) stays unproven and forces full validation.
                match prove_reused_edge(
                    &mut claims,
                    &depths,
                    &child.node_ref,
                    (child.logical_hash, child.metrics),
                    depth + 1,
                )? {
                    ReuseProof::Proven => {}
                    ReuseProof::NotReused => pending.push((child.node_ref, depth + 1)),
                    ReuseProof::Unprovable => return Ok(false),
                }
            }
        }
    }
    Ok(true)
}

enum ReuseProof {
    /// The base edge matches a reused node's claim.
    Proven,
    /// The base edge leads to a node the derived value does not reuse here.
    NotReused,
    /// The node is reused deeper than the base proves its subtree can be.
    Unprovable,
}

/// Match one authenticated base edge at `base_depth` against the claims the
/// derived graph makes for its reused nodes. Base validity bounds a reused
/// subtree's height by `MAX_TREE_DEPTH - base_depth`, so the node must sit no
/// deeper in the derived graph for the derived depth check to remain exact.
fn prove_reused_edge(
    claims: &mut BTreeMap<NodeRef, (ContentHash, NodeMetrics)>,
    derived_depths: &BTreeMap<NodeRef, usize>,
    node_ref: &NodeRef,
    base_claim: (ContentHash, NodeMetrics),
    base_depth: usize,
) -> Result<ReuseProof, Error> {
    let Some(expected) = claims.get(node_ref) else {
        return Ok(ReuseProof::NotReused);
    };
    if *expected != base_claim {
        return Err(Error::DescriptorMismatch);
    }
    if derived_depths
        .get(node_ref)
        .is_none_or(|depth| *depth > base_depth)
    {
        return Ok(ReuseProof::Unprovable);
    }
    claims.remove(node_ref);
    Ok(ReuseProof::Proven)
}

/// Read one node back from local storage while validating an upload before
/// its staging receipt is issued.
async fn read_for_validation(
    reader: &dyn crate::large_values::chunks::ChunkReader,
    node_ref: &NodeRef,
) -> Result<bytes::Bytes, ReachabilityError> {
    let encoded = reader
        .get(node_ref.object_hash)
        .await
        .map_err(crate::large_values::chunks::ChunkError::from)?;
    record_finalize_validation_bytes(encoded.len());
    Ok(encoded)
}

// Scale-independence receipt for staging-receipt validation: the encoded node
// and logical bytes that finalization reads back. Test-only, per thread, so
// parallel tests cannot observe one another.
#[cfg(any(test, feature = "testing"))]
std::thread_local! {
    static FINALIZE_VALIDATION_BYTES: Cell<u64> = const { Cell::new(0) };
}

#[inline]
pub fn record_finalize_validation_bytes(_bytes: usize) {
    #[cfg(any(test, feature = "testing"))]
    FINALIZE_VALIDATION_BYTES.with(|total| total.set(total.get().saturating_add(_bytes as u64)));
}

/// Total bytes this thread has read back while validating large-value uploads
/// before issuing staging receipts.
#[cfg(any(test, feature = "testing"))]
pub fn finalize_validation_bytes_for_test() -> u64 {
    FINALIZE_VALIDATION_BYTES.with(Cell::get)
}

#[derive(Clone)]
struct BuiltNode {
    node_ref: NodeRef,
    metrics: NodeMetrics,
    structural_hash: ContentHash,
}

impl BuiltNode {
    /// The tail-free current-format descriptor rooted at this node.
    fn into_value_ref(self, kind: LargeValueKind) -> LargeValueRef {
        LargeValueRef {
            kind,
            format_version: FORMAT_VERSION,
            logical_hash: self.structural_hash,
            root: self.node_ref,
            byte_length: self.metrics.byte_length,
            utf16_length: self.metrics.utf16_length,
            edit_tail: Vec::new(),
        }
    }
}

impl From<&BuiltNode> for BranchChild {
    fn from(node: &BuiltNode) -> Self {
        Self {
            node_ref: node.node_ref.clone(),
            metrics: node.metrics,
            logical_hash: node.structural_hash,
        }
    }
}

impl From<BranchChild> for BuiltNode {
    fn from(child: BranchChild) -> Self {
        Self {
            node_ref: child.node_ref,
            metrics: child.metrics,
            structural_hash: child.logical_hash,
        }
    }
}

/// The current-format branch over `group`, with its summed metrics.
fn branch_over(
    kind: LargeValueKind,
    group: &[BuiltNode],
) -> Result<(ChunkNode, NodeMetrics), Error> {
    let mut group_metrics = group.iter().map(|child| child.metrics);
    let first = group_metrics.next().ok_or(Error::MalformedNode)?;
    let metrics = group_metrics.try_fold(first, add_metrics)?;
    let node = ChunkNode::Branch {
        format: FORMAT_VERSION,
        kind,
        children: group.iter().map(BranchChild::from).collect(),
    };
    Ok((node, metrics))
}

/// Encode `node` and name it by its object hash. The encoded chunk is
/// returned for staging only when `fresh` says so of the hash; a node
/// already stored needs no staging.
fn seal_node(
    node: &ChunkNode,
    metrics: NodeMetrics,
    fresh: impl FnOnce(ContentHash) -> bool,
) -> Result<(BuiltNode, Option<StagedChunk>), Error> {
    let structural_hash = node_logical_hash(node);
    let encoded = encode_node(node)?;
    let object_hash = object_hash(&encoded);
    let fresh = fresh(object_hash);
    let node_ref = NodeRef { object_hash };
    let chunk = fresh.then(|| StagedChunk {
        node_ref: node_ref.clone(),
        encoded,
    });
    let built = BuiltNode {
        node_ref,
        metrics,
        structural_hash,
    };
    Ok((built, chunk))
}

#[derive(Clone)]
struct PathFrame {
    node_ref: NodeRef,
    children: Vec<BranchChild>,
    selected: usize,
}

#[derive(Clone)]
struct LocatedLeaf {
    node_ref: NodeRef,
    bytes: Vec<u8>,
    start: u64,
    path: Vec<PathFrame>,
}

/// Construct the canonical tree over `logical_bytes`, staging every node.
/// Trees depend only on content, so equal bytes give equal nodes.
pub fn prepare(kind: LargeValueKind, logical_bytes: &[u8]) -> Result<PreparedLargeValue, Error> {
    validate_logical(kind, logical_bytes)?;
    let mut stager = TreeStager::new(kind, |_| true);
    let leaves = stager.leaves(logical_bytes)?;
    let root = stager.root(leaves, 0, Error::MalformedNode)?;
    Ok(PreparedLargeValue {
        value_ref: root.into_value_ref(kind),
        staged_chunks: stager.staged,
    })
}

/// Encode the internal stored-scalar enum using the ordinary enum and
/// record codecs. The containing schema supplies the declared kind, including
/// the backing primitive type and the expected kind of every referenced node.
pub fn encode_stored_scalar(kind: LargeValueKind, value: &StoredScalar) -> Result<Vec<u8>, Error> {
    encode_stored_scalar_canonical(kind, value)
}

/// Encode the primitive arm from borrowed logical bytes. Its sole raw payload
/// field is the complete record payload, so only ordinary variant framing is
/// needed; no Value, field map, or intermediate record is required.
#[cfg(test)]
pub fn encode_primitive_stored_scalar(
    kind: LargeValueKind,
    bytes: &[u8],
) -> Result<Vec<u8>, Error> {
    let mut output = Vec::new();
    encode_primitive_stored_scalar_into(kind, bytes, &mut output)?;
    Ok(output)
}

pub fn encode_primitive_stored_scalar_into(
    kind: LargeValueKind,
    bytes: &[u8],
    output: &mut Vec<u8>,
) -> Result<(), Error> {
    #[cfg(test)]
    {
        STORED_SCALAR_CANONICAL_ENCODE_CALLS.with(|calls| calls.set(calls.get() + 1));
    }
    validate_logical(kind, bytes)?;
    crate::records::append_variant_record(output, 0, bytes);
    Ok(())
}

fn encode_primitive_payload(kind: LargeValueKind, bytes: &[u8]) -> Result<Vec<u8>, Error> {
    validate_logical(kind, bytes)?;
    Ok(crate::records::encode_variant_record(0, bytes))
}

/// The physical scalar encoder shared by public writes and raw canonicality
/// checks. The test-only public-write counter intentionally lives in the
/// wrapper above: decoding an already-inline row must not look like a write
/// that re-encoded its scalar.
fn encode_stored_scalar_canonical(
    kind: LargeValueKind,
    value: &StoredScalar,
) -> Result<Vec<u8>, Error> {
    #[cfg(test)]
    STORED_SCALAR_CANONICAL_ENCODE_CALLS.with(|calls| calls.set(calls.get() + 1));
    let schema = stored_scalar_schema(kind);
    let enum_value = match value {
        StoredScalar::Primitive(bytes) => return encode_primitive_payload(kind, bytes),
        StoredScalar::Chunked(value) => {
            if value.kind != kind {
                return Err(Error::DescriptorMismatch);
            }
            validate_descriptor(value)?;
            EnumValue::create(
                1,
                schema.case(1).map_err(|_| Error::MalformedScalar)?.payload,
                &chunked_values(value),
            )
            .map_err(|_| Error::MalformedScalar)?
        }
    };
    crate::records::encode_single_field_value(
        &Value::Enum(enum_value),
        stored_scalar_value_type(kind),
    )
    .map_err(|_| Error::MalformedScalar)
}

// A structural receipt for the current-row materialization fast path. This is
// deliberately test-only: the production contract is that inline values are
// returned verbatim without entering the scalar encoder, not that callers pay
// for metrics bookkeeping.
#[cfg(test)]
std::thread_local! {
    static STORED_SCALAR_DECODE_CALLS: Cell<usize> = const { Cell::new(0) };
    static STORED_SCALAR_CANONICAL_ENCODE_CALLS: Cell<usize> = const { Cell::new(0) };
}

/// Decode and canonically validate the internal stored-scalar enum. The
/// declared kind is supplied by the containing schema. Primitive payloads are
/// interpreted directly through that schema; indirect values authenticate the
/// expected kind when their content-addressed nodes are decoded.
pub fn decode_stored_scalar(kind: LargeValueKind, encoded: &[u8]) -> Result<StoredScalar, Error> {
    #[cfg(test)]
    STORED_SCALAR_DECODE_CALLS.with(|calls| calls.set(calls.get() + 1));
    let (tag, payload) =
        crate::records::split_variant_record(encoded).map_err(|_| Error::MalformedScalar)?;
    let decoded = match tag {
        0 => {
            // The primitive arm is one raw field. After ordinary tag framing,
            // logical validation covers the payload without reconstructing or
            // re-encoding a record to recover those same bytes.
            return decode_primitive_payload(kind, payload).map(StoredScalar::Primitive);
        }
        1 => {
            // This deliberately reads only the outer tag and the first static
            // descriptor byte. An unknown format must win before the current
            // V1 root/edit ValueTypes inspect an adversarial nested payload.
            let format = preflight_descriptor_format(payload)?;
            format
                .decode_descriptor_payload(kind, payload)
                .map(StoredScalar::Chunked)
        }
        _ => Err(Error::MalformedScalar),
    }?;
    if encode_stored_scalar_canonical(kind, &decoded)? != encoded {
        return Err(Error::MalformedScalar);
    }
    Ok(decoded)
}

pub fn inline_scalar_bytes(kind: LargeValueKind, encoded: &[u8]) -> Result<&[u8], Error> {
    let (tag, payload) =
        crate::records::split_variant_record(encoded).map_err(|_| Error::MalformedScalar)?;
    match tag {
        0 => {
            // Primitive has exactly one field, slot 1: raw bytes or raw string.
            // A sole trailing raw field has no header, offsets, padding, or
            // reserved slots: its canonical record is the payload itself.
            // split_variant_record already validates the complete tag framing.
            // Thus logical validation is precisely the old decode/recreate
            // acceptance check, without allocating copies merely to borrow it.
            validate_logical(kind, payload).map_err(|_| Error::MalformedScalar)?;
            Ok(payload)
        }
        1 => {
            // Validate the complete descriptor before reporting that materialization is needed.
            let _ = decode_stored_scalar(kind, encoded)?;
            Err(Error::RequiresEvaluation)
        }
        _ => Err(Error::MalformedScalar),
    }
}

fn stored_scalar_schema(kind: LargeValueKind) -> &'static EnumSchema {
    // Current-row evaluation decodes this ordinary scalar envelope on every
    // affected record. Building its nested descriptors repeatedly re-hashes
    // their layouts even though `RecordDescriptor` later interns them, making
    // a rapid stream of revisions pay avoidable work proportional to every
    // historical update. The schema is immutable and kind-parametric, so one
    // process-wide instance per declared kind is the canonical interpretation.
    static BYTES: OnceLock<EnumSchema> = OnceLock::new();
    static STRING: OnceLock<EnumSchema> = OnceLock::new();
    static JSON: OnceLock<EnumSchema> = OnceLock::new();

    let schema = match kind {
        LargeValueKind::Bytes => &BYTES,
        LargeValueKind::String => &STRING,
        LargeValueKind::Json => &JSON,
    };
    schema.get_or_init(|| build_stored_scalar_schema(kind))
}

fn stored_scalar_value_type(kind: LargeValueKind) -> &'static ValueType {
    // `ValueType::Enum` owns its schema. Cache this wrapper as well so the
    // ordinary scalar codec does not deep-clone the schema for every cell.
    static BYTES: OnceLock<ValueType> = OnceLock::new();
    static STRING: OnceLock<ValueType> = OnceLock::new();
    static JSON: OnceLock<ValueType> = OnceLock::new();

    let value_type = match kind {
        LargeValueKind::Bytes => &BYTES,
        LargeValueKind::String => &STRING,
        LargeValueKind::Json => &JSON,
    };
    value_type.get_or_init(|| ValueType::Enum(Box::new(stored_scalar_schema(kind).clone())))
}

pub fn node_ref_descriptor() -> RecordDescriptor {
    node_ref_record_schema().descriptor
}

fn node_ref_record_schema() -> &'static DurableLargeValueRecordSchema {
    static SCHEMA: OnceLock<DurableLargeValueRecordSchema> = OnceLock::new();
    SCHEMA.get_or_init(|| {
        durable_large_value_record_descriptor([(
            NODE_REF_OBJECT_HASH_FIELD,
            "object_hash",
            ValueType::raw_bytes(),
        )])
    })
}

pub fn node_ref_value(node_ref: &NodeRef) -> Value {
    let descriptor = node_ref_descriptor();
    Value::Record(crate::records::OwnedRecord::new(
        encode_node_ref(node_ref).expect("NodeRef fields always match their physical descriptor"),
        descriptor,
    ))
}

pub fn node_ref_from_value(value: &Value) -> Result<NodeRef, Error> {
    let Value::Record(record) = value else {
        return Err(Error::MalformedScalar);
    };
    let values = record.to_values().map_err(|_| Error::MalformedScalar)?;
    node_ref_from_values(&values)
}

fn node_ref_from_values(values: &[Value]) -> Result<NodeRef, Error> {
    let mut fields = node_ref_record_schema().decode_values(values)?;
    Ok(NodeRef {
        object_hash: ContentHash(take_raw32(&mut fields, NODE_REF_OBJECT_HASH_FIELD)?),
    })
}

/// Encode a physical chunk identity through the same canonical record
/// used by indirect scalar roots.
///
/// This is an engine-format primitive for storage compatibility receipts and
/// engine-owned metadata keys, not an application value encoding.
pub fn encode_node_ref(node_ref: &NodeRef) -> Result<Vec<u8>, Error> {
    let values = node_ref_record_schema().ordered_values([(
        NODE_REF_OBJECT_HASH_FIELD,
        Value::Bytes(node_ref.object_hash.0.to_vec()),
    )])?;
    node_ref_descriptor()
        .create(&values)
        .map_err(|_| Error::MalformedScalar)
}

/// Decode and canonically validate a physical chunk identity.
pub fn decode_node_ref(encoded: &[u8]) -> Result<NodeRef, Error> {
    let descriptor = node_ref_descriptor();
    let values = descriptor
        .bind(encoded)
        .to_values()
        .map_err(|_| Error::MalformedScalar)?;
    let node_ref = node_ref_from_values(&values)?;
    if encode_node_ref(&node_ref)? != encoded {
        return Err(Error::MalformedScalar);
    }
    Ok(node_ref)
}

fn build_stored_scalar_schema(kind: LargeValueKind) -> EnumSchema {
    let primitive = primitive_payload_schema(kind).descriptor;
    let chunked = large_value_ref_payload_descriptor(kind);
    EnumSchema::new(
        match kind {
            LargeValueKind::Bytes => "groove.internal.stored_scalar.bytes",
            LargeValueKind::String => "groove.internal.stored_scalar.string",
            LargeValueKind::Json => "groove.internal.stored_scalar.json",
        },
        // Primitive is tag 0 and Chunked tag 1. Stores and peers from before
        // storage format 1 are refused at admission, so no other byte
        // sequence reaches this decoder.
        [
            EnumCase::new("Primitive", primitive),
            EnumCase::new("Chunked", chunked),
        ],
    )
    .expect("fixed internal stored-scalar enum schema is valid")
}

fn primitive_payload_schema(kind: LargeValueKind) -> &'static DurableLargeValueRecordSchema {
    static BYTES: OnceLock<DurableLargeValueRecordSchema> = OnceLock::new();
    static STRING: OnceLock<DurableLargeValueRecordSchema> = OnceLock::new();
    static JSON: OnceLock<DurableLargeValueRecordSchema> = OnceLock::new();
    let schema = match kind {
        LargeValueKind::Bytes => &BYTES,
        LargeValueKind::String => &STRING,
        LargeValueKind::Json => &JSON,
    };
    schema.get_or_init(|| {
        durable_large_value_record_descriptor([(
            PRIMITIVE_VALUE_FIELD,
            "value",
            match kind {
                LargeValueKind::Bytes => ValueType::raw_bytes(),
                LargeValueKind::String | LargeValueKind::Json => ValueType::raw_string(),
            },
        )])
    })
}

fn large_value_edit_descriptor(kind: LargeValueKind) -> RecordDescriptor {
    large_value_edit_schema(kind).descriptor
}

fn large_value_edit_schema(kind: LargeValueKind) -> &'static DurableLargeValueRecordSchema {
    static BYTES: OnceLock<DurableLargeValueRecordSchema> = OnceLock::new();
    static TEXT: OnceLock<DurableLargeValueRecordSchema> = OnceLock::new();
    let schema = match kind {
        LargeValueKind::Bytes => &BYTES,
        LargeValueKind::String | LargeValueKind::Json => &TEXT,
    };
    schema.get_or_init(|| {
        let mut fields = vec![
            (EDIT_OFFSET_FIELD, "offset", ValueType::U64),
            (EDIT_DELETE_LENGTH_FIELD, "delete_length", ValueType::U64),
            (
                EDIT_INSERT_BYTES_FIELD,
                "insert_bytes",
                ValueType::raw_bytes(),
            ),
        ];
        if kind != LargeValueKind::Bytes {
            fields.extend([
                (EDIT_UTF16_OFFSET_FIELD, "utf16_offset", ValueType::U64),
                (
                    EDIT_DELETE_UTF16_LENGTH_FIELD,
                    "delete_utf16_length",
                    ValueType::U64,
                ),
            ]);
        }
        durable_large_value_record_descriptor(fields)
    })
}

fn large_value_ref_payload_descriptor(kind: LargeValueKind) -> RecordDescriptor {
    large_value_ref_payload_schema(kind).descriptor
}

fn large_value_ref_payload_schema(kind: LargeValueKind) -> &'static DurableLargeValueRecordSchema {
    static BYTES: OnceLock<DurableLargeValueRecordSchema> = OnceLock::new();
    static TEXT: OnceLock<DurableLargeValueRecordSchema> = OnceLock::new();
    let schema = match kind {
        LargeValueKind::Bytes => &BYTES,
        LargeValueKind::String | LargeValueKind::Json => &TEXT,
    };
    schema.get_or_init(|| {
        let mut fields = vec![
            (
                LARGE_VALUE_REF_FORMAT_VERSION_FIELD,
                "format_version",
                ValueType::U8,
            ),
            (
                LARGE_VALUE_REF_LOGICAL_HASH_FIELD,
                "logical_hash",
                ValueType::raw_bytes(),
            ),
            (
                LARGE_VALUE_REF_ROOT_FIELD,
                "root",
                ValueType::Record(Box::new(node_ref_descriptor())),
            ),
            (
                LARGE_VALUE_REF_BYTE_LENGTH_FIELD,
                "byte_length",
                ValueType::U64,
            ),
            (
                LARGE_VALUE_REF_EDIT_TAIL_FIELD,
                "edit_tail",
                ValueType::Array(Box::new(ValueType::Record(Box::new(
                    large_value_edit_descriptor(kind),
                )))),
            ),
        ];
        if kind != LargeValueKind::Bytes {
            fields.push((
                LARGE_VALUE_REF_UTF16_LENGTH_FIELD,
                "utf16_length",
                ValueType::U64,
            ));
        }
        durable_large_value_record_descriptor(fields)
    })
}

const NODE_REF_OBJECT_HASH_FIELD: u16 = 1;
const PRIMITIVE_VALUE_FIELD: u16 = 1;
const EDIT_OFFSET_FIELD: u16 = 1;
const EDIT_DELETE_LENGTH_FIELD: u16 = 2;
const EDIT_INSERT_BYTES_FIELD: u16 = 3;
const EDIT_UTF16_OFFSET_FIELD: u16 = 4;
const EDIT_DELETE_UTF16_LENGTH_FIELD: u16 = 5;
const LARGE_VALUE_REF_FORMAT_VERSION_FIELD: u16 = 1;
const LARGE_VALUE_REF_LOGICAL_HASH_FIELD: u16 = 2;
const LARGE_VALUE_REF_ROOT_FIELD: u16 = 3;
const LARGE_VALUE_REF_BYTE_LENGTH_FIELD: u16 = 4;
const LARGE_VALUE_REF_EDIT_TAIL_FIELD: u16 = 5;
const LARGE_VALUE_REF_UTF16_LENGTH_FIELD: u16 = 6;

#[derive(Clone)]
struct DurableLargeValueRecordSchema {
    field_ids: Vec<u16>,
    descriptor: RecordDescriptor,
}

impl DurableLargeValueRecordSchema {
    fn ordered_values(
        &self,
        input_values: impl IntoIterator<Item = (u16, Value)>,
    ) -> Result<Vec<Value>, Error> {
        let mut values = BTreeMap::<u16, Value>::new();
        for (id, value) in input_values {
            if values.insert(id, value).is_some() {
                return Err(Error::MalformedScalar);
            }
        }
        let mut ordered = Vec::with_capacity(self.field_ids.len());
        for id in &self.field_ids {
            ordered.push(values.remove(id).ok_or(Error::MalformedScalar)?);
        }
        if values.is_empty() {
            Ok(ordered)
        } else {
            Err(Error::MalformedScalar)
        }
    }

    fn decode_values(&self, values: &[Value]) -> Result<BTreeMap<u16, Value>, Error> {
        if values.len() != self.field_ids.len() {
            return Err(Error::MalformedScalar);
        }
        let fields = self
            .field_ids
            .iter()
            .copied()
            .zip(values.iter().cloned())
            .collect();
        Ok(fields)
    }
}

fn take_durable_large_value_field(
    fields: &mut BTreeMap<u16, Value>,
    id: u16,
) -> Result<Value, Error> {
    fields.remove(&id).ok_or(Error::MalformedScalar)
}

/// Take field `id` as the variant `extract` accepts; an absent or mistyped
/// field is malformed.
fn take_field<T>(
    fields: &mut BTreeMap<u16, Value>,
    id: u16,
    extract: impl FnOnce(Value) -> Option<T>,
) -> Result<T, Error> {
    extract(take_durable_large_value_field(fields, id)?).ok_or(Error::MalformedScalar)
}

fn take_u8(fields: &mut BTreeMap<u16, Value>, id: u16) -> Result<u8, Error> {
    take_field(fields, id, |value| match value {
        Value::U8(value) => Some(value),
        _ => None,
    })
}

fn take_u64(fields: &mut BTreeMap<u16, Value>, id: u16) -> Result<u64, Error> {
    take_field(fields, id, |value| match value {
        Value::U64(value) => Some(value),
        _ => None,
    })
}

fn take_raw32(fields: &mut BTreeMap<u16, Value>, id: u16) -> Result<[u8; 32], Error> {
    take_field(fields, id, |value| raw_bytes(&value).ok())
}

fn take_bytes(fields: &mut BTreeMap<u16, Value>, id: u16) -> Result<Vec<u8>, Error> {
    take_field(fields, id, |value| match value {
        Value::Bytes(value) => Some(value),
        _ => None,
    })
}

/// Construct a fixed engine-owned record layout from permanent, one-based
/// positional field IDs. Source declaration order is normalized, but numeric
/// IDs are actual record ordinals. Gaps and duplicate IDs are rejected.
fn durable_large_value_record_descriptor(
    fields: impl IntoIterator<Item = (u16, &'static str, ValueType)>,
) -> DurableLargeValueRecordSchema {
    let mut fields = fields.into_iter().collect::<Vec<_>>();
    fields.sort_by_key(|(id, _, _)| *id);
    assert!(
        !fields.is_empty(),
        "large-value record must have at least one field"
    );
    assert!(
        fields
            .iter()
            .enumerate()
            .all(|(index, (id, _, _))| usize::from(*id) == index + 1),
        "large-value record field IDs must be contiguous starting at one"
    );
    let field_ids = fields.iter().map(|(id, _, _)| *id).collect();
    let descriptor = RecordDescriptor::new(
        fields
            .into_iter()
            .map(|(id, name, value_type)| (format!("f{id:04}_{name}"), value_type)),
    );
    DurableLargeValueRecordSchema {
        field_ids,
        descriptor,
    }
}

const LARGE_VALUE_REF_BYTES_TAG: u8 = 0;
const LARGE_VALUE_REF_STRING_TAG: u8 = 1;
const LARGE_VALUE_REF_JSON_TAG: u8 = 2;

fn large_value_ref_schema() -> &'static EnumSchema {
    static SCHEMA: OnceLock<EnumSchema> = OnceLock::new();
    SCHEMA.get_or_init(|| {
        let cases = [
            (
                LARGE_VALUE_REF_BYTES_TAG,
                EnumCase::new(
                    "Bytes",
                    large_value_ref_payload_descriptor(LargeValueKind::Bytes),
                ),
            ),
            (
                LARGE_VALUE_REF_STRING_TAG,
                EnumCase::new(
                    "String",
                    large_value_ref_payload_descriptor(LargeValueKind::String),
                ),
            ),
            (
                LARGE_VALUE_REF_JSON_TAG,
                EnumCase::new(
                    "Json",
                    large_value_ref_payload_descriptor(LargeValueKind::Json),
                ),
            ),
        ];
        assert!(
            cases
                .iter()
                .enumerate()
                .all(|(index, (tag, _))| usize::from(*tag) == index)
        );
        EnumSchema::new(
            "groove.internal.large_value_ref",
            cases.map(|(_, case)| case),
        )
        .expect("fixed internal large-value-ref enum schema is valid")
    })
}

pub fn encode_large_value_ref(value: &LargeValueRef) -> Result<Vec<u8>, Error> {
    validate_descriptor(value)?;
    let tag = u32::from(large_value_kind_tag(value.kind));
    let schema = large_value_ref_schema();
    let payload = schema
        .case(tag)
        .map_err(|_| Error::MalformedScalar)?
        .payload;
    let value = EnumValue::create(tag, payload, &chunked_values(value))
        .map_err(|_| Error::MalformedScalar)?;
    crate::records::encode_single_field_value(
        &Value::Enum(value),
        &ValueType::Enum(Box::new(schema.clone())),
    )
    .map_err(|_| Error::MalformedScalar)
}

pub fn decode_large_value_ref(encoded: &[u8]) -> Result<LargeValueRef, Error> {
    let (tag, payload) =
        crate::records::split_variant_record(encoded).map_err(|_| Error::MalformedScalar)?;
    let kind = large_value_kind_from_tag(u8::try_from(tag).map_err(|_| Error::MalformedScalar)?)?;
    // As for a stored scalar, no V1 nested descriptor layout is decoded until
    // the raw outer descriptor has selected a recognized format.
    let format = preflight_descriptor_format(payload)?;
    let decoded = format.decode_descriptor_payload(kind, payload)?;
    if encode_large_value_ref(&decoded)? != encoded {
        return Err(Error::MalformedScalar);
    }
    Ok(decoded)
}

#[cfg(test)]
fn primitive_value(kind: LargeValueKind, bytes: Vec<u8>) -> Value {
    match kind {
        LargeValueKind::Bytes => Value::Bytes(bytes),
        LargeValueKind::String | LargeValueKind::Json => {
            Value::String(String::from_utf8(bytes).expect("validated logical text/JSON primitive"))
        }
    }
}

#[cfg(test)]
fn primitive_bytes(kind: LargeValueKind, value: &Value) -> Result<Vec<u8>, Error> {
    let bytes = match (kind, value) {
        (LargeValueKind::Bytes, Value::Bytes(bytes)) => Ok(bytes.clone()),
        (LargeValueKind::String | LargeValueKind::Json, Value::String(value)) => {
            Ok(value.as_bytes().to_vec())
        }
        _ => Err(Error::MalformedScalar),
    }?;
    validate_logical(kind, &bytes)?;
    Ok(bytes)
}

fn decode_primitive_payload(kind: LargeValueKind, payload: &[u8]) -> Result<Vec<u8>, Error> {
    // Slot 1 is the only field and is raw string/bytes: the record encoding is
    // exactly its payload. Match borrowed inline access without reconstructing
    // and re-encoding an owned record merely to recover those same bytes.
    validate_logical(kind, payload)?;
    Ok(payload.to_vec())
}

/// Read precisely the V1-independent selector: the descriptor payload starts
/// with its fixed-width `format_version:u8` field. Do not bind the enclosing
/// record here: binding computes spans for the root and edit-tail fields and
/// would let a future-format payload fail as malformed before dispatch.
fn preflight_descriptor_format(payload: &[u8]) -> Result<LargeValueFormat, Error> {
    let version = *payload.first().ok_or(Error::MalformedScalar)?;
    LargeValueFormat::from_version(version)
}

fn decode_descriptor_payload_v1(
    kind: LargeValueKind,
    payload: &[u8],
) -> Result<LargeValueRef, Error> {
    let schema = large_value_ref_payload_schema(kind);
    let values = schema
        .descriptor
        .bind(payload)
        .to_values()
        .map_err(|_| Error::MalformedScalar)?;
    if schema
        .descriptor
        .create(&values)
        .map_err(|_| Error::MalformedScalar)?
        != payload
    {
        return Err(Error::MalformedScalar);
    }
    decode_chunked_values(kind, &values)
}

fn raw_bytes(value: &Value) -> Result<[u8; 32], Error> {
    let Value::Bytes(value) = value else {
        return Err(Error::MalformedScalar);
    };
    value
        .as_slice()
        .try_into()
        .map_err(|_| Error::MalformedScalar)
}

fn chunked_values(value: &LargeValueRef) -> Vec<Value> {
    let kind = value.kind;
    let mut fields = vec![
        (
            LARGE_VALUE_REF_FORMAT_VERSION_FIELD,
            Value::U8(value.format_version),
        ),
        (
            LARGE_VALUE_REF_LOGICAL_HASH_FIELD,
            Value::Bytes(value.logical_hash.0.to_vec()),
        ),
        (LARGE_VALUE_REF_ROOT_FIELD, node_ref_value(&value.root)),
        (
            LARGE_VALUE_REF_BYTE_LENGTH_FIELD,
            Value::U64(value.byte_length),
        ),
        (
            LARGE_VALUE_REF_EDIT_TAIL_FIELD,
            Value::Array(
                value
                    .edit_tail
                    .iter()
                    .map(|edit_value| {
                        let mut fields = vec![
                            (EDIT_OFFSET_FIELD, Value::U64(edit_value.offset)),
                            (
                                EDIT_DELETE_LENGTH_FIELD,
                                Value::U64(edit_value.delete_length),
                            ),
                            (
                                EDIT_INSERT_BYTES_FIELD,
                                Value::Bytes(edit_value.insert_bytes.clone()),
                            ),
                        ];
                        if kind != LargeValueKind::Bytes {
                            fields.extend([
                                (EDIT_UTF16_OFFSET_FIELD, Value::U64(edit_value.utf16_offset)),
                                (
                                    EDIT_DELETE_UTF16_LENGTH_FIELD,
                                    Value::U64(edit_value.delete_utf16_length),
                                ),
                            ]);
                        }
                        let fields = large_value_edit_schema(kind)
                            .ordered_values(fields)
                            .expect("all internal edit fields are present");
                        Value::Record(crate::records::OwnedRecord::new(
                            large_value_edit_descriptor(kind)
                                .create(&fields)
                                .expect("internal edit record"),
                            large_value_edit_descriptor(kind),
                        ))
                    })
                    .collect(),
            ),
        ),
    ];
    if kind != LargeValueKind::Bytes {
        fields.push((
            LARGE_VALUE_REF_UTF16_LENGTH_FIELD,
            Value::U64(
                value
                    .utf16_length
                    .expect("validated text descriptor has a UTF-16 length"),
            ),
        ));
    }
    large_value_ref_payload_schema(kind)
        .ordered_values(fields)
        .expect("all internal large-value reference fields are present")
}

fn decode_chunked_values(kind: LargeValueKind, values: &[Value]) -> Result<LargeValueRef, Error> {
    let mut fields = large_value_ref_payload_schema(kind).decode_values(values)?;
    let format_version = take_u8(&mut fields, LARGE_VALUE_REF_FORMAT_VERSION_FIELD)?;
    // Select the joint descriptor/edit/scalar contract before decoding the
    // nested root or tail. Do not let a future version accidentally inherit
    // V1 field interpretation merely because the outer record decoded.
    let format = LargeValueFormat::from_version(format_version)?;
    let logical_hash = ContentHash(take_raw32(&mut fields, LARGE_VALUE_REF_LOGICAL_HASH_FIELD)?);
    let root = take_durable_large_value_field(&mut fields, LARGE_VALUE_REF_ROOT_FIELD)?;
    let byte_length = take_u64(&mut fields, LARGE_VALUE_REF_BYTE_LENGTH_FIELD)?;
    let utf16_length = if kind == LargeValueKind::Bytes {
        None
    } else {
        Some(take_u64(&mut fields, LARGE_VALUE_REF_UTF16_LENGTH_FIELD)?)
    };
    let edits = take_field(
        &mut fields,
        LARGE_VALUE_REF_EDIT_TAIL_FIELD,
        |value| match value {
            Value::Array(edits) => Some(edits),
            _ => None,
        },
    )?;
    let mut edit_tail = Vec::with_capacity(edits.len());
    for value in edits {
        let Value::Record(edit) = value else {
            return Err(Error::MalformedScalar);
        };
        let values = edit.to_values().map_err(|_| Error::MalformedScalar)?;
        let mut fields = large_value_edit_schema(kind).decode_values(&values)?;
        let insert_bytes = take_bytes(&mut fields, EDIT_INSERT_BYTES_FIELD)?;
        // Byte edits store no UTF-16 coordinates; the inserted UTF-16 length
        // is never stored and is derived from the inserted bytes.
        let (utf16_offset, delete_utf16_length) = if kind == LargeValueKind::Bytes {
            (0, 0)
        } else {
            (
                take_u64(&mut fields, EDIT_UTF16_OFFSET_FIELD)?,
                take_u64(&mut fields, EDIT_DELETE_UTF16_LENGTH_FIELD)?,
            )
        };
        let insert_utf16_length = match kind {
            LargeValueKind::Bytes => 0,
            LargeValueKind::String | LargeValueKind::Json => u64::try_from(
                std::str::from_utf8(&insert_bytes)
                    .map_err(|_| Error::InvalidUtf8)?
                    .encode_utf16()
                    .count(),
            )
            .map_err(|_| Error::MetricOverflow)?,
        };
        edit_tail.push(ReplaceEdit {
            offset: take_u64(&mut fields, EDIT_OFFSET_FIELD)?,
            delete_length: take_u64(&mut fields, EDIT_DELETE_LENGTH_FIELD)?,
            insert_bytes,
            utf16_offset,
            delete_utf16_length,
            insert_utf16_length,
        });
    }
    let value = LargeValueRef {
        kind,
        format_version,
        logical_hash,
        root: node_ref_from_value(&root)?,
        byte_length,
        utf16_length,
        edit_tail,
    };
    format.validate_descriptor(&value)?;
    Ok(value)
}

fn large_value_kind_tag(kind: LargeValueKind) -> u8 {
    match kind {
        LargeValueKind::Bytes => LARGE_VALUE_REF_BYTES_TAG,
        LargeValueKind::String => LARGE_VALUE_REF_STRING_TAG,
        LargeValueKind::Json => LARGE_VALUE_REF_JSON_TAG,
    }
}

fn large_value_kind_from_tag(tag: u8) -> Result<LargeValueKind, Error> {
    match tag {
        LARGE_VALUE_REF_BYTES_TAG => Ok(LargeValueKind::Bytes),
        LARGE_VALUE_REF_STRING_TAG => Ok(LargeValueKind::String),
        LARGE_VALUE_REF_JSON_TAG => Ok(LargeValueKind::Json),
        _ => Err(Error::MalformedScalar),
    }
}

fn validate_descriptor(value: &LargeValueRef) -> Result<(), Error> {
    LargeValueFormat::from_version(value.format_version)?.validate_descriptor(value)
}

fn validate_descriptor_v1(value: &LargeValueRef) -> Result<(), Error> {
    validate_descriptor_shape_v1(value)?;
    validate_descriptor_tail_bounds(value)
}

fn validate_descriptor_tail_bounds(value: &LargeValueRef) -> Result<(), Error> {
    // A tail fits one epoch, except for the one edit that may push it over
    // before it is consolidated.
    let Some((_, settled)) = value.edit_tail.split_last() else {
        return Ok(());
    };
    if settled.len() > MAX_EDIT_COUNT {
        return Err(Error::MalformedScalar);
    }
    let mut tail_bytes = 0_usize;
    for edit in settled {
        tail_bytes = tail_bytes
            .checked_add(edit.insert_bytes.len())
            .ok_or(Error::MetricOverflow)?;
        if tail_bytes > epoch::EPOCH_EDIT_BYTES {
            return Err(Error::MalformedScalar);
        }
    }
    Ok(())
}

fn validate_descriptor_shape(value: &LargeValueRef) -> Result<(), Error> {
    LargeValueFormat::from_version(value.format_version)?.validate_descriptor_shape(value)
}

fn validate_descriptor_shape_v1(value: &LargeValueRef) -> Result<(), Error> {
    let mut byte_length = value.byte_length;
    let mut utf16_length = value.utf16_length;
    for edit in value.edit_tail.iter().rev() {
        let inserted = u64::try_from(edit.insert_bytes.len()).map_err(|_| Error::MetricOverflow)?;
        if edit
            .offset
            .checked_add(inserted)
            .ok_or(Error::MetricOverflow)?
            > byte_length
        {
            return Err(Error::MalformedScalar);
        }
        byte_length = byte_length
            .checked_sub(inserted)
            .and_then(|n| n.checked_add(edit.delete_length))
            .ok_or(Error::MetricOverflow)?;
        match value.kind {
            LargeValueKind::Bytes => {
                if edit.utf16_offset != 0
                    || edit.delete_utf16_length != 0
                    || edit.insert_utf16_length != 0
                {
                    return Err(Error::MalformedScalar);
                }
            }
            LargeValueKind::String | LargeValueKind::Json => {
                let actual_insert = u64::try_from(
                    std::str::from_utf8(&edit.insert_bytes)
                        .map_err(|_| Error::InvalidUtf8)?
                        .encode_utf16()
                        .count(),
                )
                .map_err(|_| Error::MetricOverflow)?;
                if actual_insert != edit.insert_utf16_length {
                    return Err(Error::MalformedScalar);
                }
                let current = utf16_length.ok_or(Error::MalformedScalar)?;
                if edit
                    .utf16_offset
                    .checked_add(edit.insert_utf16_length)
                    .ok_or(Error::MetricOverflow)?
                    > current
                {
                    return Err(Error::MalformedScalar);
                }
                utf16_length = Some(
                    current
                        .checked_sub(edit.insert_utf16_length)
                        .and_then(|n| n.checked_add(edit.delete_utf16_length))
                        .ok_or(Error::MetricOverflow)?,
                );
            }
        }
    }
    match value.kind {
        LargeValueKind::Bytes if value.utf16_length.is_some() => Err(Error::MalformedScalar),
        LargeValueKind::String | LargeValueKind::Json if value.utf16_length.is_none() => {
            Err(Error::MalformedScalar)
        }
        _ => Ok(()),
    }
}

/// Recover the tail-free descriptor of `value`'s immutable base tree by
/// undoing each tail edit's length effects in reverse order. This performs no
/// reads; callers compare the result against the authenticated root node.
/// The descriptor of `value`'s immutable base tree: its tail undone.
pub fn tail_base_descriptor(value: &LargeValueRef) -> Result<LargeValueRef, Error> {
    validate_descriptor(value)?;
    let mut replay = value.clone();
    for edit in value.edit_tail.iter().rev() {
        let inserted = u64::try_from(edit.insert_bytes.len()).map_err(|_| Error::MetricOverflow)?;
        replay.byte_length = replay
            .byte_length
            .checked_sub(inserted)
            .and_then(|length| length.checked_add(edit.delete_length))
            .ok_or(Error::MetricOverflow)?;
        replay.utf16_length = match replay.kind {
            LargeValueKind::Bytes => None,
            LargeValueKind::String | LargeValueKind::Json => Some(
                replay
                    .utf16_length
                    .ok_or(Error::MalformedScalar)?
                    .checked_sub(edit.insert_utf16_length)
                    .and_then(|length| length.checked_add(edit.delete_utf16_length))
                    .ok_or(Error::MetricOverflow)?,
            ),
        };
    }
    replay.edit_tail.clear();
    validate_descriptor(&replay)?;
    Ok(replay)
}

/// Reconstruct and replay an untrusted descriptor's tail against its immutable
/// base tree. Shape validation alone cannot prove that text UTF-16 coordinates
/// describe the same byte splice, or that a JSON edit is a whole-value replace.
pub fn validate_edit_tail_attempt(
    value: &LargeValueRef,
    inputs: &mut ChunkInputs,
) -> Result<(), AttemptError> {
    let mut replay = tail_base_descriptor(value)?;

    for expected in &value.edit_tail {
        let outcome = replace_tail_with_bounds_attempt(
            &replay,
            expected.offset,
            expected.delete_length,
            expected.insert_bytes.clone(),
            inputs,
            false,
        )?;
        let next = match outcome {
            TailEditOutcome::Updated(next) | TailEditOutcome::ConsolidationRequired(next) => next,
        };
        if next.edit_tail.last() != Some(expected) {
            return Err(Error::DescriptorMismatch.into());
        }
        replay = next;
    }
    if &replay != value {
        return Err(Error::DescriptorMismatch.into());
    }
    Ok(())
}

#[cfg(test)]
fn stage_node(
    node: ChunkNode,
    metrics: NodeMetrics,
    staged_chunks: &mut Vec<StagedChunk>,
) -> Result<BuiltNode, Error> {
    let mut stager = TreeStager::new(node_kind(&node), |_| true);
    let built = stager.node(node)?;
    if built.metrics != metrics {
        return Err(Error::DescriptorMismatch);
    }
    staged_chunks.append(&mut stager.staged);
    Ok(built)
}

/// Builds content-defined tree levels. `fresh` says of each sealed node's
/// hash whether its chunk is new (and staged) or already stored (reused).
struct TreeStager<F> {
    kind: LargeValueKind,
    fresh: F,
    staged: Vec<StagedChunk>,
}

impl<F: FnMut(ContentHash) -> bool> TreeStager<F> {
    fn new(kind: LargeValueKind, fresh: F) -> Self {
        Self {
            kind,
            fresh,
            staged: Vec::new(),
        }
    }

    fn node(&mut self, node: ChunkNode) -> Result<BuiltNode, Error> {
        validate_untyped_node_structure(&node)?;
        let metrics = node_metrics(self.kind, &node)?;
        let (built, chunk) = seal_node(&node, metrics, &mut self.fresh)?;
        self.staged.extend(chunk);
        Ok(built)
    }

    fn leaves(&mut self, bytes: &[u8]) -> Result<Vec<BuiltNode>, Error> {
        leaf_ranges(self.kind, bytes)?
            .into_iter()
            .map(|range| self.node(leaf_node(self.kind, bytes[range].to_vec())))
            .collect()
    }

    fn branches(&mut self, level: &[BuiltNode]) -> Result<Vec<BuiltNode>, Error> {
        branch_ranges(self.kind, level)?
            .into_iter()
            .map(|range| {
                self.node(ChunkNode::Branch {
                    format: FORMAT_VERSION,
                    kind: self.kind,
                    children: level[range].iter().map(BranchChild::from).collect(),
                })
            })
            .collect()
    }

    /// Stage parent levels over `level`, built at `depth`, until one root
    /// remains; a tree deeper than [`MAX_TREE_DEPTH`] fails with `too_deep`.
    fn root(
        &mut self,
        mut level: Vec<BuiltNode>,
        mut depth: usize,
        too_deep: Error,
    ) -> Result<BuiltNode, Error> {
        while level.len() > 1 {
            depth += 1;
            if depth > MAX_TREE_DEPTH {
                return Err(too_deep);
            }
            level = self.branches(&level)?;
        }
        level.pop().ok_or(Error::MalformedNode)
    }
}

/// A stager that stages only nodes not already in `existing`.
fn reusing_stager(
    kind: LargeValueKind,
    existing: &std::collections::BTreeSet<ContentHash>,
) -> TreeStager<impl FnMut(ContentHash) -> bool + '_> {
    TreeStager::new(kind, move |hash| !existing.contains(&hash))
}

fn leaf_node(kind: LargeValueKind, bytes: Vec<u8>) -> ChunkNode {
    ChunkNode::Leaf {
        format: FORMAT_VERSION,
        kind,
        bytes,
    }
}

fn load_authenticated_node_attempt(
    format_version: u8,
    kind: LargeValueKind,
    node_ref: &NodeRef,
    expected_logical_hash: ContentHash,
    inputs: &mut ChunkInputs,
) -> Result<ChunkNode, AttemptError> {
    let request = ChunkRequest {
        object_hash: node_ref.object_hash.0,
    };
    let chunk = inputs.loaded_chunk(request)?;
    let node = if chunk.verifies_object_hash(&node_ref.object_hash.0) {
        decode_object_verified_node_for_format(format_version, kind, chunk.bytes())?
    } else {
        decode_node_for_format(format_version, kind, node_ref.object_hash, chunk.bytes())?
    };
    if node_logical_hash(&node) != expected_logical_hash {
        return Err(Error::DescriptorMismatch.into());
    }
    Ok(node)
}

/// Descend from `node_ref` to one leaf, letting `choose` pick a child of each
/// branch and recording the branches on `path`. A blocked read fails at once.
fn descend_attempt(
    value: &LargeValueRef,
    (mut node_ref, mut expected_hash): (NodeRef, ContentHash),
    mut path: Vec<PathFrame>,
    max_depth: usize,
    inputs: &mut ChunkInputs,
    mut choose: impl FnMut(&[BranchChild]) -> Result<usize, Error>,
) -> Result<LocatedLeaf, AttemptError> {
    loop {
        if path.len() > max_depth {
            return Err(Error::InvalidTree.into());
        }
        match load_authenticated_node_attempt(
            value.format_version,
            value.kind,
            &node_ref,
            expected_hash,
            inputs,
        )? {
            ChunkNode::Leaf { bytes, .. } => {
                return Ok(LocatedLeaf {
                    node_ref,
                    bytes,
                    start: 0,
                    path,
                });
            }
            ChunkNode::Branch { children, .. } => {
                let selected = choose(&children)?;
                let child = &children[selected];
                let parent = std::mem::replace(&mut node_ref, child.node_ref.clone());
                expected_hash = child.logical_hash;
                path.push(PathFrame {
                    node_ref: parent,
                    children,
                    selected,
                });
            }
        }
    }
}

fn locate_leaf_attempt(
    value: &LargeValueRef,
    offset: u64,
    base_length: u64,
    inputs: &mut ChunkInputs,
) -> Result<LocatedLeaf, AttemptError> {
    if offset >= base_length {
        return Err(Error::MalformedScalar.into());
    }
    let root = (value.root.clone(), value.logical_hash);
    let mut start = 0_u64;
    let mut leaf = descend_attempt(
        value,
        root,
        Vec::new(),
        MAX_TREE_DEPTH,
        inputs,
        |children| {
            children
                .iter()
                .position(|child| {
                    let end = start.saturating_add(child.metrics.byte_length);
                    let found = offset < end;
                    if !found {
                        start = end;
                    }
                    found
                })
                .ok_or(Error::DescriptorMismatch)
        },
    )?;
    if offset >= start + leaf.bytes.len() as u64 {
        return Err(Error::DescriptorMismatch.into());
    }
    leaf.start = start;
    Ok(leaf)
}

fn next_leaf_attempt(
    value: &LargeValueRef,
    current: &LocatedLeaf,
    inputs: &mut ChunkInputs,
) -> Result<LocatedLeaf, AttemptError> {
    let mut path = current.path.clone();
    let child = loop {
        let frame = path.last_mut().ok_or(Error::DescriptorMismatch)?;
        if frame.selected + 1 < frame.children.len() {
            frame.selected += 1;
            let child = &frame.children[frame.selected];
            break (child.node_ref.clone(), child.logical_hash);
        }
        path.pop();
    };
    let start = current
        .start
        .checked_add(current.bytes.len() as u64)
        .ok_or(Error::MetricOverflow)?;
    let mut leaf = descend_attempt(value, child, path, usize::MAX, inputs, |children| {
        if children.is_empty() {
            return Err(Error::MalformedNode);
        }
        Ok(0)
    })?;
    leaf.start = start;
    Ok(leaf)
}

pub fn decode_node(
    kind: LargeValueKind,
    expected_hash: ContentHash,
    encoded: &[u8],
) -> Result<ChunkNode, Error> {
    decode_node_for_format(FORMAT_VERSION, kind, expected_hash, encoded)
}

/// Decode a node through the exact format selected by its owner descriptor.
/// This is the descriptor-led dispatch boundary: callers with a descriptor
/// never probe another codec after a failure.
fn decode_node_for_format(
    format_version: u8,
    kind: LargeValueKind,
    expected_hash: ContentHash,
    encoded: &[u8],
) -> Result<ChunkNode, Error> {
    if encoded.len() > MAX_ENCODED_NODE_BYTES {
        return Err(Error::MalformedNode);
    }
    if object_hash(encoded) != expected_hash {
        return Err(Error::ObjectHashMismatch);
    }
    decode_object_verified_node_for_format(format_version, kind, encoded)
}

/// The bytes have already passed object-hash verification. Descriptor format,
/// canonical encoding, and kind remain independent checks on every access.
fn decode_object_verified_node_for_format(
    format_version: u8,
    kind: LargeValueKind,
    encoded: &[u8],
) -> Result<ChunkNode, Error> {
    if encoded.len() > MAX_ENCODED_NODE_BYTES {
        return Err(Error::MalformedNode);
    }
    // Select both contracts before V1 binds any variable node field. In
    // particular, a descriptor-led upload must not classify a hash-valid
    // future branch with an incompatible V1 payload as a malformed V1 node.
    let descriptor_format = LargeValueFormat::from_version(format_version)?;
    let node_format = preflight_node_format(encoded)?;
    if descriptor_format != node_format {
        return Err(Error::DescriptorMismatch);
    }
    let node = descriptor_format.decode_node(encoded)?;
    let encoded_kind = match &node {
        ChunkNode::Leaf { kind, .. } | ChunkNode::Branch { kind, .. } => *kind,
    };
    if encoded_kind != kind {
        return Err(Error::DescriptorMismatch);
    }
    Ok(node)
}

pub fn decode_authenticated_node(
    expected_hash: ContentHash,
    encoded: &[u8],
) -> Result<ChunkNode, Error> {
    decode_node_untyped_authenticated(expected_hash, encoded)
}

/// Authenticate and canonically decode an immutable-node envelope before a
/// referencing descriptor is available. Descriptor-bound reads additionally
/// apply [`decode_node`]'s kind check.
pub fn decode_node_untyped_authenticated(
    expected_hash: ContentHash,
    encoded: &[u8],
) -> Result<ChunkNode, Error> {
    // This path is also used while installing durable metadata, before a row
    // descriptor supplies the schema-derived kind. Keep the resource ceiling
    // ahead of authentication so an oversized stored object cannot make us
    // hash it merely because it has no descriptor yet.
    if encoded.len() > MAX_ENCODED_NODE_BYTES {
        return Err(Error::MalformedNode);
    }
    if object_hash(encoded) != expected_hash {
        return Err(Error::ObjectHashMismatch);
    }
    // Durable metadata has no owner descriptor. Read only the canonical enum
    // tag and its fixed first format byte to select a codec before V1 binds
    // any variable node field. No fallback or try-current decoding is
    // permitted here.
    preflight_node_format(encoded)?.decode_node(encoded)
}

/// Canonically decode an immutable-node envelope whose bytes already passed
/// object-hash verification (a node read back from the store that verified
/// it on the way in), without hashing them again.
pub fn decode_node_untyped_verified(encoded: &[u8]) -> Result<ChunkNode, Error> {
    if encoded.len() > MAX_ENCODED_NODE_BYTES {
        return Err(Error::MalformedNode);
    }
    preflight_node_format(encoded)?.decode_node(encoded)
}

/// Encode a chunk node using the ordinary canonical enum/record algebra.
pub fn encode_node(node: &ChunkNode) -> Result<Vec<u8>, Error> {
    LargeValueFormat::from_version(node_format(node))?.encode_node(node)
}

/// The frozen v1 node codec. Future codecs remain separate functions selected
/// by [`LargeValueFormat`], even if they initially share record machinery.
fn encode_node_v1(node: &ChunkNode) -> Result<Vec<u8>, Error> {
    let schema = chunk_node_schema(node_kind(node));
    let value = match node {
        ChunkNode::Leaf {
            format,
            kind,
            bytes,
        } => EnumValue::create(
            0,
            schema.case(0).map_err(|_| Error::MalformedNode)?.payload,
            &[
                Value::U8(*format),
                Value::U8(large_value_kind_tag(*kind)),
                Value::Bytes(bytes.clone()),
            ],
        ),
        ChunkNode::Branch {
            format,
            kind,
            children,
        } => {
            let child_schema = chunk_node_child_schema(*kind);
            let children = children
                .iter()
                .map(|child| {
                    let mut fields = vec![
                        Value::Bytes(child.node_ref.object_hash.0.to_vec()),
                        Value::U64(child.metrics.byte_length),
                        Value::Bytes(child.logical_hash.0.to_vec()),
                    ];
                    match (kind, child.metrics.utf16_length) {
                        (LargeValueKind::Bytes, None) => {}
                        (LargeValueKind::String | LargeValueKind::Json, Some(length)) => {
                            fields.push(Value::U64(length));
                        }
                        _ => return Err(Error::MalformedNode),
                    }
                    child_schema
                        .create(&fields)
                        .map(|bytes| {
                            Value::Record(crate::records::OwnedRecord::new(bytes, child_schema))
                        })
                        .map_err(|_| Error::MalformedNode)
                })
                .collect::<Result<Vec<_>, _>>()?;
            EnumValue::create(
                1,
                schema.case(1).map_err(|_| Error::MalformedNode)?.payload,
                &[
                    Value::U8(*format),
                    Value::U8(large_value_kind_tag(*kind)),
                    Value::Array(children),
                ],
            )
        }
    }
    .map_err(|_| Error::MalformedNode)?;
    crate::records::encode_single_field_value(
        &Value::Enum(value),
        &ValueType::Enum(Box::new(schema)),
    )
    .map_err(|_| Error::MalformedNode)
}

fn chunk_node_child_schema(kind: LargeValueKind) -> RecordDescriptor {
    let mut fields = vec![
        ("object_hash", ValueType::raw_bytes()),
        ("byte_length", ValueType::U64),
        ("logical_hash", ValueType::raw_bytes()),
    ];
    if kind != LargeValueKind::Bytes {
        fields.push(("utf16_length", ValueType::U64));
    }
    RecordDescriptor::new(fields)
}

fn chunk_node_schema(kind: LargeValueKind) -> EnumSchema {
    EnumSchema::new(
        "groove.internal.large_value.chunk_node",
        [
            EnumCase::new(
                "Leaf",
                RecordDescriptor::new([
                    ("format", ValueType::U8),
                    ("kind", ValueType::U8),
                    ("bytes", ValueType::raw_bytes()),
                ]),
            ),
            EnumCase::new(
                "Branch",
                RecordDescriptor::new([
                    ("format", ValueType::U8),
                    ("kind", ValueType::U8),
                    (
                        "children",
                        ValueType::Array(Box::new(ValueType::Record(Box::new(
                            chunk_node_child_schema(kind),
                        )))),
                    ),
                ]),
            ),
        ],
    )
    .expect("fixed chunk-node enum schema is valid")
}

/// Decode the authenticated chunk payload representation without interpreting
/// its schema-derived logical kind. Exact canonical re-encoding rejects any
/// alternate or trailing representation.
fn decode_canonical_node_v1(encoded: &[u8]) -> Result<ChunkNode, Error> {
    if encoded.len() > MAX_ENCODED_NODE_BYTES {
        return Err(Error::MalformedNode);
    }
    let (tag, payload) =
        crate::records::split_variant_record(encoded).map_err(|_| Error::MalformedNode)?;
    if tag == 0 {
        let [format, kind, bytes @ ..] = payload else {
            return Err(Error::MalformedNode);
        };
        let kind = large_value_kind_from_tag(*kind).map_err(|_| Error::MalformedNode)?;
        if bytes.len() > LEAF_MAX_BYTES {
            return Err(Error::MalformedNode);
        }
        // V1's ordinary Leaf record is exactly two fixed u8 fields followed
        // by its sole raw-bytes field. Its canonical re-encoding is therefore
        // this header followed by the unchanged payload, with no offset or
        // length table. Compare those segments directly instead of allocating
        // an owned EnumValue, decoded Values and another full encoded node.
        let canonical_header = [0, *format, large_value_kind_tag(kind)];
        if encoded.strip_prefix(&canonical_header) != Some(bytes) {
            return Err(Error::MalformedNode);
        }
        let node = ChunkNode::Leaf {
            format: *format,
            kind,
            bytes: bytes.to_vec(),
        };
        validate_untyped_node_structure(&node)?;
        return Ok(node);
    }
    // The fixed kind byte selects the child layout before binding any children.
    let kind = payload.get(1).copied().ok_or(Error::MalformedNode)?;
    let kind = large_value_kind_from_tag(kind).map_err(|_| Error::MalformedNode)?;
    let schema = chunk_node_schema(kind);
    preflight_node_bounds(encoded, &schema)?;
    let value =
        crate::records::decode_single_field_value(encoded, &ValueType::Enum(Box::new(schema)))
            .map_err(|_| Error::MalformedNode)?;
    let Value::Enum(value) = value else {
        return Err(Error::MalformedNode);
    };
    let fields = value
        .record()
        .to_values()
        .map_err(|_| Error::MalformedNode)?;
    let node = match (value.tag(), fields.as_slice()) {
        (0, [Value::U8(format), Value::U8(kind), Value::Bytes(bytes)]) => ChunkNode::Leaf {
            format: *format,
            kind: large_value_kind_from_tag(*kind).map_err(|_| Error::MalformedNode)?,
            bytes: bytes.clone(),
        },
        (1, [Value::U8(format), Value::U8(_), Value::Array(children)]) => {
            let children = children
                .iter()
                .map(|child| {
                    let Value::Record(child) = child else {
                        return Err(Error::MalformedNode);
                    };
                    let fields = child.to_values().map_err(|_| Error::MalformedNode)?;
                    let (object_hash, byte_length, logical_hash, utf16_length) =
                        match (kind, fields.as_slice()) {
                            (
                                LargeValueKind::Bytes,
                                [object_hash, Value::U64(byte_length), logical_hash],
                            ) => (object_hash, byte_length, logical_hash, None),
                            (
                                LargeValueKind::String | LargeValueKind::Json,
                                [
                                    object_hash,
                                    Value::U64(byte_length),
                                    logical_hash,
                                    Value::U64(utf16_length),
                                ],
                            ) => (object_hash, byte_length, logical_hash, Some(*utf16_length)),
                            _ => return Err(Error::MalformedNode),
                        };
                    Ok(BranchChild {
                        node_ref: NodeRef {
                            object_hash: ContentHash(
                                raw_bytes(object_hash).map_err(|_| Error::MalformedNode)?,
                            ),
                        },
                        metrics: NodeMetrics {
                            byte_length: *byte_length,
                            utf16_length,
                        },
                        logical_hash: ContentHash(
                            raw_bytes(logical_hash).map_err(|_| Error::MalformedNode)?,
                        ),
                    })
                })
                .collect::<Result<Vec<_>, Error>>()?;
            ChunkNode::Branch {
                format: *format,
                kind,
                children,
            }
        }
        _ => return Err(Error::MalformedNode),
    };
    let canonical = encode_node_v1(&node)?;
    if canonical != encoded {
        return Err(Error::MalformedNode);
    }
    validate_untyped_node_structure(&node)?;
    Ok(node)
}

/// Decode a self-describing canonical node for internal metadata observers.
/// Descriptor-guided reads use [`decode_node_for_format`] instead, so their
/// persisted descriptor selects the codec before node interpretation.
#[cfg(test)]
pub fn decode_canonical_node(encoded: &[u8]) -> Result<ChunkNode, Error> {
    preflight_node_format(encoded)?.decode_node(encoded)
}

fn preflight_node_bounds(encoded: &[u8], schema: &EnumSchema) -> Result<(), Error> {
    let (tag, payload) =
        crate::records::split_variant_record(encoded).map_err(|_| Error::MalformedNode)?;
    if tag == 1 {
        let descriptor = schema.case(1).map_err(|_| Error::MalformedNode)?.payload;
        let span = descriptor
            .field_span(payload, 2)
            .map_err(|_| Error::MalformedNode)?;
        let array = &payload[span];
        let count = array
            .get(..4)
            .and_then(|bytes| <[u8; 4]>::try_from(bytes).ok())
            .map(u32::from_le_bytes)
            .ok_or(Error::MalformedNode)?;
        if usize::try_from(count).map_err(|_| Error::MalformedNode)? > BRANCH_MAX_CHILDREN {
            return Err(Error::MalformedNode);
        }
    }
    Ok(())
}

/// Read precisely the self-describing node selector without binding either
/// leaf bytes or branch children. This is shared by descriptor-led admission
/// and descriptor-free durable-metadata observers: future node bytes must
/// select (and reject through) their own codec even when their remaining bytes
/// are not a valid V1 record.
fn preflight_node_format(encoded: &[u8]) -> Result<LargeValueFormat, Error> {
    let (tag, payload) =
        crate::records::split_variant_record(encoded).map_err(|_| Error::MalformedNode)?;
    match tag {
        0 | 1 => {}
        _ => return Err(Error::MalformedNode),
    }
    let version = *payload.first().ok_or(Error::MalformedNode)?;
    LargeValueFormat::from_version(version)
}

fn validate_untyped_node_structure(node: &ChunkNode) -> Result<(), Error> {
    match node {
        ChunkNode::Leaf {
            format,
            kind,
            bytes,
        } => {
            check_format(*format)?;
            if bytes.len() > LEAF_MAX_BYTES {
                return Err(Error::MalformedNode);
            }
            if *kind != LargeValueKind::Bytes {
                std::str::from_utf8(bytes).map_err(|_| Error::InvalidUtf8)?;
            }
        }
        ChunkNode::Branch {
            format,
            kind,
            children,
        } => {
            check_format(*format)?;
            if children.is_empty()
                || children.len() > BRANCH_MAX_CHILDREN
                || children.iter().any(|child| child.metrics.byte_length == 0)
            {
                return Err(Error::MalformedNode);
            }
            let mut child_metrics = children.iter().map(|child| child.metrics);
            let first_metrics = child_metrics.next().ok_or(Error::MalformedNode)?;
            let _ = child_metrics.try_fold(first_metrics, add_metrics)?;
            if *kind == LargeValueKind::Bytes
                && children
                    .iter()
                    .any(|child| child.metrics.utf16_length.is_some())
            {
                return Err(Error::MalformedNode);
            }
            if *kind != LargeValueKind::Bytes
                && children
                    .iter()
                    .any(|child| child.metrics.utf16_length.is_none())
            {
                return Err(Error::MalformedNode);
            }
        }
    }
    Ok(())
}

pub fn object_hash(encoded: &[u8]) -> ContentHash {
    hash_domain(b"groove-large-object-v1", encoded)
}

fn node_logical_hash(node: &ChunkNode) -> ContentHash {
    let (format, kind) = match node {
        ChunkNode::Leaf { format, kind, .. } | ChunkNode::Branch { format, kind, .. } => {
            (*format, *kind)
        }
    };
    bind_grouping_hash(format, kind, node_grouping_hash(node))
}

/// Content-defined grouping deliberately remains stable across semantic kinds
/// and representation-format bumps. The public logical identity binds those
/// dimensions separately, while this private reversible component lets later
/// localized consolidation make the same boundaries as a fresh construction
/// without persisting a second hash in every branch child.
fn node_grouping_hash(node: &ChunkNode) -> ContentHash {
    match node {
        ChunkNode::Leaf { bytes, .. } => hash_domain(b"groove-large-leaf-logical-v1", bytes),
        ChunkNode::Branch {
            format,
            kind,
            children,
        } => {
            let mut descriptor = Vec::with_capacity(children.len() * 48);
            for child in children {
                descriptor.extend_from_slice(
                    &grouping_hash_from_logical(*format, *kind, child.logical_hash).0,
                );
                descriptor.extend_from_slice(&child.metrics.byte_length.to_le_bytes());
                descriptor.extend_from_slice(
                    &child.metrics.utf16_length.unwrap_or(u64::MAX).to_le_bytes(),
                );
            }
            hash_domain(b"groove-large-branch-logical-v1", &descriptor)
        }
    }
}

fn bind_grouping_hash(format: u8, kind: LargeValueKind, grouping_hash: ContentHash) -> ContentHash {
    let mask = kind_format_hash_mask(format, kind);
    ContentHash(std::array::from_fn(|index| {
        grouping_hash.0[index] ^ mask.0[index]
    }))
}

fn grouping_hash_from_logical(
    format: u8,
    kind: LargeValueKind,
    logical_hash: ContentHash,
) -> ContentHash {
    // XOR is its own inverse. The mask is a full-width, domain-separated hash,
    // so this retains 256-bit cross-kind/cross-format separation while allowing
    // deterministic grouping to recover its format-neutral content component.
    bind_grouping_hash(format, kind, logical_hash)
}

fn kind_format_hash_mask(format: u8, kind: LargeValueKind) -> ContentHash {
    hash_domain(
        b"groove-large-logical-kind-format-v1",
        &[format, large_value_kind_tag(kind)],
    )
}

fn node_metrics(kind: LargeValueKind, node: &ChunkNode) -> Result<NodeMetrics, Error> {
    if node_kind(node) != kind {
        return Err(Error::DescriptorMismatch);
    }
    match node {
        ChunkNode::Leaf { bytes, .. } => metrics(kind, bytes),
        ChunkNode::Branch { children, .. } => {
            let mut metrics = children.iter().map(|child| child.metrics);
            let first = metrics.next().ok_or(Error::MalformedNode)?;
            metrics.try_fold(first, add_metrics)
        }
    }
}

fn node_format(node: &ChunkNode) -> u8 {
    match node {
        ChunkNode::Leaf { format, .. } | ChunkNode::Branch { format, .. } => *format,
    }
}

fn node_kind(node: &ChunkNode) -> LargeValueKind {
    match node {
        ChunkNode::Leaf { kind, .. } | ChunkNode::Branch { kind, .. } => *kind,
    }
}

/// Attempt to materialize one logical value using the evaluator-owned request
/// set. Missing chunks are registered in `inputs` and reported only as the
/// evaluator-internal blocked outcome.
pub fn materialize_attempt(
    value: &LargeValueRef,
    inputs: &mut ChunkInputs,
) -> Result<Vec<u8>, AttemptError> {
    /// Collects every leaf, checking each node against its claimed metrics.
    /// Children are never positioned, so their walk length is zero.
    struct Materialize(LargeValueKind, Vec<Vec<u8>>);
    impl LogicalVisitor for Materialize {
        fn leaf(&mut self, span: &WalkSpan, bytes: Vec<u8>) -> Result<(), Error> {
            if span
                .metrics
                .is_some_and(|expected| metrics(self.0, &bytes).ok() != Some(expected))
            {
                return Err(Error::DescriptorMismatch);
            }
            self.1.push(bytes);
            Ok(())
        }
        fn branch(&mut self, span: &WalkSpan, children: &[BranchChild]) -> Result<(), Error> {
            if let Some(expected) = span.metrics {
                let mut child_metrics = children.iter().map(|child| child.metrics);
                let first = child_metrics.next().ok_or(Error::MalformedNode)?;
                if child_metrics.try_fold(first, add_metrics)? != expected {
                    return Err(Error::DescriptorMismatch);
                }
            }
            Ok(())
        }
        fn child_len(&self, _: &NodeMetrics) -> Result<u64, Error> {
            Ok(0)
        }
    }

    check_format(value.format_version)?;
    let root = WalkSpan {
        start: 0,
        len: 0,
        metrics: value.edit_tail.is_empty().then_some(NodeMetrics {
            byte_length: value.byte_length,
            utf16_length: value.utf16_length,
        }),
    };
    let mut leaves = Materialize(value.kind, Vec::new());
    walk_logical(value, root, &mut leaves, inputs)?;
    let mut bytes =
        Vec::with_capacity(usize::try_from(value.byte_length).map_err(|_| Error::MetricOverflow)?);
    for leaf in leaves.1 {
        bytes.extend_from_slice(&leaf);
    }
    apply_edits(&mut bytes, &value.edit_tail)?;
    validate_logical(value.kind, &bytes)?;
    let actual_metrics = metrics(value.kind, &bytes)?;
    if actual_metrics.byte_length != value.byte_length
        || actual_metrics.utf16_length != value.utf16_length
    {
        return Err(Error::DescriptorMismatch.into());
    }
    #[cfg(feature = "testing")]
    FULL_MATERIALIZATIONS.with(|count| count.set(count.get() + 1));
    Ok(bytes)
}

#[cfg(feature = "testing")]
thread_local! {
    static FULL_MATERIALIZATIONS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Test-only count of complete large-value rebuilds on this thread.
///
/// A complete rebuild reads every chunk of a value and rejoins it into one
/// logical scalar. Range reads and descriptor-only paths never count. Tests
/// use deltas of this counter to prove that a path does not scale with value
/// size, which elapsed time cannot establish robustly.
#[cfg(feature = "testing")]
pub fn full_materializations_for_test() -> u64 {
    FULL_MATERIALIZATIONS.with(std::cell::Cell::get)
}

fn apply_edits(bytes: &mut Vec<u8>, edits: &[ReplaceEdit]) -> Result<(), Error> {
    for edit in edits {
        let start = usize::try_from(edit.offset).map_err(|_| Error::MetricOverflow)?;
        let delete = usize::try_from(edit.delete_length).map_err(|_| Error::MetricOverflow)?;
        let end = start.checked_add(delete).ok_or(Error::MetricOverflow)?;
        if end > bytes.len() {
            return Err(Error::MalformedNode);
        }
        bytes.splice(start..end, edit.insert_bytes.iter().copied());
    }
    Ok(())
}

#[derive(Clone, Debug)]
enum RangePiece {
    Base(std::ops::Range<u64>),
    Inserted(Vec<u8>),
}

/// The coordinates a range read is expressed in.
#[derive(Clone, Copy, Debug)]
enum RangeUnit {
    Bytes,
    Utf16,
}

impl RangeUnit {
    /// An edit's `(offset, inserted length, deleted length)` in this unit.
    fn edit_span(self, edit: &ReplaceEdit) -> Result<(u64, u64, u64), Error> {
        Ok(match self {
            Self::Bytes => (
                edit.offset,
                u64::try_from(edit.insert_bytes.len()).map_err(|_| Error::MetricOverflow)?,
                edit.delete_length,
            ),
            Self::Utf16 => (
                edit.utf16_offset,
                edit.insert_utf16_length,
                edit.delete_utf16_length,
            ),
        })
    }

    fn child_length(self, metrics: &NodeMetrics) -> Result<u64, Error> {
        match self {
            Self::Bytes => Ok(metrics.byte_length),
            Self::Utf16 => metrics.utf16_length.ok_or(Error::DescriptorMismatch),
        }
    }

    fn slice(self, bytes: &[u8], range: std::ops::Range<u64>) -> Result<Vec<u8>, Error> {
        match self {
            Self::Bytes => {
                let start = usize::try_from(range.start).map_err(|_| Error::MetricOverflow)?;
                let end = usize::try_from(range.end).map_err(|_| Error::MetricOverflow)?;
                Ok(bytes[start..end].to_vec())
            }
            Self::Utf16 => slice_utf16(bytes, range),
        }
    }
}

/// Read a byte range from the final logical value. Edit-tail insertions are
/// served directly and only intersecting base-tree paths are requested.
pub fn byte_range_attempt(
    value: &LargeValueRef,
    range: std::ops::Range<u64>,
    inputs: &mut ChunkInputs,
) -> Result<Vec<u8>, AttemptError> {
    range_attempt(value, range, RangeUnit::Bytes, inputs)
}

/// The smallest byte range of `value`'s base tree that a read of its
/// logical bytes `range` touches: `None` when the edit tail holds them
/// all. A replica asks for this range in one pull (SPEC 19.8).
pub fn base_byte_span(
    value: &LargeValueRef,
    range: std::ops::Range<u64>,
) -> Result<Option<std::ops::Range<u64>>, Error> {
    if range.start > range.end || range.end > value.byte_length {
        return Err(Error::MalformedScalar);
    }
    let mut span: Option<std::ops::Range<u64>> = None;
    for piece in map_final_range_to_base(range, &value.edit_tail, RangeUnit::Bytes)? {
        if let RangePiece::Base(range) = piece
            && !range.is_empty()
        {
            span = Some(match span {
                Some(span) => span.start.min(range.start)..span.end.max(range.end),
                None => range,
            });
        }
    }
    Ok(span)
}

/// [`byte_range_attempt`] in UTF-16 code-unit coordinates.
pub fn utf16_range_attempt(
    value: &LargeValueRef,
    range: std::ops::Range<u64>,
    inputs: &mut ChunkInputs,
) -> Result<Vec<u8>, AttemptError> {
    range_attempt(value, range, RangeUnit::Utf16, inputs)
}

fn range_attempt(
    value: &LargeValueRef,
    range: std::ops::Range<u64>,
    unit: RangeUnit,
    inputs: &mut ChunkInputs,
) -> Result<Vec<u8>, AttemptError> {
    let length = match unit {
        RangeUnit::Bytes => value.byte_length,
        RangeUnit::Utf16 => value.utf16_length.ok_or(Error::MalformedScalar)?,
    };
    if range.start > range.end || range.end > length {
        return Err(Error::MalformedScalar.into());
    }
    let mut output = Vec::new();
    read_pieces(value, range, length, unit, |piece, base_length| {
        match piece {
            RangePiece::Inserted(bytes) => output.extend(bytes),
            RangePiece::Base(range) => {
                output.extend(base_range_attempt(value, base_length, range, unit, inputs)?)
            }
        }
        Ok(())
    })?;
    Ok(output)
}

/// Map a final range onto the base tree and read each piece in order. A
/// blocked piece wins only after every piece has been tried.
fn read_pieces(
    value: &LargeValueRef,
    range: std::ops::Range<u64>,
    length: u64,
    unit: RangeUnit,
    mut read: impl FnMut(RangePiece, u64) -> Result<(), AttemptError>,
) -> Result<(), AttemptError> {
    let pieces = map_final_range_to_base(range, &value.edit_tail, unit)?;
    let base_length = base_length(length, &value.edit_tail, unit)?;
    let mut blocked = false;
    for piece in pieces {
        match read(piece, base_length) {
            Err(AttemptError::Blocked) => blocked = true,
            result => result?,
        }
    }
    if blocked {
        return Err(AttemptError::Blocked);
    }
    Ok(())
}

/// Resolve a final logical UTF-16 boundary to its final logical byte offset
/// without materializing the preceding text.  Prefix lengths are computed from
/// authenticated branch metrics, with only the logarithmically many boundary
/// leaves requested.  The binary search is over UTF-8 code-point boundaries so
/// a position inside a surrogate pair fails rather than being rounded.
pub fn utf16_offset_to_byte_attempt(
    value: &LargeValueRef,
    offset: u64,
    inputs: &mut ChunkInputs,
) -> Result<u64, AttemptError> {
    let total = value.utf16_length.ok_or(Error::MalformedScalar)?;
    if offset > total {
        return Err(Error::MalformedScalar.into());
    }
    if offset == 0 {
        return Ok(0);
    }
    if offset == total {
        return Ok(value.byte_length);
    }

    let mut low = 0_u64;
    let mut high = value.byte_length;
    while low < high {
        let midpoint = low + (high - low) / 2;
        let boundary = utf8_boundary_at_or_before_attempt(value, midpoint, inputs)?;
        let units = utf16_length_for_byte_range_attempt(value, 0..boundary, inputs)?;
        if units < offset {
            low = utf8_next_boundary_attempt(value, boundary, inputs)?;
        } else {
            high = boundary;
        }
    }
    let units = utf16_length_for_byte_range_attempt(value, 0..low, inputs)?;
    if units != offset {
        return Err(Error::MalformedScalar.into());
    }
    Ok(low)
}

fn utf8_boundary_at_or_before_attempt(
    value: &LargeValueRef,
    offset: u64,
    inputs: &mut ChunkInputs,
) -> Result<u64, AttemptError> {
    if offset == 0 || offset == value.byte_length {
        return Ok(offset);
    }
    let start = offset.saturating_sub(3);
    let bytes = byte_range_attempt(value, start..offset.saturating_add(1), inputs)?;
    let mut index = bytes.len().checked_sub(1).ok_or(Error::MalformedScalar)?;
    while bytes[index] & 0b1100_0000 == 0b1000_0000 {
        index = index.checked_sub(1).ok_or(Error::MalformedScalar)?;
    }
    start
        .checked_add(u64::try_from(index).map_err(|_| Error::MetricOverflow)?)
        .ok_or(Error::MetricOverflow.into())
}

fn utf8_next_boundary_attempt(
    value: &LargeValueRef,
    boundary: u64,
    inputs: &mut ChunkInputs,
) -> Result<u64, AttemptError> {
    if boundary >= value.byte_length {
        return Ok(value.byte_length);
    }
    let end = boundary.saturating_add(4).min(value.byte_length);
    let bytes = byte_range_attempt(value, boundary..end, inputs)?;
    let first = *bytes.first().ok_or(Error::MalformedScalar)?;
    let width = match first {
        0x00..=0x7f => 1,
        0xc2..=0xdf => 2,
        0xe0..=0xef => 3,
        0xf0..=0xf4 => 4,
        _ => return Err(Error::InvalidUtf8.into()),
    };
    let next = boundary.checked_add(width).ok_or(Error::MetricOverflow)?;
    if next > value.byte_length
        || usize::try_from(width).map_err(|_| Error::MetricOverflow)? > bytes.len()
    {
        return Err(Error::InvalidUtf8.into());
    }
    Ok(next)
}

fn slice_utf16(bytes: &[u8], range: std::ops::Range<u64>) -> Result<Vec<u8>, Error> {
    let text = std::str::from_utf8(bytes).map_err(|_| Error::InvalidUtf8)?;
    let mut units = 0_u64;
    let mut start = None;
    let mut end = None;
    for (byte, ch) in text
        .char_indices()
        .chain(std::iter::once((text.len(), '\0')))
    {
        if units == range.start {
            start = Some(byte);
        }
        if units == range.end {
            end = Some(byte);
            break;
        }
        if byte < text.len() {
            units = units
                .checked_add(ch.len_utf16() as u64)
                .ok_or(Error::MetricOverflow)?;
        }
        if units > range.start && start.is_none() || units > range.end {
            return Err(Error::MalformedScalar);
        }
    }
    let start = start.ok_or(Error::MalformedScalar)?;
    let end = end.ok_or(Error::MalformedScalar)?;
    Ok(bytes[start..end].to_vec())
}

/// Count UTF-16 units in a byte-coordinate range without hydrating complete
/// subtrees. Fully covered children contribute their authenticated aggregate
/// metric; only boundary leaves are decoded.
fn utf16_length_for_byte_range_attempt(
    value: &LargeValueRef,
    range: std::ops::Range<u64>,
    inputs: &mut ChunkInputs,
) -> Result<u64, AttemptError> {
    if value.kind == LargeValueKind::Bytes
        || range.start > range.end
        || range.end > value.byte_length
    {
        return Err(Error::MalformedScalar.into());
    }
    let mut total = 0_u64;
    let length = value.byte_length;
    read_pieces(
        value,
        range,
        length,
        RangeUnit::Bytes,
        |piece, base_length| {
            let length = match piece {
                RangePiece::Inserted(bytes) => {
                    utf16_length_of(std::str::from_utf8(&bytes).map_err(|_| Error::InvalidUtf8)?)?
                }
                RangePiece::Base(range) => {
                    base_utf16_length_for_byte_range_attempt(value, base_length, range, inputs)?
                }
            };
            total = total.checked_add(length).ok_or(Error::MetricOverflow)?;
            Ok(())
        },
    )?;
    Ok(total)
}

fn base_utf16_length_for_byte_range_attempt(
    value: &LargeValueRef,
    base_length: u64,
    range: std::ops::Range<u64>,
    inputs: &mut ChunkInputs,
) -> Result<u64, AttemptError> {
    /// Sums the UTF-16 metric of fully covered subtrees and counts the
    /// covered part of each boundary leaf. Spans are in bytes.
    struct Utf16Count(std::ops::Range<u64>, u64);
    impl Utf16Count {
        fn add(&mut self, units: u64) -> Result<(), Error> {
            self.1 = self.1.checked_add(units).ok_or(Error::MetricOverflow)?;
            Ok(())
        }
    }
    impl LogicalVisitor for Utf16Count {
        fn wants(&mut self, span: &WalkSpan) -> Result<bool, Error> {
            let (start, end) = (span.start, span.end()?);
            if self.0.start >= end || self.0.end <= start {
                return Ok(false);
            }
            if self.0.start <= start && end <= self.0.end {
                let units = span.metrics.and_then(|metrics| metrics.utf16_length);
                self.add(units.ok_or(Error::DescriptorMismatch)?)?;
                return Ok(false);
            }
            Ok(true)
        }
        fn leaf(&mut self, span: &WalkSpan, bytes: Vec<u8>) -> Result<(), Error> {
            let to_index = |offset: u64| usize::try_from(offset).map_err(|_| Error::MetricOverflow);
            let a = to_index(self.0.start.max(span.start) - span.start)?;
            let b = to_index(self.0.end.min(span.end()?) - span.start)?;
            let text = std::str::from_utf8(&bytes[a..b]).map_err(|_| Error::InvalidUtf8)?;
            self.add(utf16_length_of(text)?)
        }
        fn child_len(&self, metrics: &NodeMetrics) -> Result<u64, Error> {
            metrics.utf16_length.ok_or(Error::DescriptorMismatch)?;
            Ok(metrics.byte_length)
        }
    }

    if range.end > base_length {
        return Err(Error::MalformedScalar.into());
    }
    let base_utf16 = value.edit_tail.iter().rev().try_fold(
        value.utf16_length.ok_or(Error::MalformedScalar)?,
        |length, edit| {
            length
                .checked_sub(edit.insert_utf16_length)
                .and_then(|n| n.checked_add(edit.delete_utf16_length))
                .ok_or(Error::MetricOverflow)
        },
    )?;
    let root = WalkSpan {
        start: 0,
        len: base_length,
        metrics: Some(NodeMetrics {
            byte_length: base_length,
            utf16_length: Some(base_utf16),
        }),
    };
    let mut count = Utf16Count(range, 0);
    walk_logical(value, root, &mut count, inputs)?;
    Ok(count.1)
}

/// Compare an indirect logical scalar to inline source bytes, requesting one
/// bounded window at a time and stopping at the first decisive mismatch.
pub fn compare_inline_attempt(
    value: &LargeValueRef,
    inline: &[u8],
    inputs: &mut ChunkInputs,
) -> Result<std::cmp::Ordering, AttemptError> {
    validate_descriptor(value)?;
    if matches!(value.kind, LargeValueKind::String | LargeValueKind::Json) {
        std::str::from_utf8(inline).map_err(|_| Error::InvalidUtf8)?;
    }
    let common = value
        .byte_length
        .min(u64::try_from(inline.len()).map_err(|_| Error::MetricOverflow)?);
    let mut offset = 0_u64;
    while offset < common {
        let end = offset
            .saturating_add(u64::try_from(LEAF_MIN_BYTES).expect("leaf minimum fits u64"))
            .min(common);
        let indirect = byte_range_attempt(value, offset..end, inputs)?;
        let start = usize::try_from(offset).map_err(|_| Error::MetricOverflow)?;
        let end_usize = usize::try_from(end).map_err(|_| Error::MetricOverflow)?;
        match indirect.as_slice().cmp(&inline[start..end_usize]) {
            std::cmp::Ordering::Equal => offset = end,
            ordering => return Ok(ordering),
        }
    }
    Ok(value
        .byte_length
        .cmp(&u64::try_from(inline.len()).map_err(|_| Error::MetricOverflow)?))
}

#[derive(Debug)]
pub enum JsonPointerPrefix {
    Found(Option<serde_json::Value>),
    NeedMore,
    RequiresFullDocument,
}

/// Resolve pointers whose first component is an array index from a source
/// prefix. Once that complete array element has parsed, later source bytes
/// cannot change its value. Object-root pointers deliberately use the full
/// document path because serde_json's literal semantics retain the last
/// duplicate key.
pub fn json_pointer_prefix(source: &[u8], pointer: &str) -> Result<JsonPointerPrefix, Error> {
    if pointer.is_empty() {
        return Ok(JsonPointerPrefix::RequiresFullDocument);
    }
    let mut components = pointer
        .strip_prefix('/')
        .ok_or(Error::InvalidJson)?
        .split('/');
    let first = decode_json_pointer_component(components.next().ok_or(Error::InvalidJson)?)?;
    let Ok(target_index) = first.parse::<usize>() else {
        return Ok(JsonPointerPrefix::RequiresFullDocument);
    };
    if first != target_index.to_string() {
        return Ok(JsonPointerPrefix::RequiresFullDocument);
    }
    let remaining = components
        .map(decode_json_pointer_component)
        .collect::<Result<Vec<_>, _>>()?;
    let mut position = skip_json_ws(source, 0);
    if source.get(position) != Some(&b'[') {
        return Ok(JsonPointerPrefix::RequiresFullDocument);
    }
    position += 1;
    for index in 0..=target_index {
        position = skip_json_ws(source, position);
        match source.get(position) {
            None => return Ok(JsonPointerPrefix::NeedMore),
            Some(b']') => return Ok(JsonPointerPrefix::Found(None)),
            _ => {}
        }
        let mut values = serde_json::Deserializer::from_slice(&source[position..])
            .into_iter::<serde_json::Value>();
        let value = match values.next() {
            Some(Ok(value)) => value,
            Some(Err(error)) if error.is_eof() => return Ok(JsonPointerPrefix::NeedMore),
            Some(Err(_)) | None => return Err(Error::InvalidJson),
        };
        position = position
            .checked_add(values.byte_offset())
            .ok_or(Error::MetricOverflow)?;
        if index == target_index {
            let mut selected = Some(&value);
            for component in &remaining {
                selected = selected.and_then(|value| match value {
                    serde_json::Value::Object(map) => map.get(component),
                    serde_json::Value::Array(array) => component
                        .parse::<usize>()
                        .ok()
                        .filter(|index| component == &index.to_string())
                        .and_then(|index| array.get(index)),
                    _ => None,
                });
            }
            return Ok(JsonPointerPrefix::Found(selected.cloned()));
        }
        position = skip_json_ws(source, position);
        match source.get(position) {
            Some(b',') => position += 1,
            Some(b']') => return Ok(JsonPointerPrefix::Found(None)),
            None => return Ok(JsonPointerPrefix::NeedMore),
            _ => return Err(Error::InvalidJson),
        }
    }
    Err(Error::InvalidJson)
}

fn skip_json_ws(source: &[u8], mut position: usize) -> usize {
    while source
        .get(position)
        .is_some_and(|byte| matches!(byte, b' ' | b'\n' | b'\r' | b'\t'))
    {
        position += 1;
    }
    position
}

fn decode_json_pointer_component(component: &str) -> Result<String, Error> {
    let mut decoded = String::with_capacity(component.len());
    let mut chars = component.chars();
    while let Some(ch) = chars.next() {
        if ch != '~' {
            decoded.push(ch);
            continue;
        }
        match chars.next() {
            Some('0') => decoded.push('~'),
            Some('1') => decoded.push('/'),
            _ => return Err(Error::InvalidJson),
        }
    }
    Ok(decoded)
}

fn base_length(final_length: u64, edits: &[ReplaceEdit], unit: RangeUnit) -> Result<u64, Error> {
    edits.iter().rev().try_fold(final_length, |length, edit| {
        let (_, inserted, deleted) = unit.edit_span(edit)?;
        length
            .checked_sub(inserted)
            .and_then(|length| length.checked_add(deleted))
            .ok_or(Error::MetricOverflow)
    })
}

fn map_final_range_to_base(
    range: std::ops::Range<u64>,
    edits: &[ReplaceEdit],
    unit: RangeUnit,
) -> Result<Vec<RangePiece>, Error> {
    let mut pieces = vec![RangePiece::Base(range)];
    for edit in edits.iter().rev() {
        let (offset, inserted, deleted) = unit.edit_span(edit)?;
        let inserted_end = offset.checked_add(inserted).ok_or(Error::MetricOverflow)?;
        let mut previous = Vec::new();
        for piece in pieces {
            let RangePiece::Base(range) = piece else {
                previous.push(piece);
                continue;
            };
            if range.start < offset {
                previous.push(RangePiece::Base(range.start..range.end.min(offset)));
            }
            let overlap_start = range.start.max(offset);
            let overlap_end = range.end.min(inserted_end);
            if overlap_start < overlap_end {
                previous.push(RangePiece::Inserted(unit.slice(
                    &edit.insert_bytes,
                    (overlap_start - offset)..(overlap_end - offset),
                )?));
            }
            if range.end > inserted_end {
                let start = range.start.max(inserted_end);
                let map = |position: u64| {
                    position
                        .checked_sub(inserted)
                        .and_then(|n| n.checked_add(deleted))
                        .ok_or(Error::MetricOverflow)
                };
                previous.push(RangePiece::Base(map(start)?..map(range.end)?));
            }
        }
        pieces = previous;
    }
    Ok(pieces)
}

fn base_range_attempt(
    value: &LargeValueRef,
    base_length: u64,
    range: std::ops::Range<u64>,
    unit: RangeUnit,
    inputs: &mut ChunkInputs,
) -> Result<Vec<u8>, AttemptError> {
    /// Appends the in-range part of each intersecting leaf, in order.
    struct RangeRead(std::ops::Range<u64>, RangeUnit, Vec<u8>);
    impl LogicalVisitor for RangeRead {
        const EXACT_COVER: bool = true;
        fn wants(&mut self, span: &WalkSpan) -> Result<bool, Error> {
            Ok(self.0.start < span.end()? && self.0.end > span.start)
        }
        fn leaf(&mut self, span: &WalkSpan, bytes: Vec<u8>) -> Result<(), Error> {
            // UTF-16 slicing fails on its own past the end of the leaf.
            if matches!(self.1, RangeUnit::Bytes)
                && u64::try_from(bytes.len()).map_err(|_| Error::MetricOverflow)? != span.len
            {
                return Err(Error::DescriptorMismatch);
            }
            let (start, end) = (span.start, span.end()?);
            let part = self.1.slice(
                &bytes,
                (self.0.start.max(start) - start)..(self.0.end.min(end) - start),
            )?;
            self.2.extend(part);
            Ok(())
        }
        fn child_len(&self, metrics: &NodeMetrics) -> Result<u64, Error> {
            self.1.child_length(metrics)
        }
    }

    if range.end > base_length {
        return Err(Error::MalformedScalar.into());
    }
    if range.is_empty() {
        return Ok(Vec::new());
    }
    let root = WalkSpan {
        start: 0,
        len: base_length,
        metrics: None,
    };
    let mut read = RangeRead(range, unit, Vec::new());
    walk_logical(value, root, &mut read, inputs)?;
    Ok(read.2)
}

fn hash_domain(domain: &[u8], bytes: &[u8]) -> ContentHash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&(domain.len() as u64).to_le_bytes());
    hasher.update(domain);
    hasher.update(bytes);
    ContentHash(*hasher.finalize().as_bytes())
}

fn check_format(format: u8) -> Result<(), Error> {
    if format == FORMAT_VERSION {
        Ok(())
    } else {
        Err(Error::UnsupportedFormat(format))
    }
}

fn validate_logical(kind: LargeValueKind, bytes: &[u8]) -> Result<(), Error> {
    match kind {
        LargeValueKind::Bytes => Ok(()),
        LargeValueKind::String => std::str::from_utf8(bytes)
            .map(|_| ())
            .map_err(|_| Error::InvalidUtf8),
        LargeValueKind::Json => {
            let text = std::str::from_utf8(bytes).map_err(|_| Error::InvalidUtf8)?;
            serde_json::from_str::<serde_json::Value>(text)
                .map(|_| ())
                .map_err(|_| Error::InvalidJson)
        }
    }
}

fn metrics(kind: LargeValueKind, bytes: &[u8]) -> Result<NodeMetrics, Error> {
    let byte_length = u64::try_from(bytes.len()).map_err(|_| Error::MetricOverflow)?;
    let utf16_length = match kind {
        LargeValueKind::Bytes => None,
        LargeValueKind::String | LargeValueKind::Json => Some(
            u64::try_from(
                std::str::from_utf8(bytes)
                    .map_err(|_| Error::InvalidUtf8)?
                    .encode_utf16()
                    .count(),
            )
            .map_err(|_| Error::MetricOverflow)?,
        ),
    };
    Ok(NodeMetrics {
        byte_length,
        utf16_length,
    })
}

fn add_metrics(left: NodeMetrics, right: NodeMetrics) -> Result<NodeMetrics, Error> {
    let byte_length = left
        .byte_length
        .checked_add(right.byte_length)
        .ok_or(Error::MetricOverflow)?;
    let utf16_length = match (left.utf16_length, right.utf16_length) {
        (None, None) => None,
        (Some(left), Some(right)) => Some(left.checked_add(right).ok_or(Error::MetricOverflow)?),
        _ => return Err(Error::MalformedNode),
    };
    Ok(NodeMetrics {
        byte_length,
        utf16_length,
    })
}

#[allow(clippy::single_range_in_vec_init)] // The empty value still has one canonical empty leaf.
fn leaf_ranges(kind: LargeValueKind, bytes: &[u8]) -> Result<Vec<std::ops::Range<usize>>, Error> {
    if bytes.is_empty() {
        return Ok(vec![0..0]);
    }
    let text = match kind {
        LargeValueKind::Bytes => None,
        LargeValueKind::String | LargeValueKind::Json => {
            Some(std::str::from_utf8(bytes).map_err(|_| Error::InvalidUtf8)?)
        }
    };
    cdc_ranges(
        bytes,
        LEAF_MAX_BYTES,
        |hash, byte| roll(hash, *byte),
        leaf_cut,
        |start, mut end| {
            if let Some(text) = text {
                while end > start && !text.is_char_boundary(end) {
                    end -= 1;
                }
                if end == start {
                    return Err(Error::MalformedNode);
                }
            }
            Ok(end)
        },
    )
}

fn branch_ranges(
    kind: LargeValueKind,
    nodes: &[BuiltNode],
) -> Result<Vec<std::ops::Range<usize>>, Error> {
    let roll_node = |hash, node: &BuiltNode| roll_child(hash, kind, node);
    cdc_ranges(
        nodes,
        BRANCH_MAX_CHILDREN,
        roll_node,
        branch_cut,
        |_, end| Ok(end),
    )
}

/// Split `items` into content-defined groups of at most `max`: a group ends
/// at the first item after which `cut` accepts the rolling hash, and `settle`
/// may pull that end back.
fn cdc_ranges<T>(
    items: &[T],
    max: usize,
    roll: impl Fn(u64, &T) -> u64,
    cut: impl Fn(u64, usize) -> bool,
    settle: impl Fn(usize, usize) -> Result<usize, Error>,
) -> Result<Vec<std::ops::Range<usize>>, Error> {
    let mut ranges = Vec::new();
    let mut start = 0;
    while start < items.len() {
        let hard_end = (start + max).min(items.len());
        let mut hash = 0_u64;
        let end = items[start..hard_end]
            .iter()
            .enumerate()
            .find_map(|(offset, item)| {
                hash = roll(hash, item);
                cut(hash, offset + 1).then_some(start + offset + 1)
            })
            .unwrap_or(hard_end);
        let end = settle(start, end)?;
        ranges.push(start..end);
        start = end;
    }
    Ok(ranges)
}

/// One step of the V1 gear rolling hash.
fn roll(hash: u64, byte: u8) -> u64 {
    hash.wrapping_shl(1).wrapping_add(gear(byte))
}

/// Roll one child's kind-bound grouping hash into a branch boundary hash.
fn roll_child(hash: u64, kind: LargeValueKind, node: &BuiltNode) -> u64 {
    grouping_hash_from_logical(FORMAT_VERSION, kind, node.structural_hash)
        .0
        .into_iter()
        .fold(hash, roll)
}

/// Whether a leaf of `length` bytes may end where the rolling hash is `hash`.
fn leaf_cut(hash: u64, length: usize) -> bool {
    let mask = if length < LEAF_TARGET_BYTES {
        V1_FASTCDC_LEAF_SHORT_MASK
    } else {
        V1_FASTCDC_LEAF_LONG_MASK
    };
    length >= LEAF_MIN_BYTES && hash & mask == 0
}

/// Whether a branch of `count` children may end where the rolling hash is `hash`.
fn branch_cut(hash: u64, count: usize) -> bool {
    let mask = if count < BRANCH_TARGET_CHILDREN {
        V1_GROUPING_BRANCH_SHORT_MASK
    } else {
        V1_GROUPING_BRANCH_LONG_MASK
    };
    count >= BRANCH_MIN_CHILDREN && hash & mask == 0
}

fn gear(byte: u8) -> u64 {
    // V1's 256-entry gear table is generated by this frozen SplitMix64
    // parameterization. Keep the arithmetic expressed here (rather than a
    // platform-dependent RNG or tunable seed) so the content-defined profile
    // is independently reproducible from the specification.
    const V1_FASTCDC_GEAR_ADD: u64 = 0x9e37_79b9_7f4a_7c15;
    const V1_FASTCDC_GEAR_MUL_1: u64 = 0xbf58_476d_1ce4_e5b9;
    const V1_FASTCDC_GEAR_MUL_2: u64 = 0x94d0_49bb_1331_11eb;
    let mut value = u64::from(byte).wrapping_add(V1_FASTCDC_GEAR_ADD);
    value = (value ^ (value >> 30)).wrapping_mul(V1_FASTCDC_GEAR_MUL_1);
    value = (value ^ (value >> 27)).wrapping_mul(V1_FASTCDC_GEAR_MUL_2);
    value ^ (value >> 31)
}

#[cfg(test)]
fn stage_unvalidated_fixture_node(
    node: ChunkNode,
    staged_chunks: &mut Vec<StagedChunk>,
) -> BuiltNode {
    let kind = node_kind(&node);
    let metrics = node_metrics(kind, &node).unwrap();
    let structural_hash = node_logical_hash(&node);
    let encoded = encode_node(&node).unwrap();
    let object_hash = object_hash(&encoded);
    let node_ref = NodeRef { object_hash };
    staged_chunks.push(StagedChunk {
        node_ref: node_ref.clone(),
        encoded,
    });
    BuiltNode {
        node_ref,
        metrics,
        structural_hash,
    }
}

/// Authenticated JSON bytes without preparation-time JSON validation. Remote
/// admission tests must not depend on the validator they are trying to bypass.
#[cfg(test)]
pub fn unvalidated_json_leaf_fixture(bytes: &[u8]) -> PreparedLargeValue {
    assert!(bytes.len() <= LEAF_MAX_BYTES);
    let mut staged_chunks = Vec::new();
    let leaf = stage_unvalidated_fixture_node(
        ChunkNode::Leaf {
            format: FORMAT_VERSION,
            kind: LargeValueKind::Json,
            bytes: bytes.to_vec(),
        },
        &mut staged_chunks,
    );
    PreparedLargeValue {
        value_ref: LargeValueRef {
            kind: LargeValueKind::Json,
            format_version: FORMAT_VERSION,
            logical_hash: leaf.structural_hash,
            root: leaf.node_ref,
            byte_length: leaf.metrics.byte_length,
            utf16_length: leaf.metrics.utf16_length,
            edit_tail: Vec::new(),
        },
        staged_chunks,
    }
}

#[cfg(test)]
pub fn json_admission_corpus() -> Vec<(&'static str, String)> {
    let mut cases = vec![
        ("overflow", "1e400".to_owned()),
        ("negative overflow", "-1e400".to_owned()),
        ("finite maximum", "1.7976931348623157e308".to_owned()),
        (
            "maximum adjacent decimal lower",
            "1.7976931348623158e308".to_owned(),
        ),
        (
            "maximum adjacent decimal upper",
            "1.7976931348623159e308".to_owned(),
        ),
        (
            "negative maximum rounding",
            "-1.7976931348623158e308".to_owned(),
        ),
        (
            "integer fallback finite",
            "179769313486231570000e288".to_owned(),
        ),
        (
            "integer fallback overflow",
            "179769313486231590000e288".to_owned(),
        ),
        (
            "fraction truncation finite",
            "1.797693134862315700009999e308".to_owned(),
        ),
        (
            "fraction truncation overflow",
            "1.797693134862315900000001e308".to_owned(),
        ),
        ("u64 maximum", "18446744073709551615".to_owned()),
        ("above u64 maximum", "18446744073709551616".to_owned()),
        (
            "negative integer fallback",
            "-18446744073709551616".to_owned(),
        ),
        ("negative zero", "-0".to_owned()),
        ("signed fractional zero", "-0.000e+308".to_owned()),
        ("underflow", "1e-400".to_owned()),
        ("signed underflow", "-1e-400".to_owned()),
        ("subnormal", "4.9406564584124654e-324".to_owned()),
        ("positive exponent limit", "1e2147483647".to_owned()),
        ("positive exponent overflow", "1e2147483648".to_owned()),
        ("negative exponent limit", "1e-2147483648".to_owned()),
        ("zero exponent overflow", "0e2147483648".to_owned()),
        (
            "integer overflow then fitting fraction",
            "184467440737095516160.0e288".to_owned(),
        ),
        (
            "integer overflow then overflowing fraction",
            "184467440737095516150.9e288".to_owned(),
        ),
    ];
    let length = LEAF_MIN_BYTES + 17;
    cases.extend([
        (
            "long integer compensated",
            format!("1{}e-{length}", "0".repeat(length)),
        ),
        ("long integer overflow", format!("1{}", "0".repeat(length))),
        (
            "long compensated maximum",
            format!(
                "1797693134862315700{}e-{}",
                "0".repeat(length),
                length - 290
            ),
        ),
        (
            "long compensated overflow",
            format!(
                "1797693134862315900{}e-{}",
                "0".repeat(length),
                length - 290
            ),
        ),
        (
            "long leading fractional zeros",
            format!("0.{}1e{}", "0".repeat(length), length + 1),
        ),
        (
            "long leading fractional zeros overflow",
            format!("0.{}17976931348623159e{}", "0".repeat(length), length + 309),
        ),
        (
            "long fraction truncation",
            format!("1.7976931348623157{}e308", "9".repeat(length)),
        ),
        (
            "long exponent overflow",
            format!("1e{}", "9".repeat(length)),
        ),
        ("long exponent zero", format!("-0e{}", "9".repeat(length))),
        (
            "long exponent underflow",
            format!("1e-{}", "9".repeat(length)),
        ),
        (
            "long exponent leading zeros",
            format!("1e{}308", "0".repeat(length)),
        ),
    ]);
    for (name, escaped) in [
        ("lone high surrogate", r"\uD800"),
        ("lone low surrogate", r"\uDC00"),
        ("high high surrogate", r"\uD800\uDBFF"),
        ("ordinary successor", r"\uD800x"),
        ("non unicode successor", r"\uD800\n"),
        ("non surrogate unicode successor", r"\uD800\u0041"),
        ("truncated surrogate successor", r"\uD800\uDC0"),
        ("paired surrogate", r"\uD83D\uDE42"),
        ("paired surrogate extremes", r"\uDBFF\uDFFF"),
        ("ordinary unicode escape", r"\uD7FF\uE000"),
        ("raw non BMP", "🙂"),
    ] {
        cases.push((name, format!("\"{escaped}\"")));
        cases.push((name, format!("{{\"{escaped}\":0}}")));
        cases.push((name, format!("{{\"value\":\"{escaped}\"}}")));
    }
    // Place the escape pair and numeric overflow across completion-reader
    // windows as well as across public push boundaries.
    for (name, suffix) in [
        ("window split pair", r#"\uD83D\uDE42"}"#),
        ("window split unpaired surrogate", r#"\uD83D\u0041"}"#),
    ] {
        cases.push((
            name,
            format!("{{\"value\":\"{}{suffix}", "x".repeat(LEAF_MIN_BYTES - 12)),
        ));
    }
    cases.push((
        "window split exponent",
        format!("[{}1e400]", " ".repeat(LEAF_MIN_BYTES - 3)),
    ));
    cases
}

/// Build an explicitly malformed bottom-up DAG whose every branch repeats one
/// zero-metric child. This fixture deliberately bypasses canonical staging so
/// authenticated historical/wire nodes can exercise fail-closed admission.
#[cfg(test)]
pub fn zero_metric_repeated_child_dag_fixture(depth: usize, fanout: usize) -> PreparedLargeValue {
    assert!(depth <= MAX_TREE_DEPTH);
    assert!((1..=BRANCH_MAX_CHILDREN).contains(&fanout));
    let mut staged_chunks = Vec::new();
    let leaf = ChunkNode::Leaf {
        format: FORMAT_VERSION,
        kind: LargeValueKind::Bytes,
        bytes: Vec::new(),
    };
    let mut current = stage_node(
        leaf.clone(),
        node_metrics(LargeValueKind::Bytes, &leaf).unwrap(),
        &mut staged_chunks,
    )
    .unwrap();
    for _ in 0..depth {
        let node = ChunkNode::Branch {
            format: FORMAT_VERSION,
            kind: LargeValueKind::Bytes,
            children: vec![
                BranchChild {
                    node_ref: current.node_ref.clone(),
                    metrics: current.metrics,
                    logical_hash: current.structural_hash,
                };
                fanout
            ],
        };
        current = stage_unvalidated_fixture_node(node, &mut staged_chunks);
    }
    PreparedLargeValue {
        value_ref: LargeValueRef {
            kind: LargeValueKind::Bytes,
            format_version: FORMAT_VERSION,
            logical_hash: current.structural_hash,
            root: current.node_ref,
            byte_length: 0,
            utf16_length: None,
            edit_tail: Vec::new(),
        },
        staged_chunks,
    }
}

/// Build a shallow valid DAG whose every branch repeats one positive-byte
/// child. Callers must keep the chosen depth and fanout within `u64` metrics.
#[cfg(test)]
pub fn positive_repeated_child_dag_fixture(depth: usize, fanout: usize) -> PreparedLargeValue {
    assert!(depth <= MAX_TREE_DEPTH);
    assert!((1..=BRANCH_MAX_CHILDREN).contains(&fanout));
    let mut staged_chunks = Vec::new();
    let leaf = ChunkNode::Leaf {
        format: FORMAT_VERSION,
        kind: LargeValueKind::Bytes,
        bytes: vec![0x5a],
    };
    let mut current = stage_node(
        leaf.clone(),
        node_metrics(LargeValueKind::Bytes, &leaf).unwrap(),
        &mut staged_chunks,
    )
    .unwrap();
    for _ in 0..depth {
        let node = ChunkNode::Branch {
            format: FORMAT_VERSION,
            kind: LargeValueKind::Bytes,
            children: vec![
                BranchChild {
                    node_ref: current.node_ref.clone(),
                    metrics: current.metrics,
                    logical_hash: current.structural_hash,
                };
                fanout
            ],
        };
        current = stage_node(
            node.clone(),
            node_metrics(LargeValueKind::Bytes, &node).unwrap(),
            &mut staged_chunks,
        )
        .unwrap();
    }
    PreparedLargeValue {
        value_ref: LargeValueRef {
            kind: LargeValueKind::Bytes,
            format_version: FORMAT_VERSION,
            logical_hash: current.structural_hash,
            root: current.node_ref,
            byte_length: current.metrics.byte_length,
            utf16_length: None,
            edit_tail: Vec::new(),
        },
        staged_chunks,
    }
}

/// Build a diamond in which one branch is reached twice and both reach one
/// leaf, exercising ownership of a shared node.
#[cfg(test)]
pub fn shared_child_dag_fixture() -> PreparedLargeValue {
    let mut staged_chunks = Vec::new();
    let mut stage = |node: ChunkNode| {
        stage_node(
            node.clone(),
            node_metrics(LargeValueKind::Bytes, &node).unwrap(),
            &mut staged_chunks,
        )
        .unwrap()
    };
    let leaf = stage(ChunkNode::Leaf {
        format: FORMAT_VERSION,
        kind: LargeValueKind::Bytes,
        bytes: vec![0x7d],
    });
    let branch_node = ChunkNode::Branch {
        format: FORMAT_VERSION,
        kind: LargeValueKind::Bytes,
        children: vec![BranchChild {
            node_ref: leaf.node_ref.clone(),
            metrics: leaf.metrics,
            logical_hash: leaf.structural_hash,
        }],
    };
    let left = stage(branch_node.clone());
    let right = stage(branch_node);
    let root_node = ChunkNode::Branch {
        format: FORMAT_VERSION,
        kind: LargeValueKind::Bytes,
        children: vec![
            BranchChild {
                node_ref: left.node_ref,
                metrics: left.metrics,
                logical_hash: left.structural_hash,
            },
            BranchChild {
                node_ref: right.node_ref,
                metrics: right.metrics,
                logical_hash: right.structural_hash,
            },
        ],
    };
    let root = stage(root_node);
    PreparedLargeValue {
        value_ref: LargeValueRef {
            kind: LargeValueKind::Bytes,
            format_version: FORMAT_VERSION,
            logical_hash: root.structural_hash,
            root: root.node_ref,
            byte_length: root.metrics.byte_length,
            utf16_length: None,
            edit_tail: Vec::new(),
        },
        staged_chunks,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_ref_codec_reuses_the_stored_scalar_root_record() {
        let node_ref = NodeRef {
            object_hash: ContentHash([7; 32]),
        };
        let encoded = encode_node_ref(&node_ref).unwrap();

        assert_eq!(decode_node_ref(&encoded).unwrap(), node_ref);

        let value_ref = LargeValueRef {
            kind: LargeValueKind::Bytes,
            format_version: FORMAT_VERSION,
            logical_hash: ContentHash([11; 32]),
            root: node_ref,
            byte_length: 0,
            utf16_length: None,
            edit_tail: Vec::new(),
        };
        let Value::Record(stored_root) = &chunked_values(&value_ref)[2] else {
            panic!("stored scalar root must be a record");
        };
        assert_eq!(stored_root.raw(), encoded);

        let malformed = node_ref_descriptor()
            .create(&[Value::Bytes(vec![0; 31])])
            .unwrap();
        assert_eq!(decode_node_ref(&malformed), Err(Error::MalformedScalar));
    }

    // This is intentionally an internal physical-codec receipt. Public rows
    // only see logical primitives; the exact bytes here freeze the engine's
    // descriptor-led v1 boundary and make a future codec addition explicit.
    #[test]
    fn v1_codec_golden_bytes_decode_semantically_and_reject_alternates() {
        fn hex(bytes: &[u8]) -> String {
            bytes.iter().map(|byte| format!("{byte:02x}")).collect()
        }
        let leaf = ChunkNode::Leaf {
            format: FORMAT_VERSION,
            kind: LargeValueKind::Bytes,
            bytes: b"v1-fixture".to_vec(),
        };
        let node_bytes = encode_node(&leaf).unwrap();
        let node_hash = object_hash(&node_bytes);
        assert_eq!(
            hex(&node_bytes),
            "00010076312d66697874757265",
            "v1 node bytes are a reviewed storage fixture"
        );
        assert_eq!(
            hex(&node_hash.0),
            "84e5cf641223d7cd2110f4d1b891d150af05976427192759fd8aba94df9b638e",
            "object hashes commit to the exact v1 node bytes"
        );
        assert_eq!(
            hex(&node_logical_hash(&leaf).0),
            "357c7ae2a6895ca8c8f21120af72cc37cdc43d2929f506cb17a543c03e90da65",
            "logical hashes bind the v1 format and semantic kind"
        );
        assert_eq!(
            decode_node_for_format(
                FORMAT_VERSION,
                LargeValueKind::Bytes,
                node_hash,
                &node_bytes
            )
            .unwrap(),
            leaf
        );

        let value_ref = LargeValueRef {
            kind: LargeValueKind::Bytes,
            format_version: FORMAT_VERSION,
            logical_hash: node_logical_hash(&leaf),
            root: NodeRef {
                object_hash: node_hash,
            },
            byte_length: 10,
            utf16_length: None,
            edit_tail: vec![ReplaceEdit {
                offset: 9,
                delete_length: 1,
                insert_bytes: b"e".to_vec(),
                utf16_offset: 0,
                delete_utf16_length: 0,
                insert_utf16_length: 0,
            }],
        };
        let descriptor_bytes = encode_large_value_ref(&value_ref).unwrap();
        assert_eq!(
            hex(&descriptor_bytes),
            "00010a000000000000003100000051000000357c7ae2a6895ca8c8f21120af72cc37cdc43d2929f506cb17a543c03e90da6584e5cf641223d7cd2110f4d1b891d150af05976427192759fd8aba94df9b638e010000000900000000000000010000000000000065",
            "v1 descriptor bytes are a reviewed storage fixture"
        );
        let decoded_ref = decode_large_value_ref(&descriptor_bytes).unwrap();
        assert_eq!(decoded_ref, value_ref);
        let mut inputs = ChunkInputs::default();
        inputs.install_chunk(
            ChunkRequest {
                object_hash: node_hash.0,
            },
            bytes::Bytes::from(node_bytes.clone()),
        );
        assert_eq!(
            materialize_attempt(&decoded_ref, &mut inputs).unwrap(),
            b"v1-fixture",
            "decoded fixture reaches the same logical scalar receipt"
        );

        let scalar = StoredScalar::Chunked(value_ref);
        let scalar_bytes = encode_stored_scalar(LargeValueKind::Bytes, &scalar).unwrap();
        assert_eq!(
            hex(&scalar_bytes),
            "01010a000000000000003100000051000000357c7ae2a6895ca8c8f21120af72cc37cdc43d2929f506cb17a543c03e90da6584e5cf641223d7cd2110f4d1b891d150af05976427192759fd8aba94df9b638e010000000900000000000000010000000000000065",
            "schema-known stored-scalar kind does not add a descriptor kind tag"
        );
        assert_eq!(
            decode_stored_scalar(LargeValueKind::Bytes, &scalar_bytes).unwrap(),
            scalar
        );

        for bytes in [&node_bytes[..], &descriptor_bytes[..], &scalar_bytes[..]] {
            let mut alternate = bytes.to_vec();
            alternate.push(0);
            assert_ne!(alternate, bytes, "fixture mutation must be sensitive");
        }
        let branch_child = BranchChild {
            node_ref: NodeRef {
                object_hash: ContentHash([0x55; 32]),
            },
            metrics: NodeMetrics {
                byte_length: 1,
                utf16_length: None,
            },
            logical_hash: ContentHash([0x77; 32]),
        };
        let mut trailing_node = encode_node(&ChunkNode::Branch {
            format: FORMAT_VERSION,
            kind: LargeValueKind::Bytes,
            children: vec![branch_child; BRANCH_MIN_CHILDREN],
        })
        .unwrap();
        trailing_node.push(0);
        assert_eq!(
            decode_node_for_format(
                FORMAT_VERSION,
                LargeValueKind::Bytes,
                object_hash(&trailing_node),
                &trailing_node,
            ),
            Err(Error::MalformedNode)
        );
        let mut trailing_descriptor = descriptor_bytes.clone();
        trailing_descriptor.push(0);
        assert_eq!(
            decode_large_value_ref(&trailing_descriptor),
            Err(Error::MalformedScalar)
        );
        let mut trailing_scalar = scalar_bytes.clone();
        trailing_scalar.push(0);
        assert_eq!(
            decode_stored_scalar(LargeValueKind::Bytes, &trailing_scalar),
            Err(Error::MalformedScalar)
        );

        assert_eq!(
            decode_node_for_format(2, LargeValueKind::Bytes, node_hash, &node_bytes),
            Err(Error::UnsupportedFormat(2)),
            "a descriptor never falls back to the current codec"
        );

        let text = ChunkNode::Leaf {
            format: FORMAT_VERSION,
            kind: LargeValueKind::String,
            bytes: "v1-🙂".as_bytes().to_vec(),
        };
        let json = ChunkNode::Leaf {
            format: FORMAT_VERSION,
            kind: LargeValueKind::Json,
            bytes: br#"{"n":-0}"#.to_vec(),
        };
        for (node, expected_bytes, expected_object_hash, expected_logical_hash, expected_metrics) in [
            (
                text,
                "00010176312df09f9982",
                "8ca733330fa66126ffe0096382780f11b272047aea7be36fc5c23b6f16d8680a",
                "9e87a0d9054fae5cbe72f01ea5ee76cf97c0073ae4955797470365ab08952661",
                NodeMetrics {
                    byte_length: 7,
                    utf16_length: Some(5),
                },
            ),
            (
                json,
                "0001027b226e223a2d307d",
                "a9865ebcd28c3e1868f670948dd798c1ff5287c025efe5d26b344c261e628dcc",
                "4be873c1509bff9e7e4a861e5e405c29088d67a49cd685ec82be6f4fb4532a28",
                NodeMetrics {
                    byte_length: 8,
                    utf16_length: Some(8),
                },
            ),
        ] {
            let bytes = encode_node(&node).unwrap();
            assert_eq!(hex(&bytes), expected_bytes);
            assert_eq!(hex(&object_hash(&bytes).0), expected_object_hash);
            assert_eq!(hex(&node_logical_hash(&node).0), expected_logical_hash);
            assert_eq!(
                node_metrics(node_kind(&node), &node).unwrap(),
                expected_metrics
            );
            assert_eq!(
                decode_node_for_format(
                    FORMAT_VERSION,
                    node_kind(&node),
                    object_hash(&bytes),
                    &bytes,
                )
                .unwrap(),
                node
            );
        }
    }

    #[test]
    fn v1_json_literal_duplicate_numeric_unicode_and_tail_fixtures_are_canonical() {
        fn hex(bytes: &[u8]) -> String {
            bytes.iter().map(|byte| format!("{byte:02x}")).collect()
        }
        // Deliberately literal source: duplicate keys, numeric spelling and
        // escape spelling are physical JSON content, while parsed reads keep
        // serde_json's ordinary last-duplicate-key semantics.
        let source = b"{\"literal\":\"\\uD83D\\uDE42\",\"dup\":1,\"dup\":2,\"n\":-0,\"scientific\":1e+00,\"text\":\"\\u00e9\"}";
        let node = ChunkNode::Leaf {
            format: FORMAT_VERSION,
            kind: LargeValueKind::Json,
            bytes: source.to_vec(),
        };
        let encoded_node = encode_node(&node).unwrap();
        assert_eq!(
            hex(&encoded_node),
            "0001027b226c69746572616c223a225c75443833445c7544453432222c22647570223a312c22647570223a322c226e223a2d302c22736369656e7469666963223a31652b30302c2274657874223a225c7530306539227d"
        );
        assert_eq!(
            hex(&object_hash(&encoded_node).0),
            "14dff6ca13ae13d1dd320af8f190088393f1ffaf270a0a66172f3b4a18beaf37"
        );
        assert_eq!(
            hex(&node_logical_hash(&node).0),
            "4a267f38a9486460670c54d2ca475f4058aee0dc528742ddd18ed8f3dae8bab8"
        );
        assert_eq!(
            decode_node_for_format(
                FORMAT_VERSION,
                LargeValueKind::Json,
                object_hash(&encoded_node),
                &encoded_node,
            ),
            Ok(node.clone())
        );
        let parsed: serde_json::Value = serde_json::from_slice(source).unwrap();
        assert_eq!(parsed.pointer("/literal"), Some(&serde_json::json!("🙂")));
        assert_eq!(parsed.pointer("/dup"), Some(&serde_json::json!(2)));
        assert_eq!(parsed.pointer("/n"), Some(&serde_json::json!(-0.0)));
        assert_eq!(parsed.pointer("/scientific"), Some(&serde_json::json!(1.0)));

        let primitive = encode_stored_scalar(
            LargeValueKind::Json,
            &StoredScalar::Primitive(source.to_vec()),
        )
        .unwrap();
        assert_eq!(hex(&primitive), format!("00{}", hex(source)));
        assert_eq!(
            decode_stored_scalar(LargeValueKind::Json, &primitive),
            Ok(StoredScalar::Primitive(source.to_vec()))
        );
        let prepared = prepare(LargeValueKind::Json, source).unwrap();
        let mut inputs = ChunkInputs::default();
        for chunk in &prepared.staged_chunks {
            inputs.install_chunk(
                ChunkRequest {
                    object_hash: chunk.node_ref.object_hash.0,
                },
                bytes::Bytes::copy_from_slice(&chunk.encoded),
            );
        }
        let replacement =
            "{\"literal\":\"🙂\",\"dup\":3,\"n\":0,\"scientific\":1.0,\"text\":\"é!\"}".as_bytes();
        let TailEditOutcome::Updated(with_tail) = replace_tail_attempt(
            &prepared.value_ref,
            0,
            prepared.value_ref.byte_length,
            replacement.to_vec(),
            &mut inputs,
        )
        .unwrap() else {
            panic!("whole JSON replacement fits one tail entry")
        };
        assert_eq!(
            with_tail.utf16_length,
            Some(
                std::str::from_utf8(replacement)
                    .unwrap()
                    .encode_utf16()
                    .count() as u64
            )
        );
        assert_eq!(
            hex(&encode_large_value_ref(&with_tail).unwrap()),
            "02013e000000000000003b0000000000000039000000590000004a267f38a9486460670c54d2ca475f4058aee0dc528742ddd18ed8f3dae8bab814dff6ca13ae13d1dd320af8f190088393f1ffaf270a0a66172f3b4a18beaf370100000000000000000000005400000000000000000000000000000054000000000000007b226c69746572616c223a22f09f9982222c22647570223a332c226e223a302c22736369656e7469666963223a312e302c2274657874223a22c3a921227d"
        );
        let scalar = encode_stored_scalar(
            LargeValueKind::Json,
            &StoredScalar::Chunked(with_tail.clone()),
        )
        .unwrap();
        assert_eq!(
            hex(&scalar),
            "01013e000000000000003b0000000000000039000000590000004a267f38a9486460670c54d2ca475f4058aee0dc528742ddd18ed8f3dae8bab814dff6ca13ae13d1dd320af8f190088393f1ffaf270a0a66172f3b4a18beaf370100000000000000000000005400000000000000000000000000000054000000000000007b226c69746572616c223a22f09f9982222c22647570223a332c226e223a302c22736369656e7469666963223a312e302c2274657874223a22c3a921227d"
        );
        assert_eq!(
            decode_stored_scalar(LargeValueKind::Json, &scalar),
            Ok(StoredScalar::Chunked(with_tail.clone()))
        );

        let mut future_descriptor = encode_large_value_ref(&with_tail).unwrap();
        future_descriptor[1] = FORMAT_VERSION + 1;
        assert_eq!(
            decode_large_value_ref(&future_descriptor),
            Err(Error::UnsupportedFormat(FORMAT_VERSION + 1)),
            "descriptor version is dispatched before its root and edit tail"
        );
        let mut future_scalar = scalar.clone();
        future_scalar[1] = FORMAT_VERSION + 1;
        assert_eq!(
            decode_stored_scalar(LargeValueKind::Json, &future_scalar),
            Err(Error::UnsupportedFormat(FORMAT_VERSION + 1)),
            "stored scalar must use the descriptor's same joint dispatch"
        );
        // These deliberately omit every V1 root/edit field. The unknown
        // selector must still win: no current nested ValueType may inspect a
        // future payload before the dispatch boundary rejects it.
        assert_eq!(
            decode_large_value_ref(&[LARGE_VALUE_REF_JSON_TAG, FORMAT_VERSION + 1, 0xff]),
            Err(Error::UnsupportedFormat(FORMAT_VERSION + 1))
        );
        assert_eq!(
            decode_stored_scalar(LargeValueKind::Json, &[1, FORMAT_VERSION + 1, 0xff]),
            Err(Error::UnsupportedFormat(FORMAT_VERSION + 1))
        );

        let mut alternate = scalar.clone();
        alternate.push(0);
        assert_eq!(
            decode_stored_scalar(LargeValueKind::Json, &alternate),
            Err(Error::MalformedScalar),
            "planted trailing-byte mutation proves the fixture is exact"
        );
        let mut forged_utf16 = with_tail.clone();
        forged_utf16.edit_tail[0].insert_utf16_length -= 1;
        assert_eq!(
            encode_large_value_ref(&forged_utf16),
            Err(Error::MalformedScalar),
            "the UTF-16 witness must match literal UTF-8 insert bytes"
        );
        assert!(matches!(
            replace_tail_attempt(
                &prepared.value_ref,
                1,
                prepared.value_ref.byte_length - 1,
                replacement.to_vec(),
                &mut ChunkInputs::default(),
            ),
            Err(AttemptError::LargeValue(Error::InvalidJson))
        ));
        assert_eq!(
            prepare(LargeValueKind::Json, b"{\"broken\":"),
            Err(Error::InvalidJson)
        );
        assert_eq!(
            prepare(LargeValueKind::Json, b"{\"text\":\"\xff\"}"),
            Err(Error::InvalidUtf8)
        );

        let consolidated = consolidate_single_edit_attempt(&with_tail, &mut inputs).unwrap();
        for chunk in &consolidated.staged_chunks {
            inputs.install_chunk(
                ChunkRequest {
                    object_hash: chunk.node_ref.object_hash.0,
                },
                bytes::Bytes::copy_from_slice(&chunk.encoded),
            );
        }
        assert!(consolidated.value_ref.edit_tail.is_empty());
        assert_eq!(
            materialize_attempt(&consolidated.value_ref, &mut inputs).unwrap(),
            replacement,
            "tail consolidation preserves literal JSON replacement bytes"
        );
    }

    #[test]
    fn v1_content_defined_profile_manifest_is_permanent() {
        fn hex(bytes: &[u8]) -> String {
            bytes.iter().map(|byte| format!("{byte:02x}")).collect()
        }
        let mut state = 0x4d59_5df4_d0f3_3173_u64;
        let bytes = (0..(LEAF_MAX_BYTES * 12 + LEAF_TARGET_BYTES))
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect::<Vec<_>>();
        assert_eq!(
            (gear(0), gear(1), gear(0x7f), gear(u8::MAX)),
            (
                0xe220_a839_7b1d_cdaf,
                0x910a_2dec_8902_5cc1,
                0x3fad_b6bd_e928_5e98,
                0x338c_5071_4628_3fb4,
            ),
            "V1 gear vectors are storage-format data"
        );
        assert_eq!(
            leaf_ranges(LargeValueKind::Bytes, &bytes).unwrap(),
            [
                0..145537,
                145537..180487,
                180487..333056,
                333056..374799,
                374799..417781,
                417781..487110,
                487110..543713,
                543713..668001,
                668001..741970,
                741970..827720,
                827720..912126,
                912126..935212,
                935212..1047063,
                1047063..1130487,
                1130487..1271830,
                1271830..1340443,
                1340443..1388172,
                1388172..1463168,
                1463168..1536176,
                1536176..1622009,
                1622009..1833371,
                1833371..1906888,
                1906888..1988979,
                1988979..2008288,
                2008288..2092742,
                2092742..2253159,
                2253159..2379622,
                2379622..2464077,
                2464077..2543777,
                2543777..2582151,
                2582151..2628177,
                2628177..2699778,
                2699778..2769680,
                2769680..2805165,
                2805165..2849502,
                2849502..2868543,
                2868543..3007861,
                3007861..3058280,
                3058280..3131945,
                3131945..3211264,
            ],
            "both short and long masks must choose the same leaf boundaries"
        );
        assert_eq!(
            leaf_ranges(LargeValueKind::Bytes, &vec![0; LEAF_MAX_BYTES * 2 + 1]).unwrap(),
            [
                0..LEAF_MAX_BYTES,
                LEAF_MAX_BYTES..LEAF_MAX_BYTES * 2,
                LEAF_MAX_BYTES * 2..LEAF_MAX_BYTES * 2 + 1
            ],
            "V1's hard maximum cuts even when the rolling mask never does"
        );
        let mut text = Vec::new();
        while text.len() < LEAF_MAX_BYTES * 2 + 100 {
            text.extend_from_slice("a🙂".as_bytes());
        }
        assert_eq!(
            leaf_ranges(LargeValueKind::String, &text).unwrap(),
            [0..262141, 262141..524285, 524285..524390],
            "the hard boundary is repaired backwards to the preceding UTF-8 code-point boundary"
        );

        let prepared = prepare(LargeValueKind::Bytes, &bytes).unwrap();
        let branches = prepared
            .staged_chunks
            .iter()
            .filter_map(|chunk| {
                let node = decode_node(
                    LargeValueKind::Bytes,
                    chunk.node_ref.object_hash,
                    &chunk.encoded,
                )
                .unwrap();
                let logical = node_logical_hash(&node);
                match node {
                    ChunkNode::Leaf { .. } => None,
                    ChunkNode::Branch { children, .. } => Some((
                        children
                            .iter()
                            .map(|child| child.metrics.byte_length)
                            .collect::<Vec<_>>(),
                        hex(&chunk.node_ref.object_hash.0),
                        hex(&logical.0),
                    )),
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(
            branches,
            vec![
                (
                    vec![
                        145537, 34950, 152569, 41743, 42982, 69329, 56603, 124288, 73969, 85750,
                        84406, 23086, 111851, 83424
                    ],
                    "b4d852672e851cd0b793c3c371c23be93635bfec4d74045b7529f0c91eb210f2".to_owned(),
                    "c8b977ca28bab218dd195785eec03dc78706b126edae83a480772b517381a575".to_owned()
                ),
                (
                    vec![
                        141343, 68613, 47729, 74996, 73008, 85833, 211362, 73517, 82091, 19309,
                        84454, 160417, 126463, 84455, 79700, 38374, 46026, 71601, 69902, 35485,
                        44337, 19041, 139318, 50419
                    ],
                    "47cc24bd324a5d621f3c1d10746324b256a2569a39897c5d4fc3dbdd72886a9e".to_owned(),
                    "d274d2f7b43d8e4a774c8b52929567e6fd059a5fb035a76ba7d694c561536d1a".to_owned()
                ),
                (
                    vec![73665, 79319],
                    "93b0c07ec7c6c4dc8d870d7318f65935c7ae6da0f8a135c0c91321e40a00c0e9".to_owned(),
                    "64119616bbd532be38b6ae4e33fb1b9260cd022f322cc9225b3efe4607e27365".to_owned()
                ),
                (
                    vec![1130487, 1927793, 152984],
                    "e4f9965a295739a36a1ff9bf93a808135dbddc3a9ebda35aeb96d4ed85f79141".to_owned(),
                    "2d9f7ec079e2ac62ff79eafb27784ad9dbe6651e91df5795fe3fe54807247e2e".to_owned()
                ),
            ],
            "multi-branch grouping and every branch object hash are frozen"
        );
    }

    /// Alice's engine-owned record layout uses contiguous numeric IDs.
    /// This internal byte receipt pins ordering that public row queries cannot expose.
    #[test]
    fn durable_large_value_field_ids_are_physical_slots() {
        let schema = durable_large_value_record_descriptor([
            (1, "object_hash", ValueType::U64),
            (2, "byte_length", ValueType::U64),
        ]);
        let reordered = durable_large_value_record_descriptor([
            (2, "byte_length", ValueType::U64),
            (1, "object_hash", ValueType::U64),
        ]);
        let values = schema
            .ordered_values([(2, Value::U64(11)), (1, Value::U64(7))])
            .unwrap();
        let bytes = schema.descriptor.create(&values).unwrap();
        assert_eq!(bytes, [7, 0, 0, 0, 0, 0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(bytes, reordered.descriptor.create(&values).unwrap());
        assert_eq!(
            schema.decode_values(&values).unwrap(),
            BTreeMap::from([(1, Value::U64(7)), (2, Value::U64(11))])
        );
        for ids in [vec![], vec![0], vec![2], vec![1, 1], vec![1, 3]] {
            assert!(
                std::panic::catch_unwind(|| {
                    durable_large_value_record_descriptor(
                        ids.iter().map(|id| (*id, "field", ValueType::U64)),
                    );
                })
                .is_err(),
                "invalid field IDs must be rejected: {ids:?}"
            );
        }
    }

    struct PreparedProvider {
        chunks: std::collections::BTreeMap<ContentHash, bytes::Bytes>,
    }

    impl PreparedProvider {
        fn new(prepared: &PreparedLargeValue) -> Self {
            Self {
                chunks: prepared
                    .staged_chunks
                    .iter()
                    .map(|chunk| {
                        (
                            chunk.node_ref.object_hash,
                            bytes::Bytes::copy_from_slice(&chunk.encoded),
                        )
                    })
                    .collect(),
            }
        }
    }

    impl crate::large_values::chunks::ChunkProvider for PreparedProvider {
        fn get(
            &self,
            request: ChunkRequest,
        ) -> crate::large_values::chunks::ChunkFuture<
            '_,
            Result<bytes::Bytes, crate::large_values::chunks::ChunkError>,
        > {
            Box::pin(async move {
                self.chunks
                    .get(&ContentHash(request.object_hash))
                    .cloned()
                    .ok_or(crate::large_values::chunks::ChunkError::Unavailable)
            })
        }
    }

    #[test]
    fn physical_graph_completion_accepts_sharing_but_rejects_cycles_and_long_paths() {
        let node_ref = |id: u8| NodeRef {
            object_hash: ContentHash([id; 32]),
        };
        let [root, left, right, shared] = [node_ref(1), node_ref(2), node_ref(3), node_ref(4)];
        let mut diamond = PhysicalTraversal::new(root.clone(), None, ContentHash([9; 32]));
        diamond
            .edges
            .insert(root.clone(), vec![left.clone(), right.clone()]);
        diamond.edges.insert(left.clone(), vec![shared.clone()]);
        diamond.edges.insert(right, vec![shared.clone()]);
        diamond.edges.insert(shared, Vec::new());
        assert_eq!(diamond.finish(), Ok(()));

        let mut cycle = PhysicalTraversal::new(root.clone(), None, ContentHash([9; 32]));
        cycle.edges.insert(root.clone(), vec![left.clone()]);
        cycle.edges.insert(left, vec![root]);
        assert_eq!(cycle.finish(), Err(Error::InvalidTree));

        let path = (0..=MAX_TREE_DEPTH + 1)
            .map(|id| node_ref(id as u8 + 10))
            .collect::<Vec<_>>();
        let mut too_deep = PhysicalTraversal::new(path[0].clone(), None, ContentHash([9; 32]));
        for edge in path.windows(2) {
            too_deep
                .edges
                .insert(edge[0].clone(), vec![edge[1].clone()]);
        }
        too_deep
            .edges
            .insert(path.last().unwrap().clone(), Vec::new());
        assert_eq!(too_deep.finish(), Err(Error::InvalidTree));
    }

    #[test]
    fn zero_byte_branch_child_is_rejected_before_descendant_discovery_or_materialization() {
        let prepared = zero_metric_repeated_child_dag_fixture(1, BRANCH_MIN_CHILDREN);
        let root = prepared
            .staged_chunks
            .iter()
            .find(|chunk| chunk.node_ref == prepared.value_ref.root)
            .unwrap();

        assert_eq!(
            decode_node(
                LargeValueKind::Bytes,
                root.node_ref.object_hash,
                &root.encoded,
            ),
            Err(Error::MalformedNode)
        );
        assert_eq!(
            decode_authenticated_node(root.node_ref.object_hash, &root.encoded),
            Err(Error::MalformedNode)
        );

        let provider = PreparedProvider::new(&prepared);
        let mut visited = std::collections::BTreeSet::new();
        assert_eq!(
            futures::executor::block_on(visit_reachable_chunks(
                &prepared.value_ref,
                &provider,
                |request| {
                    visited.insert(request.clone());
                },
            )),
            Err(ReachabilityError::LargeValue(Error::MalformedNode))
        );
        let root_request = ChunkRequest {
            object_hash: root.node_ref.object_hash.0,
        };
        assert!(
            visited.is_empty(),
            "an authenticated malformed root must not expose its child frontier"
        );

        let mut inputs = ChunkInputs::default();
        inputs.install_chunk(root_request, bytes::Bytes::copy_from_slice(&root.encoded));
        assert!(matches!(
            materialize_attempt(&prepared.value_ref, &mut inputs),
            Err(AttemptError::LargeValue(Error::MalformedNode))
        ));
        assert!(
            inputs.take_missing_chunks().is_empty(),
            "materialization must reject the root before requesting descendants"
        );
    }

    #[test]
    fn canonical_empty_value_is_one_empty_root_leaf() {
        let prepared = prepare(LargeValueKind::Bytes, b"").unwrap();
        assert_eq!(prepared.staged_chunks.len(), 1);
        let root = &prepared.staged_chunks[0];
        assert_eq!(root.node_ref, prepared.value_ref.root);
        assert_eq!(prepared.value_ref.byte_length, 0);
        assert_eq!(
            decode_node(
                LargeValueKind::Bytes,
                root.node_ref.object_hash,
                &root.encoded,
            ),
            Ok(ChunkNode::Leaf {
                format: FORMAT_VERSION,
                kind: LargeValueKind::Bytes,
                bytes: Vec::new(),
            })
        );

        let mut inputs = ChunkInputs::default();
        inputs.install_chunk(
            ChunkRequest {
                object_hash: root.node_ref.object_hash.0,
            },
            bytes::Bytes::copy_from_slice(&root.encoded),
        );
        let materialized = materialize_attempt(&prepared.value_ref, &mut inputs).unwrap();
        assert_eq!(materialized, Vec::<u8>::new());
        assert!(inputs.take_missing_chunks().is_empty());
    }

    #[test]
    fn shared_dag_physical_walks_deduplicate_and_logical_materialization_preserves_occurrences() {
        let depth = 2;
        let fanout = 4;
        let prepared = positive_repeated_child_dag_fixture(depth, fanout);
        assert_eq!(prepared.staged_chunks.len(), depth + 1);

        let provider = PreparedProvider::new(&prepared);
        let mut visited = std::collections::BTreeSet::new();
        let count = futures::executor::block_on(visit_reachable_chunks(
            &prepared.value_ref,
            &provider,
            |request| {
                visited.insert(request.clone());
            },
        ))
        .unwrap();
        assert_eq!(count as usize, prepared.staged_chunks.len());
        assert_eq!(visited.len(), prepared.staged_chunks.len());

        let mut inputs = ChunkInputs::default();
        for chunk in &prepared.staged_chunks {
            inputs.install_chunk(
                ChunkRequest {
                    object_hash: chunk.node_ref.object_hash.0,
                },
                bytes::Bytes::copy_from_slice(&chunk.encoded),
            );
        }
        assert_eq!(
            materialize_attempt(&prepared.value_ref, &mut inputs).unwrap(),
            vec![0x5a; fanout.pow(depth as u32)]
        );
    }

    fn traverse_fixture_with_node_limit(
        prepared: &PreparedLargeValue,
        max_nodes: usize,
    ) -> Result<usize, Error> {
        let root_metrics = Some(NodeMetrics {
            byte_length: prepared.value_ref.byte_length,
            utf16_length: prepared.value_ref.utf16_length,
        });
        let chunks = prepared
            .staged_chunks
            .iter()
            .map(|chunk| (chunk.node_ref.clone(), chunk))
            .collect::<BTreeMap<_, _>>();
        let mut traversal = PhysicalTraversal::new_with_node_limit(
            prepared.value_ref.root.clone(),
            root_metrics,
            prepared.value_ref.logical_hash,
            max_nodes,
        )?;
        let mut visited = 0;
        while let Some(entry) = traversal.pop() {
            let chunk = chunks
                .get(&entry.node_ref)
                .expect("fixture has every reachable physical node");
            let node = decode_node(
                prepared.value_ref.kind,
                entry.node_ref.object_hash,
                &chunk.encoded,
            )?;
            traversal.validate_node(prepared.value_ref.kind, &entry.node_ref, &node)?;
            if let ChunkNode::Branch { children, .. } = node {
                traversal.discover_children(&entry, children)?;
            }
            visited += 1;
        }
        Ok(visited)
    }

    #[test]
    fn preparation_depends_only_on_content() {
        let first = prepare(LargeValueKind::Bytes, b"same logical bytes").unwrap();
        let second = prepare(LargeValueKind::Bytes, b"same logical bytes").unwrap();
        assert_eq!(first, second);
        let other = prepare(LargeValueKind::String, b"same logical bytes").unwrap();
        assert_ne!(
            first.value_ref.root, other.value_ref.root,
            "kinds hash apart"
        );
    }

    #[test]
    fn reachability_traversal_authenticates_and_visits_the_complete_tree() {
        let mut state = 0x4d59_5df4_d0f3_3173_u64;
        let logical = (0..LEAF_TARGET_BYTES * (BRANCH_MAX_CHILDREN + 5))
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect::<Vec<_>>();
        let prepared = prepare(LargeValueKind::Bytes, &logical).unwrap();
        let provider = PreparedProvider::new(&prepared);
        let mut visited = std::collections::BTreeSet::new();
        let count = futures::executor::block_on(visit_reachable_chunks(
            &prepared.value_ref,
            &provider,
            |request| {
                visited.insert(request.clone());
            },
        ))
        .unwrap();
        assert_eq!(count as usize, prepared.staged_chunks.len());
        assert_eq!(visited.len(), prepared.staged_chunks.len());
    }

    #[test]
    fn physical_traversal_budget_allows_the_exact_distinct_node_boundary_for_shared_dag() {
        let prepared = positive_repeated_child_dag_fixture(2, 4);
        assert_eq!(prepared.staged_chunks.len(), 3);
        assert_eq!(
            traverse_fixture_with_node_limit(&prepared, prepared.staged_chunks.len()),
            Ok(prepared.staged_chunks.len()),
            "repeated logical child edges consume one physical-node slot"
        );
    }

    #[test]
    fn physical_traversal_budget_rejects_one_distinct_node_over_the_boundary() {
        let prepared = positive_repeated_child_dag_fixture(3, 4);
        assert_eq!(prepared.staged_chunks.len(), 4);
        assert_eq!(
            traverse_fixture_with_node_limit(&prepared, prepared.staged_chunks.len() - 1),
            Err(Error::PhysicalTraversalNodeLimitExceeded)
        );
    }

    struct WindowReader<'a> {
        bytes: &'a [u8],
        offset: usize,
        state: u64,
    }

    impl std::io::Read for WindowReader<'_> {
        fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
            if self.offset == self.bytes.len() {
                return Ok(0);
            }
            self.state ^= self.state << 13;
            self.state ^= self.state >> 7;
            self.state ^= self.state << 17;
            let window = 1 + self.state as usize % (LEAF_MIN_BYTES * 2);
            let count = window.min(output.len()).min(self.bytes.len() - self.offset);
            output[..count].copy_from_slice(&self.bytes[self.offset..self.offset + count]);
            self.offset += count;
            Ok(count)
        }
    }

    #[test]
    fn streaming_prepare_matches_one_shot_across_random_input_windows() {
        let cases = [
            (
                LargeValueKind::Bytes,
                (0..LEAF_TARGET_BYTES * 90)
                    .map(|i| (i.wrapping_mul(31) & 0xff) as u8)
                    .collect::<Vec<_>>(),
            ),
            (
                LargeValueKind::String,
                "a😀é-stream-boundary-".repeat(180_000).into_bytes(),
            ),
            (
                LargeValueKind::Json,
                format!(
                    "[{{\"first\":true}},{}]",
                    (0..80_000)
                        .map(|i| format!("{{\"n\":{i}}}"))
                        .collect::<Vec<_>>()
                        .join(",")
                )
                .into_bytes(),
            ),
        ];
        for (kind, bytes) in cases {
            let expected = prepare(kind, &bytes).unwrap();
            for seed in 1..=12 {
                let mut staged = Vec::new();
                let (actual, stats) = prepare_streaming(
                    kind,
                    WindowReader {
                        bytes: &bytes,
                        offset: 0,
                        state: seed,
                    },
                    |chunk| {
                        staged.push(chunk);
                        Ok(())
                    },
                )
                .unwrap();
                assert_eq!(actual, expected.value_ref, "kind {kind:?}, seed {seed}");
                let chunks = |chunks: Vec<StagedChunk>| {
                    chunks
                        .into_iter()
                        .map(|chunk| (chunk.node_ref, chunk.encoded))
                        .collect::<std::collections::BTreeMap<_, _>>()
                };
                assert_eq!(
                    chunks(staged),
                    chunks(expected.staged_chunks.clone()),
                    "kind {kind:?}, seed {seed}"
                );
                assert!(stats.peak_leaf_buffer_bytes <= LEAF_MAX_BYTES + LEAF_MIN_BYTES);
                assert!(stats.peak_frontier_nodes <= BRANCH_MAX_CHILDREN * MAX_TREE_DEPTH);
            }
        }
    }

    #[test]
    fn streamed_and_buffered_json_admission_reject_numeric_overflow() {
        let bytes = b"1e400";
        assert_eq!(
            prepare(LargeValueKind::Json, bytes).unwrap_err(),
            Error::InvalidJson
        );

        let mut streamed_chunks = Vec::new();
        let streamed =
            prepare_streaming(LargeValueKind::Json, std::io::Cursor::new(bytes), |chunk| {
                streamed_chunks.push(chunk);
                Ok(())
            });

        let mut pushed_chunks = Vec::new();
        let mut preparation = PushStreamingPreparation::new(LargeValueKind::Json, |chunk| {
            pushed_chunks.push(chunk);
            Ok(())
        });
        let pushed = bytes
            .iter()
            .try_for_each(|byte| preparation.push(std::slice::from_ref(byte)))
            .and_then(|()| preparation.finish());

        assert_eq!(
            (streamed.map(|_| ()), pushed.map(|_| ())),
            (Err(Error::InvalidJson), Err(Error::InvalidJson)),
            "neither streaming entry point may publish a descriptor for buffered-invalid JSON"
        );
    }

    fn assert_json_admission_matches_serde(bytes: &[u8]) {
        let expected = serde_json::from_slice::<serde_json::Value>(bytes)
            .map(|_| ())
            .map_err(|_| Error::InvalidJson);
        assert_eq!(prepare(LargeValueKind::Json, bytes).map(|_| ()), expected);
        let streamed =
            prepare_streaming(
                LargeValueKind::Json,
                std::io::Cursor::new(bytes),
                |_| Ok(()),
            )
            .map(|_| ());
        let mut preparation = PushStreamingPreparation::new(LargeValueKind::Json, |_| Ok(()));
        let pushed = bytes
            .iter()
            .try_for_each(|byte| preparation.push(std::slice::from_ref(byte)))
            .and_then(|()| preparation.finish())
            .map(|_| ());
        assert_eq!(
            (streamed, pushed),
            (expected.clone(), expected),
            "streamed and pushed admission must match serde_json for {bytes:?}"
        );
    }

    #[test]
    fn json_admission_matches_serde_for_ascii_whitespace() {
        for whitespace in [b' ', b'\t', b'\n', b'\r', b'\x0b', b'\x0c'] {
            for (prefix, suffix) in [("", "0"), ("0", ""), ("[0,", "1]"), ("{\"key\":", "0}")] {
                let bytes = [prefix.as_bytes(), &[whitespace], suffix.as_bytes()].concat();
                assert_json_admission_matches_serde(&bytes);
            }
        }
    }

    #[test]
    fn json_admission_matches_serde_at_recursion_boundary() {
        for depth in [126, 127, 128, 129] {
            let arrays = format!("{}0{}", "[".repeat(depth), "]".repeat(depth));
            assert_json_admission_matches_serde(arrays.as_bytes());
            let objects = format!("{}0{}", "{\"key\":".repeat(depth), "}".repeat(depth));
            assert_json_admission_matches_serde(objects.as_bytes());
            let mixed = format!(
                "{}0{}",
                "[{\"key\":".repeat(depth / 2),
                "}]".repeat(depth / 2)
            );
            let mixed = if depth % 2 == 1 {
                format!("[{mixed}]")
            } else {
                mixed
            };
            assert_json_admission_matches_serde(mixed.as_bytes());
        }
    }

    #[test]
    fn json_admission_matches_serde_value_across_numeric_and_surrogate_splits() {
        for (name, json) in json_admission_corpus() {
            let bytes = json.as_bytes();
            let expected = serde_json::from_slice::<serde_json::Value>(bytes)
                .map(|_| ())
                .map_err(|_| Error::InvalidJson);
            let buffered = prepare(LargeValueKind::Json, bytes);
            assert_eq!(
                buffered.as_ref().map(|_| ()).map_err(Clone::clone),
                expected,
                "buffered {name}"
            );
            let check = |result: Result<(LargeValueRef, StreamingPrepareStats), Error>| {
                assert_eq!(
                    result.as_ref().map(|_| ()).map_err(Clone::clone),
                    expected,
                    "{name}"
                );
                if let Ok((value, _)) = result {
                    let buffered = &buffered.as_ref().unwrap().value_ref;
                    assert_eq!(
                        value.logical_hash, buffered.logical_hash,
                        "{name}: literal identity"
                    );
                    assert_eq!(
                        value.byte_length, buffered.byte_length,
                        "{name}: byte length"
                    );
                    assert_eq!(
                        value.utf16_length, buffered.utf16_length,
                        "{name}: UTF-16 length"
                    );
                }
            };
            check(prepare_streaming(
                LargeValueKind::Json,
                std::io::Cursor::new(bytes),
                |_| Ok(()),
            ));
            check(prepare_streaming(
                LargeValueKind::Json,
                WindowReader {
                    bytes,
                    offset: 0,
                    state: 7,
                },
                |_| Ok(()),
            ));

            let mut pushed = PushStreamingPreparation::new(LargeValueKind::Json, |_| Ok(()));
            check(
                bytes
                    .iter()
                    .try_for_each(|byte| pushed.push(std::slice::from_ref(byte)))
                    .and_then(|()| pushed.finish()),
            );

            let splits = if bytes.len() < 128 {
                (0..=bytes.len()).collect::<Vec<_>>()
            } else {
                vec![
                    0,
                    1,
                    bytes.len() / 2,
                    bytes.len() - 1,
                    bytes.len(),
                    LEAF_MIN_BYTES - 1,
                    LEAF_MIN_BYTES,
                    LEAF_MIN_BYTES + 1,
                ]
            };
            for split in splits {
                let mut pushed = PushStreamingPreparation::new(LargeValueKind::Json, |_| Ok(()));
                check(
                    pushed
                        .push(&bytes[..split])
                        .and_then(|()| pushed.push(&bytes[split..]))
                        .and_then(|()| pushed.finish()),
                );
                // A chained reader forces the same two windows at its public
                // Read seam, including every escape byte in the short corpus.
                use std::io::Read;
                check(prepare_streaming(
                    LargeValueKind::Json,
                    std::io::Cursor::new(&bytes[..split])
                        .chain(std::io::Cursor::new(&bytes[split..])),
                    |_| Ok(()),
                ));
            }
        }
    }

    #[test]
    fn push_streaming_json_does_not_buffer_one_large_string_token() {
        let bytes = format!("{{\"body\":\"{}\"}}", "x".repeat(LEAF_MAX_BYTES * 8)).into_bytes();
        let expected = prepare(LargeValueKind::Json, &bytes).unwrap();
        let mut staged = Vec::new();
        let mut preparation = PushStreamingPreparation::new(LargeValueKind::Json, |chunk| {
            staged.push(chunk);
            Ok(())
        });
        for byte in &bytes {
            preparation.push(std::slice::from_ref(byte)).unwrap();
        }
        let (actual, stats) = preparation.finish().unwrap();
        assert_eq!(actual, expected.value_ref);
        assert!(stats.peak_leaf_buffer_bytes <= LEAF_MAX_BYTES + 1);
        assert_eq!(staged.len(), expected.staged_chunks.len());
    }

    #[test]
    fn streaming_prepare_never_returns_a_descriptor_after_validation_or_staging_failure() {
        let mut invalid_json = br#"["#.to_vec();
        invalid_json.extend(std::iter::repeat_n(b' ', LEAF_MAX_BYTES + 1));
        invalid_json.extend_from_slice(br#"{"ok":true}, invalid]"#);
        let mut staged = 0;
        let result = prepare_streaming(
            LargeValueKind::Json,
            std::io::Cursor::new(&invalid_json),
            |_| {
                staged += 1;
                Ok(())
            },
        );
        assert_eq!(result.unwrap_err(), Error::InvalidJson);
        assert!(
            staged >= 1,
            "immutable orphan staging is allowed before validation finishes"
        );

        let mut attempts = 0;
        let result = prepare_streaming(
            LargeValueKind::Bytes,
            std::io::Cursor::new(vec![7; LEAF_MAX_BYTES * 3]),
            |_| {
                attempts += 1;
                if attempts == 2 {
                    Err(Error::MalformedScalar)
                } else {
                    Ok(())
                }
            },
        );
        assert_eq!(result.unwrap_err(), Error::MalformedScalar);
        assert_eq!(attempts, 2);
    }

    // Internal format tests are appropriate here: tree shape, authenticated
    // metrics and boundary locality are not observable through public queries
    // until the physical scalar arm is wired into records.
    #[test]
    fn construction_is_deterministic_and_text_metrics_are_exact() {
        let text = "a😀é".repeat(100_000);
        let first = prepare(LargeValueKind::String, text.as_bytes()).unwrap();
        let second = prepare(LargeValueKind::String, text.as_bytes()).unwrap();

        assert_eq!(first, second);
        assert_eq!(first.value_ref.byte_length, text.len() as u64);
        assert_eq!(
            first.value_ref.utf16_length,
            Some(text.encode_utf16().count() as u64)
        );
        for chunk in &first.staged_chunks {
            decode_node(
                LargeValueKind::String,
                chunk.node_ref.object_hash,
                &chunk.encoded,
            )
            .unwrap();
        }
    }

    #[test]
    fn append_tail_updates_metrics_without_changing_base_identity_or_reading_chunks() {
        let base = "base 😀 ".repeat(20_000);
        let prepared = prepare(LargeValueKind::String, base.as_bytes()).unwrap();
        let base_hash = prepared.value_ref.logical_hash;
        let suffix = "suffix 🪩";

        let TailAppendOutcome::Updated(updated) =
            append_tail(&prepared.value_ref, suffix.as_bytes().to_vec()).unwrap()
        else {
            panic!("one small append must fit in the tail");
        };

        assert_eq!(updated.logical_hash, base_hash);
        assert_eq!(updated.edit_tail.len(), 1);
        assert_eq!(updated.byte_length, (base.len() + suffix.len()) as u64);
        assert_eq!(
            updated.utf16_length,
            Some(base.encode_utf16().count() as u64 + suffix.encode_utf16().count() as u64)
        );
    }

    /// Alice persists each value kind; Bob restores its required length semantics.
    /// The codec boundary pins field widths that logical row reads cannot expose.
    #[test]
    fn descriptor_utf16_length_layout_follows_value_kind() {
        for (kind, bytes, utf16) in [
            (LargeValueKind::Bytes, b"bytes".as_slice(), None),
            (LargeValueKind::String, "é🙂".as_bytes(), Some(3_u64)),
            (LargeValueKind::Json, "\"🙂\"".as_bytes(), Some(4)),
        ] {
            let value = prepare(kind, bytes).unwrap().value_ref;
            let encoded = encode_large_value_ref(&value).unwrap();
            assert_eq!(encoded.len(), if utf16.is_some() { 94 } else { 86 });
            assert_eq!(&encoded[2..10], &(bytes.len() as u64).to_le_bytes());
            if let Some(length) = utf16 {
                assert_eq!(&encoded[10..18], &length.to_le_bytes());
            }
            assert_eq!(decode_large_value_ref(&encoded).unwrap(), value);
            assert_eq!(value.utf16_length, utf16);
            let scalar = StoredScalar::Chunked(value.clone());
            let stored = encode_stored_scalar(kind, &scalar).unwrap();
            assert_eq!(decode_stored_scalar(kind, &stored).unwrap(), scalar);

            let mut invalid = value;
            invalid.utf16_length = if utf16.is_some() { None } else { Some(0) };
            assert_eq!(
                encode_large_value_ref(&invalid),
                Err(Error::MalformedScalar)
            );
            assert_eq!(
                encode_stored_scalar(kind, &StoredScalar::Chunked(invalid)),
                Err(Error::MalformedScalar)
            );
        }
    }

    /// Alice persists byte/text edits; Bob derives insertion metrics on reopen.
    /// This codec-level test pins omitted fields that public rows cannot expose.
    #[test]
    fn edit_codec_derives_insert_utf16_length_from_insert_bytes() {
        for (kind, insertion, expected_utf16) in [
            (LargeValueKind::Bytes, vec![0xff, 0x80], 0),
            (LargeValueKind::String, "é🙂".as_bytes().to_vec(), 3),
        ] {
            let prepared = prepare(kind, b"base").unwrap();
            let TailAppendOutcome::Updated(updated) =
                append_tail(&prepared.value_ref, insertion.clone()).unwrap()
            else {
                panic!("one small append must fit in the tail");
            };
            let encoded = encode_large_value_ref(&updated).unwrap();
            let decoded = decode_large_value_ref(&encoded).unwrap();
            assert_eq!(decoded, updated);
            assert_eq!(decoded.edit_tail[0].insert_utf16_length, expected_utf16);
            let scalar = StoredScalar::Chunked(updated.clone());
            let scalar_bytes = encode_stored_scalar(kind, &scalar).unwrap();
            assert_eq!(decode_stored_scalar(kind, &scalar_bytes).unwrap(), scalar);
            if kind == LargeValueKind::Bytes {
                for coordinate in [0, 1] {
                    let mut malformed = updated.clone();
                    if coordinate == 0 {
                        malformed.edit_tail[0].utf16_offset = 1;
                    } else {
                        malformed.edit_tail[0].delete_utf16_length = 1;
                    }
                    assert_eq!(
                        encode_large_value_ref(&malformed),
                        Err(Error::MalformedScalar)
                    );
                }
            }

            // Bytes store two fixed coordinates; text stores four.
            let mut edit_bytes = 4_u64.to_le_bytes().to_vec();
            edit_bytes.extend_from_slice(&0_u64.to_le_bytes());
            if kind == LargeValueKind::String {
                edit_bytes.extend_from_slice(&4_u64.to_le_bytes());
                edit_bytes.extend_from_slice(&0_u64.to_le_bytes());
            }
            edit_bytes.extend_from_slice(&insertion);
            assert!(encoded.ends_with(&edit_bytes));

            let insertion_start = encoded.len() - insertion.len();
            let mut previous_layout = encoded[..insertion_start].to_vec();
            if kind == LargeValueKind::Bytes {
                previous_layout.extend_from_slice(&[0; 16]);
            } else {
                previous_layout.extend_from_slice(&expected_utf16.to_le_bytes());
            }
            previous_layout.extend_from_slice(&insertion);
            assert!(decode_large_value_ref(&previous_layout).is_err());

            if kind == LargeValueKind::String {
                let mut invalid_utf8 = encoded;
                *invalid_utf8.last_mut().unwrap() = 0xff;
                assert_eq!(
                    decode_large_value_ref(&invalid_utf8),
                    Err(Error::InvalidUtf8)
                );
            }
        }
    }

    #[test]
    fn append_tail_reports_consolidation_before_exceeding_its_hard_bound() {
        let prepared = prepare(LargeValueKind::Bytes, b"base").unwrap();
        let mut value = prepared.value_ref.clone();
        // A one-byte append encodes to two bytes: an epoch holds this many.
        for _ in 0..epoch::EPOCH_EDIT_BYTES / 2 {
            let TailAppendOutcome::Updated(updated) = append_tail(&value, vec![1]).unwrap() else {
                panic!("a tail that fits an epoch should be admissible");
            };
            value = updated;
        }
        assert!(matches!(
            append_tail(&value, vec![2]).unwrap(),
            TailAppendOutcome::ConsolidationRequired(_)
        ));
        // So does one insertion over an epoch's insertion bound.
        assert!(matches!(
            append_tail(&prepared.value_ref, vec![3; epoch::EPOCH_INSERT_BYTES + 1]).unwrap(),
            TailAppendOutcome::ConsolidationRequired(_)
        ));
    }

    #[test]
    fn byte_replacement_enters_tail_without_fetching_the_base_tree() {
        let prepared = prepare(LargeValueKind::Bytes, &vec![3; LEAF_TARGET_BYTES * 4]).unwrap();
        let mut inputs = ChunkInputs::default();
        let TailEditOutcome::Updated(updated) = replace_tail_attempt(
            &prepared.value_ref,
            100,
            20,
            b"replacement".to_vec(),
            &mut inputs,
        )
        .unwrap() else {
            panic!("small replacement must fit in the edit tail");
        };

        assert!(inputs.take_missing_chunks().is_empty());
        assert_eq!(updated.edit_tail.len(), 1);
        assert_eq!(
            updated.byte_length,
            prepared.value_ref.byte_length - 20 + b"replacement".len() as u64
        );
    }

    #[test]
    fn text_replacement_reads_only_its_range_and_preserves_exact_utf16_metrics() {
        let base = "zero 😀 one é two ".repeat(30_000);
        let prepared = prepare(LargeValueKind::String, base.as_bytes()).unwrap();
        let start = base.find("😀").unwrap() as u64;
        let end = start + "😀".len() as u64;
        let available = prepared
            .staged_chunks
            .iter()
            .map(|chunk| {
                (
                    ChunkRequest {
                        object_hash: chunk.node_ref.object_hash.0,
                    },
                    bytes::Bytes::copy_from_slice(&chunk.encoded),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut inputs = ChunkInputs::default();
        let updated = loop {
            match replace_tail_attempt(
                &prepared.value_ref,
                start,
                end - start,
                "🪩".as_bytes().to_vec(),
                &mut inputs,
            ) {
                Ok(TailEditOutcome::Updated(value)) => break value,
                Ok(TailEditOutcome::ConsolidationRequired(_)) => panic!("small edit must fit"),
                Err(AttemptError::Blocked) => {
                    for request in inputs.take_missing_chunks() {
                        inputs.install_chunk(request.clone(), available[&request].clone());
                    }
                }
                Err(error) => panic!("unexpected text edit failure: {error}"),
            }
        };
        let mut expected = base;
        expected.replace_range(start as usize..end as usize, "🪩");
        assert_eq!(updated.byte_length, expected.len() as u64);
        assert_eq!(
            updated.utf16_length,
            Some(expected.encode_utf16().count() as u64)
        );
    }

    #[test]
    fn append_consolidation_reads_only_right_spine_and_reuses_untouched_locators() {
        let mut state = 0xa11c_e5ed_1234_5678_u64;
        let base = (0..LEAF_TARGET_BYTES * 96)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect::<Vec<_>>();
        let prepared = prepare(LargeValueKind::Bytes, &base).unwrap();
        // Too large for an epoch: the transient tail is consolidated.
        let suffix = vec![0x5a; LEAF_TARGET_BYTES * 3];
        let TailAppendOutcome::ConsolidationRequired(with_tail) =
            append_tail(&prepared.value_ref, suffix.clone()).unwrap()
        else {
            panic!("an append over the epoch insertion bound consolidates");
        };
        let available = prepared
            .staged_chunks
            .iter()
            .map(|chunk| {
                (
                    ChunkRequest {
                        object_hash: chunk.node_ref.object_hash.0,
                    },
                    bytes::Bytes::copy_from_slice(&chunk.encoded),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut inputs = ChunkInputs::default();
        let mut requested = std::collections::BTreeSet::new();
        let consolidated = loop {
            match consolidate_appends_attempt(&with_tail, &mut inputs) {
                Ok(value) => break value,
                Err(AttemptError::Blocked) => {
                    let missing = inputs.take_missing_chunks();
                    assert_eq!(missing.len(), 1, "right-spine discovery is sequential");
                    for request in missing {
                        requested.insert(request.clone());
                        inputs.install_chunk(request.clone(), available[&request].clone());
                    }
                }
                Err(error) => panic!("unexpected consolidation failure: {error}"),
            }
        };
        let mut final_bytes = base;
        final_bytes.extend_from_slice(&suffix);
        let fresh = prepare(LargeValueKind::Bytes, &final_bytes).unwrap();

        assert!(consolidated.value_ref.edit_tail.is_empty());
        assert_eq!(
            consolidated.value_ref.logical_hash, fresh.value_ref.logical_hash,
            "localized consolidation must produce the canonical fresh tree"
        );
        assert!(
            requested.len() * 8 < prepared.staged_chunks.len(),
            "consolidation requested {} of {} old nodes",
            requested.len(),
            prepared.staged_chunks.len()
        );
        let old_refs = prepared
            .staged_chunks
            .iter()
            .map(|chunk| chunk.node_ref.clone())
            .collect::<std::collections::BTreeSet<_>>();
        let newly_staged_refs = consolidated
            .staged_chunks
            .iter()
            .map(|chunk| chunk.node_ref.clone())
            .collect::<std::collections::BTreeSet<_>>();
        let encoded_by_hash = prepared
            .staged_chunks
            .iter()
            .chain(consolidated.staged_chunks.iter())
            .map(|chunk| (chunk.node_ref.object_hash, chunk.encoded.as_slice()))
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut reachable = std::collections::BTreeSet::new();
        let mut pending = vec![consolidated.value_ref.root.clone()];
        while let Some(node_ref) = pending.pop() {
            if !reachable.insert(node_ref.clone()) {
                continue;
            }
            let node = decode_node(
                LargeValueKind::Bytes,
                node_ref.object_hash,
                encoded_by_hash[&node_ref.object_hash],
            )
            .unwrap();
            if let ChunkNode::Branch { children, .. } = node {
                pending.extend(children.into_iter().map(|child| child.node_ref));
            }
        }
        let reused = reachable.intersection(&old_refs).count();
        assert!(
            reused * 4 > old_refs.len() * 3,
            "only {reused} of {} exact old NodeRefs remained reachable",
            old_refs.len()
        );
        assert!(
            old_refs.len() > newly_staged_refs.len() * 4,
            "localized append staged too many replacement nodes"
        );
    }

    #[test]
    fn middle_edit_consolidation_resynchronizes_and_splices_only_local_tree_groups() {
        let mut state = 0x55aa_1234_dead_beef_u64;
        let base = (0..LEAF_TARGET_BYTES * 128)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect::<Vec<_>>();
        let prepared = prepare(LargeValueKind::Bytes, &base).unwrap();
        let offset = (base.len() / 2 + 137) as u64;
        let delete_length = 31_u64;
        let insert = b"localized replacement payload".to_vec();
        let mut no_reads = ChunkInputs::default();
        let TailEditOutcome::Updated(with_tail) = replace_tail_attempt(
            &prepared.value_ref,
            offset,
            delete_length,
            insert.clone(),
            &mut no_reads,
        )
        .unwrap() else {
            panic!("small edit must fit");
        };
        assert!(no_reads.take_missing_chunks().is_empty());
        let available = prepared
            .staged_chunks
            .iter()
            .map(|chunk| {
                (
                    ChunkRequest {
                        object_hash: chunk.node_ref.object_hash.0,
                    },
                    bytes::Bytes::copy_from_slice(&chunk.encoded),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut inputs = ChunkInputs::default();
        let mut requested = std::collections::BTreeSet::new();
        let consolidated = loop {
            match consolidate_single_edit_attempt(&with_tail, &mut inputs) {
                Ok(value) => break value,
                Err(AttemptError::Blocked) => {
                    for request in inputs.take_missing_chunks() {
                        requested.insert(request.clone());
                        inputs.install_chunk(request.clone(), available[&request].clone());
                    }
                }
                Err(error) => panic!("unexpected middle consolidation failure: {error}"),
            }
        };
        let mut expected = base;
        expected.splice(offset as usize..(offset + delete_length) as usize, insert);
        let fresh = prepare(LargeValueKind::Bytes, &expected).unwrap();

        assert_eq!(
            consolidated.value_ref.logical_hash,
            fresh.value_ref.logical_hash
        );
        assert!(consolidated.value_ref.edit_tail.is_empty());
        assert!(
            requested.len() * 6 < prepared.staged_chunks.len(),
            "middle splice fetched {} of {} old nodes",
            requested.len(),
            prepared.staged_chunks.len()
        );
        assert!(
            consolidated.staged_chunks.len() * 5 < prepared.staged_chunks.len(),
            "middle splice staged {} nodes against {} old nodes",
            consolidated.staged_chunks.len(),
            prepared.staged_chunks.len()
        );
        let old_refs = prepared
            .staged_chunks
            .iter()
            .map(|chunk| chunk.node_ref.clone())
            .collect::<std::collections::BTreeSet<_>>();
        let encoded_by_hash = prepared
            .staged_chunks
            .iter()
            .chain(consolidated.staged_chunks.iter())
            .map(|chunk| (chunk.node_ref.object_hash, chunk.encoded.as_slice()))
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut reachable = std::collections::BTreeSet::new();
        let mut pending = vec![consolidated.value_ref.root.clone()];
        while let Some(node_ref) = pending.pop() {
            if !reachable.insert(node_ref.clone()) {
                continue;
            }
            if let ChunkNode::Branch { children, .. } = decode_node(
                LargeValueKind::Bytes,
                node_ref.object_hash,
                encoded_by_hash[&node_ref.object_hash],
            )
            .unwrap()
            {
                pending.extend(children.into_iter().map(|child| child.node_ref));
            }
        }
        let reused = reachable.intersection(&old_refs).count();
        assert!(
            reused * 4 > old_refs.len() * 3,
            "only {reused} of {} exact locator-bearing old nodes survived",
            old_refs.len()
        );
    }

    #[test]
    fn full_deletion_consolidates_to_one_empty_root_leaf() {
        let mut state = 0x5eed_cafe_f00d_beef_u64;
        let base = (0..LEAF_TARGET_BYTES * 96)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect::<Vec<_>>();
        let prepared = prepare(LargeValueKind::Bytes, &base).unwrap();
        let encoded_by_hash = prepared
            .staged_chunks
            .iter()
            .map(|chunk| (chunk.node_ref.object_hash, chunk.encoded.as_slice()))
            .collect::<std::collections::BTreeMap<_, _>>();
        let ChunkNode::Branch { children, .. } = decode_node(
            LargeValueKind::Bytes,
            prepared.value_ref.root.object_hash,
            encoded_by_hash[&prepared.value_ref.root.object_hash],
        )
        .unwrap() else {
            panic!("fixture must have a branch root");
        };
        assert!(
            children.iter().any(|child| matches!(
                decode_node(
                    LargeValueKind::Bytes,
                    child.node_ref.object_hash,
                    encoded_by_hash[&child.node_ref.object_hash],
                ),
                Ok(ChunkNode::Branch { .. })
            )),
            "fixture must have multiple branch levels"
        );

        let available = prepared
            .staged_chunks
            .iter()
            .map(|chunk| {
                (
                    ChunkRequest {
                        object_hash: chunk.node_ref.object_hash.0,
                    },
                    bytes::Bytes::copy_from_slice(&chunk.encoded),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut edit_inputs = ChunkInputs::default();
        let TailEditOutcome::Updated(with_tail) = replace_tail_attempt(
            &prepared.value_ref,
            0,
            base.len() as u64,
            Vec::new(),
            &mut edit_inputs,
        )
        .unwrap() else {
            panic!("one deletion must fit the edit tail");
        };
        assert!(edit_inputs.take_missing_chunks().is_empty());

        let mut inputs = ChunkInputs::default();
        let consolidated = loop {
            match consolidate_single_edit_attempt(&with_tail, &mut inputs) {
                Ok(value) => break value,
                Err(AttemptError::Blocked) => {
                    for request in inputs.take_missing_chunks() {
                        inputs.install_chunk(request.clone(), available[&request].clone());
                    }
                }
                Err(error) => panic!("unexpected full-deletion consolidation failure: {error}"),
            }
        };

        assert_eq!(consolidated.value_ref.byte_length, 0);
        assert!(consolidated.value_ref.edit_tail.is_empty());
        assert_eq!(
            consolidated.staged_chunks.len(),
            1,
            "a fully deleted value must not retain zero-metric branch ancestors"
        );
        let root = &consolidated.staged_chunks[0];
        assert_eq!(root.node_ref, consolidated.value_ref.root);
        assert_eq!(
            decode_node(
                LargeValueKind::Bytes,
                root.node_ref.object_hash,
                &root.encoded,
            ),
            Ok(ChunkNode::Leaf {
                format: FORMAT_VERSION,
                kind: LargeValueKind::Bytes,
                bytes: Vec::new(),
            })
        );

        let mut materialize_inputs = ChunkInputs::default();
        materialize_inputs.install_chunk(
            ChunkRequest {
                object_hash: root.node_ref.object_hash.0,
            },
            bytes::Bytes::copy_from_slice(&root.encoded),
        );
        let materialized =
            materialize_attempt(&consolidated.value_ref, &mut materialize_inputs).unwrap();
        assert_eq!(materialized, Vec::<u8>::new());
        assert!(materialize_inputs.take_missing_chunks().is_empty());
    }

    #[test]
    fn complete_suffix_deletion_collapses_singleton_root_to_fresh_leaf() {
        let retained = b"retained prefix".to_vec();
        let deleted = b"deleted suffix".to_vec();
        let mut staged_chunks = Vec::new();

        let retained_node = ChunkNode::Leaf {
            format: FORMAT_VERSION,
            kind: LargeValueKind::Bytes,
            bytes: retained.clone(),
        };
        let retained_leaf = stage_node(
            retained_node.clone(),
            node_metrics(LargeValueKind::Bytes, &retained_node).unwrap(),
            &mut staged_chunks,
        )
        .unwrap();
        let deleted_node = ChunkNode::Leaf {
            format: FORMAT_VERSION,
            kind: LargeValueKind::Bytes,
            bytes: deleted.clone(),
        };
        let deleted_leaf = stage_node(
            deleted_node.clone(),
            node_metrics(LargeValueKind::Bytes, &deleted_node).unwrap(),
            &mut staged_chunks,
        )
        .unwrap();
        let inner_node = ChunkNode::Branch {
            format: FORMAT_VERSION,
            kind: LargeValueKind::Bytes,
            children: [retained_leaf.clone(), deleted_leaf]
                .into_iter()
                .map(|child| BranchChild {
                    node_ref: child.node_ref,
                    metrics: child.metrics,
                    logical_hash: child.structural_hash,
                })
                .collect(),
        };
        let inner = stage_node(
            inner_node.clone(),
            node_metrics(LargeValueKind::Bytes, &inner_node).unwrap(),
            &mut staged_chunks,
        )
        .unwrap();
        let root_node = ChunkNode::Branch {
            format: FORMAT_VERSION,
            kind: LargeValueKind::Bytes,
            children: vec![BranchChild {
                node_ref: inner.node_ref,
                metrics: inner.metrics,
                logical_hash: inner.structural_hash,
            }],
        };
        let root = stage_node(
            root_node.clone(),
            node_metrics(LargeValueKind::Bytes, &root_node).unwrap(),
            &mut staged_chunks,
        )
        .unwrap();
        let prepared = PreparedLargeValue {
            value_ref: LargeValueRef {
                kind: LargeValueKind::Bytes,
                format_version: FORMAT_VERSION,
                logical_hash: root.structural_hash,
                root: root.node_ref,
                byte_length: root.metrics.byte_length,
                utf16_length: None,
                edit_tail: Vec::new(),
            },
            staged_chunks,
        };
        for chunk in &prepared.staged_chunks {
            if let ChunkNode::Branch { children, .. } = decode_node(
                LargeValueKind::Bytes,
                chunk.node_ref.object_hash,
                &chunk.encoded,
            )
            .unwrap()
            {
                assert!(
                    children.iter().all(|child| child.metrics.byte_length > 0),
                    "the valid source fixture must contain only positive branch children"
                );
            }
        }

        let available = prepared
            .staged_chunks
            .iter()
            .map(|chunk| {
                (
                    ChunkRequest {
                        object_hash: chunk.node_ref.object_hash.0,
                    },
                    bytes::Bytes::copy_from_slice(&chunk.encoded),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut edit_inputs = ChunkInputs::default();
        let TailEditOutcome::Updated(with_tail) = replace_tail_attempt(
            &prepared.value_ref,
            retained.len() as u64,
            deleted.len() as u64,
            Vec::new(),
            &mut edit_inputs,
        )
        .unwrap() else {
            panic!("one suffix deletion must fit the edit tail");
        };
        assert!(edit_inputs.take_missing_chunks().is_empty());

        let mut inputs = ChunkInputs::default();
        let consolidated = loop {
            match consolidate_single_edit_attempt(&with_tail, &mut inputs) {
                Ok(value) => break value,
                Err(AttemptError::Blocked) => {
                    for request in inputs.take_missing_chunks() {
                        inputs.install_chunk(request.clone(), available[&request].clone());
                    }
                }
                Err(error) => panic!("unexpected suffix-deletion failure: {error}"),
            }
        };
        let fresh = prepare(LargeValueKind::Bytes, &retained).unwrap();
        assert_eq!(
            consolidated.value_ref.logical_hash,
            fresh.value_ref.logical_hash
        );
        assert_eq!(consolidated.value_ref.root, fresh.value_ref.root);
        assert_eq!(consolidated.value_ref.root, retained_leaf.node_ref);
        assert!(
            consolidated.staged_chunks.is_empty(),
            "collapsing the root must reuse the unaffected leaf without staging wrappers"
        );
        let retained_chunk = prepared
            .staged_chunks
            .iter()
            .find(|chunk| chunk.node_ref == consolidated.value_ref.root)
            .unwrap();
        assert_eq!(
            decode_node(
                LargeValueKind::Bytes,
                retained_chunk.node_ref.object_hash,
                &retained_chunk.encoded,
            ),
            Ok(ChunkNode::Leaf {
                format: FORMAT_VERSION,
                kind: LargeValueKind::Bytes,
                bytes: retained.clone(),
            })
        );

        let mut materialize_inputs = ChunkInputs::default();
        for chunk in &prepared.staged_chunks {
            materialize_inputs.install_chunk(
                ChunkRequest {
                    object_hash: chunk.node_ref.object_hash.0,
                },
                bytes::Bytes::copy_from_slice(&chunk.encoded),
            );
        }
        assert_eq!(
            materialize_attempt(&consolidated.value_ref, &mut materialize_inputs).unwrap(),
            retained
        );
        assert!(materialize_inputs.take_missing_chunks().is_empty());
    }

    #[test]
    fn localized_single_edit_matches_fresh_tree_across_seeded_ranges() {
        let mut state = 0x9e37_79b9_cafe_f00d_u64;
        let base = (0..LEAF_TARGET_BYTES * 24)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect::<Vec<_>>();
        let prepared = prepare(LargeValueKind::Bytes, &base).unwrap();
        let available = prepared
            .staged_chunks
            .iter()
            .map(|chunk| {
                (
                    ChunkRequest {
                        object_hash: chunk.node_ref.object_hash.0,
                    },
                    bytes::Bytes::copy_from_slice(&chunk.encoded),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        for case in 0..24_u64 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let offset = state as usize % base.len();
            let delete = ((state >> 17) as usize % 257).min(base.len() - offset);
            let insert_len = (state >> 31) as usize % 257;
            let insert = (0..insert_len)
                .map(|index| state.wrapping_add(index as u64) as u8)
                .collect::<Vec<_>>();
            let mut edit_inputs = ChunkInputs::default();
            let TailEditOutcome::Updated(with_tail) = replace_tail_attempt(
                &prepared.value_ref,
                offset as u64,
                delete as u64,
                insert.clone(),
                &mut edit_inputs,
            )
            .unwrap() else {
                panic!("seeded small edit must fit");
            };
            let mut inputs = ChunkInputs::default();
            let consolidated = loop {
                match consolidate_single_edit_attempt(&with_tail, &mut inputs) {
                    Ok(value) => break value,
                    Err(AttemptError::Blocked) => {
                        for request in inputs.take_missing_chunks() {
                            inputs.install_chunk(request.clone(), available[&request].clone());
                        }
                    }
                    Err(error) => panic!("case {case} failed: {error}"),
                }
            };
            let mut expected = base.clone();
            expected.splice(offset..offset + delete, insert);
            let fresh = prepare(LargeValueKind::Bytes, &expected).unwrap();
            assert_eq!(
                consolidated.value_ref.logical_hash, fresh.value_ref.logical_hash,
                "case {case}: offset={offset} delete={delete} insert={insert_len}"
            );
        }
    }

    #[test]
    fn multi_edit_continuation_keeps_completed_local_splices_across_suspension() {
        let mut state = 0x1234_abcd_9876_fedc_u64;
        let base = (0..LEAF_TARGET_BYTES * 40)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect::<Vec<_>>();
        let prepared = prepare(LargeValueKind::Bytes, &base).unwrap();
        let available = prepared
            .staged_chunks
            .iter()
            .map(|chunk| {
                (
                    ChunkRequest {
                        object_hash: chunk.node_ref.object_hash.0,
                    },
                    bytes::Bytes::copy_from_slice(&chunk.encoded),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut logical = base;
        let mut with_tail = prepared.value_ref;
        let mut no_reads = ChunkInputs::default();
        for case in 0..8_usize {
            let offset = (logical.len() * (case + 1) / 10).min(logical.len() - 1);
            let delete = (case * 7 + 3).min(logical.len() - offset);
            let insert = vec![case as u8 + 10; case * 11 + 5];
            let TailEditOutcome::Updated(updated) = replace_tail_attempt(
                &with_tail,
                offset as u64,
                delete as u64,
                insert.clone(),
                &mut no_reads,
            )
            .unwrap() else {
                panic!("bounded seeded edit must fit");
            };
            with_tail = updated;
            logical.splice(offset..offset + delete, insert);
        }
        let mut continuation = ConsolidationContinuation::new(with_tail).unwrap();
        let mut inputs = ChunkInputs::default();
        let mut max_completed_before_block = 0_usize;
        let consolidated = loop {
            match continuation.step(&mut inputs) {
                Ok(Some(value)) => break value,
                Ok(None) => unreachable!(),
                Err(AttemptError::Blocked) => {
                    max_completed_before_block =
                        max_completed_before_block.max(continuation.next_edit);
                    for request in inputs.take_missing_chunks() {
                        inputs.install_chunk(request.clone(), available[&request].clone());
                    }
                }
                Err(error) => panic!("unexpected continuation failure: {error}"),
            }
        };
        let fresh = prepare(LargeValueKind::Bytes, &logical).unwrap();

        assert_eq!(
            consolidated.value_ref.logical_hash,
            fresh.value_ref.logical_hash
        );
        assert!(consolidated.value_ref.edit_tail.is_empty());
        assert!(
            max_completed_before_block > 0,
            "at least one completed splice should survive a later chunk suspension"
        );
    }

    #[test]
    fn object_hash_authenticates_locator_bearing_branch_bytes() {
        let bytes = vec![9; LEAF_MAX_BYTES * 2];
        let prepared = prepare(LargeValueKind::Bytes, &bytes).unwrap();
        let root = prepared.staged_chunks.last().unwrap();
        let mut corrupted = root.encoded.clone();
        *corrupted.last_mut().unwrap() ^= 1;
        assert_eq!(
            decode_node(LargeValueKind::Bytes, root.node_ref.object_hash, &corrupted,),
            Err(Error::ObjectHashMismatch)
        );
    }

    #[test]
    fn leaf_raw_bytes_are_exactly_authenticated_content() {
        let prepared = prepare(LargeValueKind::Bytes, b"canonical").unwrap();
        let mut encoded = prepared.staged_chunks[0].encoded.clone();
        encoded.push(0);
        let appended_hash = object_hash(&encoded);
        assert_eq!(
            decode_node(
                LargeValueKind::Bytes,
                prepared.staged_chunks[0].node_ref.object_hash,
                &encoded,
            ),
            Err(Error::ObjectHashMismatch)
        );
        assert!(matches!(
            decode_node(LargeValueKind::Bytes, appended_hash, &encoded),
            Ok(ChunkNode::Leaf { bytes, .. }) if bytes == b"canonical\0"
        ));

        let mut forged = prepared.value_ref.clone();
        forged.root.object_hash = appended_hash;
        let mut inputs = ChunkInputs::default();
        inputs.install_chunk(
            ChunkRequest {
                object_hash: appended_hash.0,
            },
            bytes::Bytes::from(encoded),
        );
        assert!(matches!(
            materialize_attempt(&forged, &mut inputs),
            Err(AttemptError::LargeValue(Error::DescriptorMismatch))
        ));
    }

    #[test]
    fn branch_decode_rejects_unused_bytes_under_a_recomputed_hash() {
        let prepared = prepare(LargeValueKind::Bytes, &vec![7; LEAF_MAX_BYTES * 2]).unwrap();
        let root = prepared.staged_chunks.last().unwrap();
        assert!(matches!(
            decode_node(
                LargeValueKind::Bytes,
                root.node_ref.object_hash,
                &root.encoded
            ),
            Ok(ChunkNode::Branch { .. })
        ));
        let mut encoded = root.encoded.clone();
        encoded.push(0);
        assert_eq!(
            decode_node(LargeValueKind::Bytes, object_hash(&encoded), &encoded),
            Err(Error::MalformedNode)
        );
    }

    /// Alice stores each branch kind; Bob reads its exact child layout without
    /// a descriptor. This codec-level fixture pins bytes and authenticated hashes.
    #[test]
    fn branch_child_lengths_have_kind_specific_golden_bytes() {
        for (kind, kind_tag, utf16) in [
            (LargeValueKind::Bytes, 0, None),
            (LargeValueKind::String, 1, Some(3)),
            (LargeValueKind::Json, 2, Some(3)),
        ] {
            let child = BranchChild {
                node_ref: NodeRef {
                    object_hash: ContentHash([0x11; 32]),
                },
                metrics: NodeMetrics {
                    byte_length: 7,
                    utf16_length: utf16,
                },
                logical_hash: ContentHash([0x33; 32]),
            };
            let node = ChunkNode::Branch {
                format: FORMAT_VERSION,
                kind,
                children: vec![child.clone()],
            };
            let mut expected = vec![1, 1, kind_tag, 1, 0, 0, 0];
            expected.extend_from_slice(&7_u64.to_le_bytes());
            if utf16.is_some() {
                expected.extend_from_slice(&3_u64.to_le_bytes());
            }
            let first_end = if utf16.is_some() { 52_u32 } else { 44 };
            expected.extend_from_slice(&first_end.to_le_bytes());
            expected.extend_from_slice(&[0x11; 32]);
            expected.extend_from_slice(&[0x33; 32]);
            assert_eq!(encode_node(&node).unwrap(), expected);
            assert_eq!(expected.len(), if utf16.is_some() { 91 } else { 83 });
            let hash = object_hash(&expected);
            let expected_hashes = [
                [
                    195, 212, 162, 65, 159, 4, 233, 245, 84, 192, 101, 31, 146, 204, 183, 231, 218,
                    44, 185, 114, 203, 31, 92, 250, 182, 240, 104, 89, 122, 224, 205, 204,
                ],
                [
                    126, 12, 124, 189, 40, 65, 241, 189, 83, 136, 45, 201, 157, 35, 167, 129, 151,
                    42, 186, 223, 205, 32, 170, 14, 43, 184, 64, 63, 115, 177, 9, 243,
                ],
                [
                    26, 87, 38, 110, 179, 118, 246, 176, 114, 47, 133, 52, 241, 243, 62, 178, 219,
                    157, 115, 6, 29, 27, 201, 211, 197, 120, 251, 68, 70, 58, 214, 124,
                ],
            ];
            assert_eq!(hash.0, expected_hashes[usize::from(kind_tag)]);
            assert_eq!(decode_authenticated_node(hash, &expected).unwrap(), node);
            for length in 0..expected.len() {
                assert!(
                    decode_authenticated_node(
                        object_hash(&expected[..length]),
                        &expected[..length]
                    )
                    .is_err()
                );
            }
            let mut malformed = child;
            malformed.metrics.utf16_length = if utf16.is_some() { None } else { Some(0) };
            assert_eq!(
                encode_node(&ChunkNode::Branch {
                    format: FORMAT_VERSION,
                    kind,
                    children: vec![malformed]
                }),
                Err(Error::MalformedNode)
            );
        }
    }

    #[test]
    fn oversized_branch_fanout_is_rejected_before_metadata_decode() {
        let child = BranchChild {
            node_ref: NodeRef {
                object_hash: ContentHash([1; 32]),
            },
            metrics: NodeMetrics {
                byte_length: 1,
                utf16_length: None,
            },
            logical_hash: ContentHash([3; 32]),
        };
        let encoded = encode_node(&ChunkNode::Branch {
            format: FORMAT_VERSION,
            kind: LargeValueKind::Bytes,
            children: vec![child; BRANCH_MAX_CHILDREN + 1],
        })
        .unwrap();
        assert!(encoded.len() <= MAX_ENCODED_NODE_BYTES);
        assert_eq!(
            preflight_node_bounds(&encoded, &chunk_node_schema(LargeValueKind::Bytes)),
            Err(Error::MalformedNode)
        );
        assert_eq!(
            decode_authenticated_node(object_hash(&encoded), &encoded),
            Err(Error::MalformedNode)
        );
    }

    #[test]
    fn untyped_authenticated_decode_rejects_oversized_bytes_before_hashing() {
        // This is intentionally an internal receipt: durable chunk-install
        // metadata authenticates nodes before any descriptor exists, so the
        // public row API cannot exercise this resource boundary directly.
        let encoded = vec![0; MAX_ENCODED_NODE_BYTES + 1];
        let matching_hash = object_hash(&encoded);
        assert_eq!(
            decode_node_untyped_authenticated(matching_hash, &encoded),
            Err(Error::MalformedNode),
            "the size ceiling must win even when the oversized payload's hash matches"
        );
    }

    #[test]
    fn raw_node_selector_rejects_hash_valid_future_branch_before_v1_decode() {
        // A future branch need not retain V1's child-array layout. Its object
        // hash is valid, but the only bytes this binary may inspect before
        // selecting the codec are the canonical outer tag and format byte.
        let encoded = vec![1, FORMAT_VERSION + 1];
        let hash = object_hash(&encoded);
        assert_eq!(
            decode_node_untyped_authenticated(hash, &encoded),
            Err(Error::UnsupportedFormat(FORMAT_VERSION + 1))
        );
        assert_eq!(
            decode_node_for_format(FORMAT_VERSION, LargeValueKind::Bytes, hash, &encoded),
            Err(Error::UnsupportedFormat(FORMAT_VERSION + 1)),
            "descriptor-led admission must not bind a future branch as V1"
        );
        assert_eq!(
            validate_staged_chunk_batch(
                LargeValueKind::Bytes,
                &[StagedChunk {
                    node_ref: NodeRef { object_hash: hash },
                    encoded,
                }],
            ),
            Err(Error::UnsupportedFormat(FORMAT_VERSION + 1)),
            "upload preflight must select the node format before V1 decoding"
        );
    }

    #[test]
    fn staged_batch_rejects_malformed_standard_enum_nodes_before_publication() {
        let prepared = prepare(LargeValueKind::Bytes, b"canonical").unwrap();
        let mut chunk = prepared.staged_chunks[0].clone();
        chunk.encoded[0] = u8::MAX;
        chunk.node_ref.object_hash = object_hash(&chunk.encoded);
        assert_eq!(
            validate_staged_chunk_batch(LargeValueKind::Bytes, &[chunk]),
            Err(Error::MalformedNode)
        );
    }

    #[test]
    fn json_preserves_literal_source_and_rejects_invalid_input() {
        let source = br#"{ "b": 2, "a": [1, true, null] }"#;
        let prepared = prepare(LargeValueKind::Json, source).unwrap();
        assert_ne!(prepared.value_ref.logical_hash, ContentHash([0; 32]));
        assert_eq!(
            prepare(LargeValueKind::Json, b"{broken"),
            Err(Error::InvalidJson)
        );
    }

    #[test]
    fn materialization_discovers_authenticated_tree_in_request_rounds() {
        let logical = (0..(LEAF_MAX_BYTES * 3))
            .map(|index| (index.wrapping_mul(31) & 0xff) as u8)
            .collect::<Vec<_>>();
        let prepared = prepare(LargeValueKind::Bytes, &logical).unwrap();
        let available = prepared
            .staged_chunks
            .iter()
            .map(|chunk| {
                (
                    ChunkRequest {
                        object_hash: chunk.node_ref.object_hash.0,
                    },
                    bytes::Bytes::copy_from_slice(&chunk.encoded),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut inputs = ChunkInputs::default();
        let mut rounds = 0;
        loop {
            match materialize_attempt(&prepared.value_ref, &mut inputs) {
                Ok(materialized) => {
                    assert_eq!(materialized, logical);
                    break;
                }
                Err(AttemptError::Blocked) => {
                    let requests = inputs.take_missing_chunks();
                    assert!(!requests.is_empty());
                    if rounds == 0 {
                        assert_eq!(requests.len(), 1, "only the root is initially known");
                    }
                    for request in requests {
                        inputs.install_chunk(request.clone(), available[&request].clone());
                    }
                    rounds += 1;
                }
                Err(error) => panic!("unexpected materialization failure: {error}"),
            }
        }
        assert!(rounds >= 2, "branches reveal descendants in later rounds");
    }

    #[test]
    fn reconstruction_reuses_unchanged_nodes() {
        let original = (0..(LEAF_MAX_BYTES * 12))
            .map(|index| (index.wrapping_mul(17) & 0xff) as u8)
            .collect::<Vec<_>>();
        let first = prepare(LargeValueKind::Bytes, &original).unwrap();
        let mut edited = original;
        edited.splice(LEAF_MAX_BYTES * 6..LEAF_MAX_BYTES * 6, [1, 2, 3, 4]);
        let second = prepare(LargeValueKind::Bytes, &edited).unwrap();

        let old = first
            .staged_chunks
            .iter()
            .map(|chunk| chunk.node_ref.object_hash)
            .collect::<std::collections::BTreeSet<_>>();
        let reused = second
            .staged_chunks
            .iter()
            .filter(|chunk| old.contains(&chunk.node_ref.object_hash))
            .count();
        assert!(reused > 0, "the edit must preserve unaffected nodes");
    }

    #[test]
    fn primitive_scalar_fast_codec_matches_generic_record_codec() {
        // Internal byte contract: public value equality cannot detect a changed
        // durable envelope. Build the reference through the ordinary enum/record
        // codecs, independently of the specialized scalar encoder.
        for (kind, payload) in [
            (LargeValueKind::Bytes, b"".as_slice()),
            (LargeValueKind::Bytes, b"\0\xffbytes".as_slice()),
            (LargeValueKind::String, "".as_bytes()),
            (LargeValueKind::String, "text 🙂".as_bytes()),
            (LargeValueKind::Json, b"null".as_slice()),
            (LargeValueKind::Json, br#" {"a":1,"a":2} "#.as_slice()),
        ] {
            let fields = primitive_payload_schema(kind)
                .ordered_values([(
                    PRIMITIVE_VALUE_FIELD,
                    primitive_value(kind, payload.to_vec()),
                )])
                .unwrap();
            let value = EnumValue::create(
                0,
                stored_scalar_schema(kind).case(0).unwrap().payload,
                &fields,
            )
            .unwrap();
            let reference = crate::records::encode_single_field_value(
                &Value::Enum(value),
                stored_scalar_value_type(kind),
            )
            .unwrap();
            let encoded = encode_primitive_stored_scalar(kind, payload).unwrap();
            assert_eq!(encoded, reference);
            assert_eq!(encoded, [b"\x00".as_slice(), payload].concat());
            assert_eq!(
                encode_stored_scalar(kind, &StoredScalar::Primitive(payload.to_vec())).unwrap(),
                reference
            );
            assert_eq!(
                decode_stored_scalar(kind, &encoded).unwrap(),
                StoredScalar::Primitive(payload.to_vec())
            );
        }
    }

    #[test]
    fn stored_scalar_schema_is_cached_per_declared_kind() {
        let bytes = stored_scalar_schema(LargeValueKind::Bytes);
        let string = stored_scalar_schema(LargeValueKind::String);
        let json = stored_scalar_schema(LargeValueKind::Json);

        assert!(std::ptr::eq(
            bytes,
            stored_scalar_schema(LargeValueKind::Bytes)
        ));
        assert!(std::ptr::eq(
            string,
            stored_scalar_schema(LargeValueKind::String)
        ));
        assert!(std::ptr::eq(
            json,
            stored_scalar_schema(LargeValueKind::Json)
        ));
        assert!(!std::ptr::eq(bytes, string));
        assert!(!std::ptr::eq(bytes, json));
        assert!(!std::ptr::eq(string, json));
    }

    fn reference_inline_scalar_bytes(kind: LargeValueKind, encoded: &[u8]) -> Result<&[u8], Error> {
        let schema = stored_scalar_schema(kind);
        let (tag, payload) =
            crate::records::split_variant_record(encoded).map_err(|_| Error::MalformedScalar)?;
        match tag {
            0 => {
                let descriptor = schema.case(0).map_err(|_| Error::MalformedScalar)?.payload;
                let values = descriptor
                    .bind(payload)
                    .to_values()
                    .map_err(|_| Error::MalformedScalar)?;
                let mut fields = primitive_payload_schema(kind).decode_values(&values)?;
                let value = take_durable_large_value_field(&mut fields, PRIMITIVE_VALUE_FIELD)?;
                if primitive_bytes(kind, &value).is_err()
                    || descriptor
                        .create(&values)
                        .map_err(|_| Error::MalformedScalar)?
                        != payload
                {
                    return Err(Error::MalformedScalar);
                }
                let span = descriptor
                    .field_span(payload, usize::from(PRIMITIVE_VALUE_FIELD - 1))
                    .map_err(|_| Error::MalformedScalar)?;
                Ok(&payload[span])
            }
            1 => {
                // Validate the complete descriptor before reporting that materialization is needed.
                let _ = decode_stored_scalar(kind, encoded)?;
                Err(Error::RequiresEvaluation)
            }
            _ => Err(Error::MalformedScalar),
        }
    }

    #[test]
    fn borrowed_inline_scalar_matches_roundtrip_oracle() {
        // Pin the premise of the borrowed path: slot 1 is the sole trailing
        // raw field. There are no reserved slots, offsets, or padding to skip.
        for kind in [
            LargeValueKind::Bytes,
            LargeValueKind::String,
            LargeValueKind::Json,
        ] {
            let schema = primitive_payload_schema(kind);
            assert_eq!(schema.field_ids, [1]);
            assert_eq!(schema.descriptor.fields().len(), 1);
            assert_eq!(
                schema.descriptor.fields()[0].value_type,
                if kind == LargeValueKind::Bytes {
                    ValueType::raw_bytes()
                } else {
                    ValueType::raw_string()
                }
            );
            let check = |bytes: &[u8]| {
                assert_eq!(
                    inline_scalar_bytes(kind, bytes),
                    reference_inline_scalar_bytes(kind, bytes),
                    "kind={kind:?}, bytes={bytes:?}"
                );
            };
            for payload in [
                b"".as_slice(),
                b"plain",
                b"{}",
                b"null",
                b" {\"a\":1,\"a\":2} ",
                b"{}x",
                b"[1,",
                b"\xff",
                b"\0",
                "text 🙂".as_bytes(),
            ] {
                let encoded = [b"\x00".as_slice(), payload].concat();
                check(&encoded);
                for end in 0..encoded.len() {
                    check(&encoded[..end]);
                }
                for index in 0..encoded.len() {
                    for replacement in 0..=255 {
                        let mut changed = encoded.clone();
                        changed[index] = replacement;
                        check(&changed);
                    }
                }
                for prefix in [
                    vec![0],
                    vec![1],
                    vec![4],
                    vec![0x82, 0],
                    vec![0x82, 0x80, 0],
                    vec![0xff; 12],
                ] {
                    check(&[prefix.as_slice(), payload].concat());
                }
            }
            let large = vec![b' '; 65536];
            check(&[b"\x00".as_slice(), &large].concat());
        }
    }

    #[test]
    fn primitive_scalar_decode_preserves_exact_validation_without_reencoding() {
        // Exercise the persisted byte boundary, including logical errors that
        // a public write correctly refuses to produce. Primitive payloads have
        // one final raw field: there are no offsets or reserved payload slots.
        // Extra bytes are valid Bytes data, but invalid after a JSON document.
        fn reference(kind: LargeValueKind, encoded: &[u8]) -> Result<StoredScalar, Error> {
            let (tag, payload) = crate::records::split_variant_record(encoded)
                .map_err(|_| Error::MalformedScalar)?;
            if tag != 0 {
                return Err(Error::MalformedScalar);
            }
            let decoded = StoredScalar::Primitive(decode_primitive_payload(kind, payload)?);
            if encode_stored_scalar_canonical(kind, &decoded)? != encoded {
                return Err(Error::MalformedScalar);
            }
            Ok(decoded)
        }

        for (kind, logical) in [
            (LargeValueKind::Bytes, b"abc".as_slice()),
            (LargeValueKind::String, "text 🙂".as_bytes()),
            (LargeValueKind::Json, br#" {"a":1,"a":2} "#.as_slice()),
        ] {
            let encoded =
                encode_stored_scalar(kind, &StoredScalar::Primitive(logical.to_vec())).unwrap();
            assert_eq!(encoded, [b"\x00".as_slice(), logical].concat());
            STORED_SCALAR_CANONICAL_ENCODE_CALLS.with(|calls| calls.set(0));
            assert_eq!(
                decode_stored_scalar(kind, &encoded),
                Ok(StoredScalar::Primitive(logical.to_vec()))
            );
            STORED_SCALAR_CANONICAL_ENCODE_CALLS.with(|calls| {
                assert_eq!(
                    calls.get(),
                    0,
                    "canonical primitive decoding must not reconstruct the envelope again"
                )
            });

            for length in 0..=encoded.len() {
                let bytes = &encoded[..length];
                assert_eq!(decode_stored_scalar(kind, bytes), reference(kind, bytes));
            }
            for index in 1..encoded.len() {
                for byte in [0, 1, 0x7f, 0x80, 0xff] {
                    let mut changed = encoded.clone();
                    changed[index] = byte;
                    assert_eq!(
                        decode_stored_scalar(kind, &changed),
                        reference(kind, &changed)
                    );
                }
            }
            for prefix in [
                vec![2],
                vec![3],
                vec![4],
                vec![0x82, 0],
                vec![0x82, 0x80, 0],
                vec![0x82, 0x80, 0x80, 0x80, 0x10],
            ] {
                let invalid = [prefix.as_slice(), logical].concat();
                assert_eq!(
                    decode_stored_scalar(kind, &invalid),
                    Err(Error::MalformedScalar)
                );
            }
        }
        assert_eq!(
            decode_stored_scalar(LargeValueKind::String, &[0, 0xff]),
            Err(Error::InvalidUtf8)
        );
        for payload in [b"not json".as_slice(), b"{}x", b"{}\0"] {
            assert!(
                decode_stored_scalar(
                    LargeValueKind::Json,
                    &[b"\x00".as_slice(), payload].concat()
                )
                .is_err()
            );
        }
        assert_eq!(
            decode_stored_scalar(LargeValueKind::Bytes, &[0, 0, 0xff]),
            Ok(StoredScalar::Primitive(vec![0, 0xff]))
        );
    }

    #[test]
    fn stored_scalar_is_a_canonical_generic_enum_for_each_declared_kind() {
        for prefix in 0..=u8::MAX {
            let logical = vec![prefix, 1, 2, 3];
            let encoded = encode_stored_scalar(
                LargeValueKind::Bytes,
                &StoredScalar::Primitive(logical.clone()),
            )
            .unwrap();
            assert_eq!(
                decode_stored_scalar(LargeValueKind::Bytes, &encoded),
                Ok(StoredScalar::Primitive(logical))
            );
            let generic = crate::records::decode_single_field_value(
                &encoded,
                &ValueType::Enum(Box::new(
                    stored_scalar_schema(LargeValueKind::Bytes).clone(),
                )),
            )
            .unwrap();
            assert!(matches!(generic, Value::Enum(ref value) if value.tag() == 0));
        }
        for (kind, primitive) in [
            (
                LargeValueKind::String,
                StoredScalar::Primitive("text".into()),
            ),
            (
                LargeValueKind::Json,
                StoredScalar::Primitive(br#"{"key":1}"#.to_vec()),
            ),
        ] {
            let encoded = encode_stored_scalar(kind, &primitive).unwrap();
            assert_eq!(decode_stored_scalar(kind, &encoded), Ok(primitive));
        }
        assert_eq!(
            decode_stored_scalar(LargeValueKind::Bytes, &[]),
            Err(Error::MalformedScalar)
        );
        assert_eq!(
            decode_stored_scalar(LargeValueKind::Bytes, &[4, 0]),
            Err(Error::MalformedScalar)
        );
        // Alice's preceding storage format used tags 2 and 3. Neither is
        // an admitted scalar in the compact two-case format.
        for retired_tag in [2, 3] {
            assert_eq!(
                decode_stored_scalar(LargeValueKind::Bytes, &[retired_tag, 0]),
                Err(Error::MalformedScalar)
            );
            assert_eq!(
                inline_scalar_bytes(LargeValueKind::Bytes, &[retired_tag, 0]),
                Err(Error::MalformedScalar)
            );
        }
    }

    #[test]
    fn stored_scalar_chunked_round_trips_through_generic_enum_records_and_tail() {
        for (kind, logical) in [
            (LargeValueKind::Bytes, b"bytes root".as_slice()),
            (LargeValueKind::String, "text root 🙂".as_bytes()),
            (LargeValueKind::Json, br#"{"title":"json root"}"#.as_slice()),
        ] {
            let prepared = prepare(kind, logical).unwrap();
            let mut value = prepared.value_ref;
            value.edit_tail.push(ReplaceEdit {
                offset: value.byte_length,
                delete_length: 0,
                insert_bytes: match kind {
                    LargeValueKind::Bytes => vec![0, 0xff],
                    LargeValueKind::String => "!".as_bytes().to_vec(),
                    // This receipt proves ordinary list/record tail encoding;
                    // JSON edits are validated by the higher replacement path.
                    LargeValueKind::Json => Vec::new(),
                },
                utf16_offset: value.utf16_length.unwrap_or(0),
                delete_utf16_length: 0,
                insert_utf16_length: 0,
            });
            if kind == LargeValueKind::Bytes {
                value.byte_length += 2;
            }
            // Keep the text and JSON receipt descriptor shape valid while
            // retaining a real tail record for text.
            if kind == LargeValueKind::String {
                value.byte_length += 1;
                value.utf16_length = value.utf16_length.map(|length| length + 1);
                value.edit_tail[0].insert_utf16_length = 1;
            }
            let encoded =
                encode_stored_scalar(kind, &StoredScalar::Chunked(value.clone())).unwrap();
            let decoded = decode_stored_scalar(kind, &encoded).unwrap();
            assert_eq!(decoded, StoredScalar::Chunked(value));
            let generic = crate::records::decode_single_field_value(
                &encoded,
                &ValueType::Enum(Box::new(stored_scalar_schema(kind).clone())),
            )
            .unwrap();
            assert!(matches!(generic, Value::Enum(ref value) if value.tag() == 1));
        }
    }

    #[test]
    fn primitive_payload_is_interpreted_by_its_declared_schema_kind() {
        // Inline values need no duplicated kind witness: the same canonical
        // UTF-8/JSON payload is valid under either parameterized schema.
        let logical = br#"{"valid":"json and utf8"}"#.to_vec();
        let encoded = encode_stored_scalar(
            LargeValueKind::Bytes,
            &StoredScalar::Primitive(logical.clone()),
        )
        .unwrap();

        for replay_kind in [LargeValueKind::String, LargeValueKind::Json] {
            assert_eq!(
                decode_stored_scalar(replay_kind, &encoded),
                Ok(StoredScalar::Primitive(logical.clone())),
                "the schema must supply {replay_kind:?} semantics"
            );
            assert_eq!(
                inline_scalar_bytes(replay_kind, &encoded),
                Ok(logical.as_slice()),
                "inline fast path must use the {replay_kind:?} schema"
            );
        }
    }

    #[test]
    fn single_leaf_kind_witness_rejects_descriptor_replay_with_valid_metrics() {
        // This must stay an internal format test: a hostile persisted descriptor
        // is rejected while resolving its authenticated physical root, before a
        // public query can observe a relabeled logical value.
        let logical = br#"{"valid":"json and utf8"}"#;
        let prepared = prepare(LargeValueKind::Bytes, logical).unwrap();
        assert_eq!(prepared.staged_chunks.len(), 1);
        let root = &prepared.staged_chunks[0];

        for replay_kind in [LargeValueKind::String, LargeValueKind::Json] {
            let mut forged = prepared.value_ref.clone();
            forged.kind = replay_kind;
            forged.utf16_length = Some(logical.len() as u64);
            // Replay the exact source root identity. If decode_node stopped
            // authenticating the embedded kind, every later identity and
            // metric check would still pass and expose these bytes as the
            // target semantic kind.
            forged.logical_hash = prepared.value_ref.logical_hash;
            let encoded =
                encode_stored_scalar(replay_kind, &StoredScalar::Chunked(forged.clone())).unwrap();
            assert_eq!(
                decode_stored_scalar(replay_kind, &encoded),
                Ok(StoredScalar::Chunked(forged.clone()))
            );

            let mut inputs = ChunkInputs::default();
            inputs.install_chunk(
                ChunkRequest {
                    object_hash: root.node_ref.object_hash.0,
                },
                bytes::Bytes::copy_from_slice(&root.encoded),
            );
            assert!(matches!(
                materialize_attempt(&forged, &mut inputs),
                Err(AttemptError::LargeValue(Error::DescriptorMismatch))
            ));
        }
    }

    #[test]
    fn every_node_kind_witness_rejects_multi_leaf_replay() {
        // Forge a target-kind branch root with correct target metrics so the
        // traversal reaches an original bytes child. The child witness, not
        // merely the descriptor/root witness, must stop the replay.
        let logical = format!(r#"{{"body":"{}"}}"#, "x".repeat(LEAF_MAX_BYTES * 3));
        let prepared = prepare(LargeValueKind::Bytes, logical.as_bytes()).unwrap();
        let original_root = prepared
            .staged_chunks
            .iter()
            .find(|chunk| chunk.node_ref == prepared.value_ref.root)
            .unwrap();
        let ChunkNode::Branch { mut children, .. } = decode_node(
            LargeValueKind::Bytes,
            original_root.node_ref.object_hash,
            &original_root.encoded,
        )
        .unwrap() else {
            panic!("fixture must produce a multi-leaf branch root");
        };
        assert!(children.len() > 1);
        for child in &mut children {
            child.metrics.utf16_length = Some(child.metrics.byte_length);
        }
        let forged_root = ChunkNode::Branch {
            format: FORMAT_VERSION,
            kind: LargeValueKind::Json,
            children,
        };
        let forged_root_encoded = encode_node(&forged_root).unwrap();
        let forged_root_ref = NodeRef {
            object_hash: object_hash(&forged_root_encoded),
        };
        let mut forged = prepared.value_ref.clone();
        forged.kind = LargeValueKind::Json;
        forged.root = forged_root_ref.clone();
        forged.logical_hash = node_logical_hash(&forged_root);
        forged.utf16_length = Some(logical.encode_utf16().count() as u64);

        let mut inputs = ChunkInputs::default();
        let mut supplied_original_child = false;
        loop {
            match materialize_attempt(&forged, &mut inputs) {
                Err(AttemptError::Blocked) => {
                    for request in inputs.take_missing_chunks() {
                        if request.object_hash == forged_root_ref.object_hash.0 {
                            inputs.install_chunk(
                                request,
                                bytes::Bytes::copy_from_slice(&forged_root_encoded),
                            );
                            continue;
                        }
                        let chunk = prepared
                            .staged_chunks
                            .iter()
                            .find(|chunk| chunk.node_ref.object_hash.0 == request.object_hash)
                            .expect("forged root may reveal only original children");
                        supplied_original_child = true;
                        inputs
                            .install_chunk(request, bytes::Bytes::copy_from_slice(&chunk.encoded));
                    }
                }
                Err(AttemptError::LargeValue(Error::DescriptorMismatch)) => break,
                result => panic!("unexpected replay result: {result:?}"),
            }
        }
        assert!(
            supplied_original_child,
            "the forged branch must pass before a child witness rejects replay"
        );
    }

    #[test]
    fn candidate_future_format_two_nodes_fail_closed() {
        // This local type represents a hypothetical alternate format-2 leaf.
        // Keeping the receipt independent of the current enum ensures a future
        // serde layout cannot accidentally turn new content into a v1 node.
        #[derive(Serialize)]
        enum CandidateFutureFormatTwoNode {
            Leaf { format: u8, bytes: Vec<u8> },
        }

        for bytes in [
            Vec::new(),
            b"plain utf8".to_vec(),
            br#"{"valid":"json"}"#.to_vec(),
            vec![0, 1, 2, 3],
        ] {
            let encoded =
                postcard::to_allocvec(&CandidateFutureFormatTwoNode::Leaf { format: 2, bytes })
                    .unwrap();
            let hash = object_hash(&encoded);
            for expected_kind in [
                LargeValueKind::Bytes,
                LargeValueKind::String,
                LargeValueKind::Json,
            ] {
                assert!(
                    decode_node(expected_kind, hash, &encoded).is_err(),
                    "future format-2 leaf must fail closed as {expected_kind:?}"
                );
            }
        }
    }

    #[test]
    fn same_kind_prepare_reuse_and_append_consolidation_remain_deterministic() {
        // Internal construction coverage is necessary because deterministic
        // object/logical identity and node reuse are representation
        // invariants intentionally hidden from public queries.
        let logical = (0..LEAF_MAX_BYTES * 5)
            .map(|index| (index.wrapping_mul(37) & 0xff) as u8)
            .collect::<Vec<_>>();
        let first = prepare(LargeValueKind::Bytes, &logical).unwrap();
        let second = prepare(LargeValueKind::Bytes, &logical).unwrap();
        assert_eq!(first, second);

        let append = b"deterministic append".to_vec();
        let TailAppendOutcome::Updated(with_tail) =
            append_tail(&first.value_ref, append.clone()).unwrap()
        else {
            panic!("one small append must remain in the bounded tail");
        };
        let available = first
            .staged_chunks
            .iter()
            .map(|chunk| {
                (
                    ChunkRequest {
                        object_hash: chunk.node_ref.object_hash.0,
                    },
                    bytes::Bytes::copy_from_slice(&chunk.encoded),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let consolidate = || {
            let mut inputs = ChunkInputs::default();
            loop {
                match consolidate_appends_attempt(&with_tail, &mut inputs) {
                    Ok(prepared) => break prepared,
                    Err(AttemptError::Blocked) => {
                        for request in inputs.take_missing_chunks() {
                            inputs.install_chunk(request.clone(), available[&request].clone());
                        }
                    }
                    Err(error) => panic!("unexpected consolidation failure: {error}"),
                }
            }
        };
        let consolidated = consolidate();
        assert_eq!(consolidated, consolidate());
        let mut expected = logical;
        expected.extend_from_slice(&append);
        let fresh = prepare(LargeValueKind::Bytes, &expected).unwrap();
        assert_eq!(consolidated.value_ref, fresh.value_ref);

        let shared = br#"{"same":"valid bytes, text, and json"}"#;
        let cross_kind_hashes = [
            LargeValueKind::Bytes,
            LargeValueKind::String,
            LargeValueKind::Json,
        ]
        .map(|kind| prepare(kind, shared).unwrap().value_ref.logical_hash);
        assert_ne!(cross_kind_hashes[0], cross_kind_hashes[1]);
        assert_ne!(cross_kind_hashes[0], cross_kind_hashes[2]);
        assert_ne!(cross_kind_hashes[1], cross_kind_hashes[2]);

        let current_node = ChunkNode::Leaf {
            format: FORMAT_VERSION,
            kind: LargeValueKind::Bytes,
            bytes: shared.to_vec(),
        };
        let candidate_future_node = ChunkNode::Leaf {
            format: 2,
            kind: LargeValueKind::Bytes,
            bytes: shared.to_vec(),
        };
        assert_ne!(
            node_logical_hash(&current_node),
            node_logical_hash(&candidate_future_node),
            "node format must participate in locator-independent identity"
        );
    }

    #[test]
    fn schema_derived_stored_scalar_kind_rejects_a_mismatched_descriptor() {
        let json = prepare(LargeValueKind::Json, br#"{"same":"bytes"}"#)
            .unwrap()
            .value_ref;
        assert!(
            encode_stored_scalar(LargeValueKind::String, &StoredScalar::Chunked(json)).is_err()
        );
    }

    #[test]
    fn small_insertion_preserves_most_content_defined_nodes() {
        let mut state = 0x9e37_79b9_u64;
        let original = (0..(LEAF_TARGET_BYTES * 128))
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect::<Vec<_>>();
        let first = prepare(LargeValueKind::Bytes, &original).unwrap();
        let mut edited = original;
        edited.splice(
            edited.len() / 2..edited.len() / 2,
            b"localized edit".iter().copied(),
        );
        let second = prepare(LargeValueKind::Bytes, &edited).unwrap();
        let old_hashes = first
            .staged_chunks
            .iter()
            .map(|chunk| chunk.node_ref.object_hash)
            .collect::<std::collections::BTreeSet<_>>();
        let reused = second
            .staged_chunks
            .iter()
            .filter(|chunk| old_hashes.contains(&chunk.node_ref.object_hash))
            .count();
        let rewritten = second.staged_chunks.len() - reused;
        assert!(
            rewritten < second.staged_chunks.len() / 3,
            "a local insertion rewrote {rewritten} of {} nodes",
            second.staged_chunks.len()
        );
    }

    #[test]
    fn narrow_ranges_map_back_through_edit_tail_without_full_hydration() {
        let mut state = 0x1234_5678_9abc_def0_u64;
        let base = (0..(LEAF_TARGET_BYTES * 48))
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect::<Vec<_>>();
        let prepared = prepare(LargeValueKind::Bytes, &base).unwrap();
        let available = prepared
            .staged_chunks
            .iter()
            .map(|chunk| {
                (
                    ChunkRequest {
                        object_hash: chunk.node_ref.object_hash.0,
                    },
                    bytes::Bytes::copy_from_slice(&chunk.encoded),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let edits = vec![
            ReplaceEdit {
                offset: 100,
                delete_length: 3,
                insert_bytes: b"early insertion".to_vec(),
                utf16_offset: 0,
                delete_utf16_length: 0,
                insert_utf16_length: 0,
            },
            ReplaceEdit {
                offset: (LEAF_TARGET_BYTES * 24) as u64,
                delete_length: 19,
                insert_bytes: b"middle replacement".to_vec(),
                utf16_offset: 0,
                delete_utf16_length: 0,
                insert_utf16_length: 0,
            },
        ];
        let mut logical = base;
        apply_edits(&mut logical, &edits).unwrap();
        let mut value = prepared.value_ref;
        value.edit_tail = edits;
        value.byte_length = logical.len() as u64;
        let requested = (LEAF_TARGET_BYTES * 24 - 20)..(LEAF_TARGET_BYTES * 24 + 80);
        let mut inputs = ChunkInputs::default();
        let mut requested_chunks = std::collections::BTreeSet::new();
        loop {
            match byte_range_attempt(
                &value,
                requested.start as u64..requested.end as u64,
                &mut inputs,
            ) {
                Ok(bytes) => {
                    assert_eq!(bytes, logical[requested.clone()]);
                    break;
                }
                Err(AttemptError::Blocked) => {
                    let requests = inputs.take_missing_chunks();
                    assert!(!requests.is_empty());
                    for request in requests {
                        requested_chunks.insert(request.clone());
                        inputs.install_chunk(request.clone(), available[&request].clone());
                    }
                }
                Err(error) => panic!("unexpected range failure: {error}"),
            }
        }
        assert!(
            requested_chunks.len() * 4 < available.len(),
            "narrow range fetched {} of {} chunks",
            requested_chunks.len(),
            available.len()
        );
    }

    #[test]
    fn reverse_edit_range_mapping_matches_materialized_oracle() {
        let base = (0..512)
            .map(|index| (index & 0xff) as u8)
            .collect::<Vec<_>>();
        let mut state = 0xfeed_face_cafe_beef_u64;
        for _case in 0..200 {
            let mut logical = base.clone();
            let mut edits = Vec::new();
            for _ in 0..8 {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                let offset = (state as usize) % (logical.len() + 1);
                let available = logical.len() - offset;
                let delete = ((state >> 11) as usize % 12).min(available);
                let insert_len = (state >> 23) as usize % 12;
                let insert = (0..insert_len)
                    .map(|index| state.wrapping_add(index as u64) as u8)
                    .collect::<Vec<_>>();
                let edit = ReplaceEdit {
                    offset: offset as u64,
                    delete_length: delete as u64,
                    insert_bytes: insert,
                    utf16_offset: 0,
                    delete_utf16_length: 0,
                    insert_utf16_length: 0,
                };
                apply_edits(&mut logical, std::slice::from_ref(&edit)).unwrap();
                edits.push(edit);
            }
            for _ in 0..8 {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                let left = state as usize % (logical.len() + 1);
                let right = (state >> 19) as usize % (logical.len() + 1);
                let range = left.min(right)..left.max(right);
                let pieces = map_final_range_to_base(
                    range.start as u64..range.end as u64,
                    &edits,
                    RangeUnit::Bytes,
                )
                .unwrap();
                let mapped = pieces
                    .into_iter()
                    .flat_map(|piece| match piece {
                        RangePiece::Base(range) => {
                            base[range.start as usize..range.end as usize].to_vec()
                        }
                        RangePiece::Inserted(bytes) => bytes,
                    })
                    .collect::<Vec<_>>();
                assert_eq!(mapped, logical[range]);
            }
        }
    }
}
