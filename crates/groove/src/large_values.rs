//! Canonical indirect representation for large logical scalar values.

#[cfg(test)]
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

mod json_syntax;
use json_syntax::StreamingJsonValidator;
use thiserror::Error;

use crate::chunks::ChunkRequest;
use crate::ivm::runtime::IvmRuntimeError;
use crate::ivm::runtime::evaluation_session::EvaluationInputs;
use crate::records::{EnumCase, EnumSchema, EnumValue, RecordDescriptor, Value, ValueType};

const FORMAT_V2: u8 = 2;
pub const FORMAT_VERSION: u8 = FORMAT_V2;

/// The authoritative immutable-large-value codecs that this binary can read.
///
/// The descriptor selects one of these codecs before any descriptor-guided
/// traversal starts.  Keep an explicit case per persisted format: accepting a
/// later format through the v2 codec (or vice versa) would turn a format bump
/// into an accidental, lossy migration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LargeValueFormat {
    V2,
}

impl LargeValueFormat {
    fn from_version(version: u8) -> Result<Self, Error> {
        match version {
            FORMAT_V2 => Ok(Self::V2),
            _ => Err(Error::UnsupportedFormat(version)),
        }
    }

    fn version(self) -> u8 {
        match self {
            Self::V2 => FORMAT_V2,
        }
    }

    fn encode_node(self, node: &ChunkNode) -> Result<Vec<u8>, Error> {
        match self {
            Self::V2 => encode_node_v2(node),
        }
    }

    fn decode_node(self, encoded: &[u8]) -> Result<ChunkNode, Error> {
        let node = match self {
            Self::V2 => decode_canonical_node_v2(encoded)?,
        };
        if node_format(&node) != self.version() {
            return Err(Error::UnsupportedFormat(node_format(&node)));
        }
        Ok(node)
    }
}
/// Logical scalar size above which ordinary writes use indirect storage.
pub const INLINE_VALUE_MAX_BYTES: usize = 64 * 1024;
pub const LEAF_MIN_BYTES: usize = 16 * 1024;
pub const LEAF_TARGET_BYTES: usize = 64 * 1024;
pub const LEAF_MAX_BYTES: usize = 256 * 1024;
/// Hard allocation/CPU boundary for one encoded immutable node. Leaves dominate
/// the format; the additional envelope allowance is comfortably larger than a
/// maximum-fanout branch with ordinary locators.
pub const MAX_ENCODED_NODE_BYTES: usize = LEAF_MAX_BYTES + 16 * 1024;
pub const BRANCH_MIN_CHILDREN: usize = 4;
pub const BRANCH_TARGET_CHILDREN: usize = 16;
pub const BRANCH_MAX_CHILDREN: usize = 64;
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
pub const MAX_JSON_NESTING_DEPTH: usize = 128;
pub const MAX_EDIT_COUNT: usize = 64;
pub const MAX_EDIT_TAIL_BYTES: usize = 256 * 1024;
/// A randomly allocated 256-bit retrieval capability for one immutable chunk.
///
/// This is deliberately not a storage key: each storage adapter derives its
/// private key layout from the capability internally.
pub const LOCATOR_BYTES: usize = 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ContentHash(pub [u8; 32]);

/// An opaque retrieval capability allocated by Groove.
///
/// ```compile_fail
/// use groove::large_values::Locator;
/// let forged = Locator([0_u8; 32]);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Locator([u8; LOCATOR_BYTES]);

impl Locator {
    /// Allocate a fresh random 256-bit retrieval capability.
    pub fn random() -> Self {
        let mut locator = [0_u8; LOCATOR_BYTES];
        getrandom::fill(&mut locator).expect("OS CSPRNG unavailable for chunk capability");
        Self(locator)
    }

    pub fn as_bytes(&self) -> &[u8; LOCATOR_BYTES] {
        &self.0
    }

    /// Deterministically derive a capability for crate-internal fixtures.
    #[cfg(test)]
    pub(crate) fn from_seed(seed: &[u8]) -> Self {
        Self(*blake3::hash(seed).as_bytes())
    }
}

impl<'de> Deserialize<'de> for Locator {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes = Vec::<u8>::deserialize(deserializer)?;
        let length = bytes.len();
        let locator: [u8; LOCATOR_BYTES] = bytes.try_into().map_err(|_| {
            <D::Error as serde::de::Error>::custom(format!(
                "chunk locator must be exactly {LOCATOR_BYTES} bytes, got {length}"
            ))
        })?;
        Ok(Self(locator))
    }
}

impl Serialize for Locator {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.to_vec().serialize(serializer)
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
    pub locator: Locator,
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
    /// Deterministic subtree identity excluding retrieval locators.
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

/// A read-only preparation returned by Groove's graph builder.
///
/// ```compile_fail
/// use groove::large_values::{LargeValueRef, PreparedLargeValue, StagedChunk};
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

/// Opaque identity for a persisted Groove staging root.
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
/// receipt. Chunk identities stay Groove-owned and are used only for safe
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
    pub(crate) fn consume_window(&mut self, bytes: &[u8]) -> Result<bool, Error> {
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

    pub(crate) fn record_yield(&mut self) -> Result<(), Error> {
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

    pub(crate) fn next_range(&self) -> Option<std::ops::Range<u64>> {
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

    pub(crate) fn advance_to(&mut self, offset: u64) {
        self.offset = offset;
    }
}

/// Incremental write-admission validation for an already-chunked logical
/// value. The caller feeds consecutive final-logical windows, so this retains
/// only an incomplete UTF-8 sequence and JSON syntax state, never the value.
pub(crate) struct LogicalValueValidator {
    kind: LargeValueKind,
    utf8_tail: Vec<u8>,
    utf16_length: u64,
    json: Option<StreamingJsonValidator>,
}

/// Validate every supplied immutable node before an upload batch reaches a
/// durable chunk backend. Upload batches are protocol-bounded, so retaining
/// these small identity sets does not change the streaming memory bound.
pub(crate) fn validate_staged_chunk_batch(
    kind: LargeValueKind,
    chunks: &[StagedChunk],
) -> Result<(), Error> {
    let mut locator_hashes = std::collections::BTreeMap::new();
    for chunk in chunks {
        if let Some(existing) =
            locator_hashes.insert(chunk.node_ref.locator, chunk.node_ref.object_hash)
            && existing != chunk.node_ref.object_hash
        {
            return Err(Error::ObjectHashMismatch);
        }
        // `decode_node` also checks the encoded-size and object-hash bounds.
        // Validate the full semantic envelope here, rather than discovering a
        // malformed later member after an earlier one is already durable.
        decode_node(kind, chunk.node_ref.object_hash, &chunk.encoded)?;
    }
    Ok(())
}

impl LogicalValueValidator {
    pub(crate) fn new(value: &LargeValueRef) -> Result<Self, Error> {
        validate_descriptor(value)?;
        Ok(Self {
            kind: value.kind,
            utf8_tail: Vec::new(),
            utf16_length: 0,
            json: (value.kind == LargeValueKind::Json).then(StreamingJsonValidator::new),
        })
    }

    pub(crate) fn push(&mut self, bytes: &[u8]) -> Result<(), Error> {
        if self.kind == LargeValueKind::Bytes {
            return Ok(());
        }

        self.utf8_tail.extend_from_slice(bytes);
        match std::str::from_utf8(&self.utf8_tail) {
            Ok(text) => {
                self.utf16_length = self
                    .utf16_length
                    .checked_add(
                        u64::try_from(text.encode_utf16().count())
                            .map_err(|_| Error::MetricOverflow)?,
                    )
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
                    .checked_add(
                        u64::try_from(valid_text.encode_utf16().count())
                            .map_err(|_| Error::MetricOverflow)?,
                    )
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

    pub(crate) fn finish(self, value: &LargeValueRef) -> Result<(), Error> {
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
    reader: R,
    stage: impl FnMut(StagedChunk) -> Result<(), Error>,
) -> Result<(LargeValueRef, StreamingPrepareStats), Error> {
    prepare_streaming_with_locator(kind, reader, random_locator, stage)
}

fn prepare_streaming_with_locator<R: std::io::Read>(
    kind: LargeValueKind,
    mut reader: R,
    locator_for: impl FnMut(ContentHash) -> Locator,
    stage: impl FnMut(StagedChunk) -> Result<(), Error>,
) -> Result<(LargeValueRef, StreamingPrepareStats), Error> {
    let mut builder = PushStreamingPreparation::new_with_locator(kind, locator_for, stage);
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
pub struct PushStreamingPreparation<L, S>
where
    L: FnMut(ContentHash) -> Locator,
    S: FnMut(StagedChunk) -> Result<(), Error>,
{
    builder: StreamingTreeBuilder<L, S>,
    json: Option<StreamingJsonValidator>,
}

impl<L, S> PushStreamingPreparation<L, S>
where
    L: FnMut(ContentHash) -> Locator,
    S: FnMut(StagedChunk) -> Result<(), Error>,
{
    fn new_with_locator(kind: LargeValueKind, locator_for: L, stage: S) -> Self {
        Self {
            builder: StreamingTreeBuilder::new(kind, locator_for, stage),
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

fn random_locator(_: ContentHash) -> Locator {
    Locator::random()
}

impl<S> PushStreamingPreparation<fn(ContentHash) -> Locator, S>
where
    S: FnMut(StagedChunk) -> Result<(), Error>,
{
    pub fn new(kind: LargeValueKind, stage: S) -> Self {
        Self::new_with_locator(kind, random_locator, stage)
    }
}

struct StreamingLevel {
    pending: Vec<BuiltNode>,
    hash: u64,
    total: usize,
}

struct StreamingTreeBuilder<L, S> {
    kind: LargeValueKind,
    locator_for: L,
    stage: S,
    leaf: Vec<u8>,
    leaf_scan: usize,
    leaf_hash: u64,
    levels: Vec<StreamingLevel>,
    stats: StreamingPrepareStats,
}

impl<L, S> StreamingTreeBuilder<L, S>
where
    L: FnMut(ContentHash) -> Locator,
    S: FnMut(StagedChunk) -> Result<(), Error>,
{
    fn new(kind: LargeValueKind, locator_for: L, stage: S) -> Self {
        Self {
            kind,
            locator_for,
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
                self.leaf_hash = self
                    .leaf_hash
                    .wrapping_shl(1)
                    .wrapping_add(gear(self.leaf[self.leaf_scan]));
                self.leaf_scan += 1;
                let length = self.leaf_scan;
                let cut = length >= LEAF_MIN_BYTES
                    && if length < LEAF_TARGET_BYTES {
                        self.leaf_hash & (LEAF_TARGET_BYTES as u64 * 2 - 1) == 0
                    } else {
                        self.leaf_hash & (LEAF_TARGET_BYTES as u64 / 2 - 1) == 0
                    };
                if cut || length == LEAF_MAX_BYTES {
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
        let chunk_node = ChunkNode::Leaf {
            format: FORMAT_VERSION,
            kind: self.kind,
            bytes,
        };
        let node = self.stage_node_to(chunk_node, metrics)?;
        self.add_node(0, node)
    }

    fn stage_node_to(&mut self, node: ChunkNode, metrics: NodeMetrics) -> Result<BuiltNode, Error> {
        if node_kind(&node) != self.kind {
            return Err(Error::DescriptorMismatch);
        }
        if node_metrics(self.kind, &node)? != metrics {
            return Err(Error::DescriptorMismatch);
        }
        let structural_hash = node_logical_hash(&node);
        let encoded = encode_node(&node)?;
        let object_hash = object_hash(&encoded);
        let node_ref = NodeRef {
            object_hash,
            locator: (self.locator_for)(object_hash),
        };
        (self.stage)(StagedChunk {
            node_ref: node_ref.clone(),
            encoded,
        })?;
        self.stats.staged_chunk_count = self
            .stats
            .staged_chunk_count
            .checked_add(1)
            .ok_or(Error::MetricOverflow)?;
        Ok(BuiltNode {
            node_ref,
            metrics,
            structural_hash,
        })
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
                for byte in
                    grouping_hash_from_logical(FORMAT_VERSION, self.kind, node.structural_hash).0
                {
                    state.hash = state.hash.wrapping_shl(1).wrapping_add(gear(byte));
                }
                state.pending.push(node);
                state.total += 1;
                let count = state.pending.len();
                let boundary = count >= BRANCH_MIN_CHILDREN
                    && if count < BRANCH_TARGET_CHILDREN {
                        state.hash & (BRANCH_TARGET_CHILDREN as u64 * 2 - 1) == 0
                    } else {
                        state.hash & (BRANCH_TARGET_CHILDREN as u64 / 2 - 1) == 0
                    };
                (count, boundary)
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
        let mut group_metrics = group.iter().map(|child| child.metrics);
        let first = group_metrics.next().ok_or(Error::MalformedNode)?;
        let metrics = group_metrics.try_fold(first, add_metrics)?;
        let children = group
            .iter()
            .map(|child| BranchChild {
                node_ref: child.node_ref.clone(),
                metrics: child.metrics,
                logical_hash: child.structural_hash,
            })
            .collect();
        let chunk_node = ChunkNode::Branch {
            format: FORMAT_VERSION,
            kind: self.kind,
            children,
        };
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
                return Ok((
                    LargeValueRef {
                        kind: self.kind,
                        format_version: FORMAT_VERSION,
                        logical_hash: root.structural_hash,
                        root: root.node_ref,
                        byte_length: root.metrics.byte_length,
                        utf16_length: root.metrics.utf16_length,
                        edit_tail: Vec::new(),
                    },
                    self.stats,
                ));
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
pub(crate) struct ConsolidationContinuation {
    source: LargeValueRef,
    current: Option<LargeValueRef>,
    next_edit: usize,
    staged_chunks: Vec<StagedChunk>,
}

impl ConsolidationContinuation {
    pub(crate) fn new(source: LargeValueRef) -> Result<Self, Error> {
        validate_descriptor_shape(&source)?;
        Ok(Self {
            source,
            current: None,
            next_edit: 0,
            staged_chunks: Vec::new(),
        })
    }

    pub(crate) fn step(
        &mut self,
        inputs: &mut EvaluationInputs,
    ) -> Result<Option<PreparedLargeValue>, IvmRuntimeError> {
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
            let prepared =
                consolidate_single_edit_attempt(&with_tail, inputs, &mut random_locator)?;
            for chunk in &prepared.staged_chunks {
                inputs.install_chunk(
                    ChunkRequest {
                        object_hash: chunk.node_ref.object_hash.0,
                        locator: chunk.node_ref.locator,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TailAppendOutcome {
    Updated(LargeValueRef),
    ConsolidationRequired(LargeValueRef),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TailEditOutcome {
    Updated(LargeValueRef),
    ConsolidationRequired(LargeValueRef),
}

/// Physical arm stored inside a logical bytes/string/JSON cell.
///
/// This is deliberately an engine-owned *normal Groove enum*: `Primitive`
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
    let edit = ReplaceEdit {
        offset: value.byte_length,
        delete_length: 0,
        insert_bytes,
        utf16_offset: value.utf16_length.unwrap_or(0),
        delete_utf16_length: 0,
        insert_utf16_length: 0,
    };
    let next_tail_bytes = value
        .edit_tail
        .iter()
        .try_fold(0_usize, |total, edit| {
            total
                .checked_add(edit.insert_bytes.len())
                .and_then(|total| total.checked_add(24))
                .ok_or(Error::MetricOverflow)
        })?
        .checked_add(edit.insert_bytes.len())
        .and_then(|total| total.checked_add(24))
        .ok_or(Error::MetricOverflow)?;
    let inserted_length =
        u64::try_from(edit.insert_bytes.len()).map_err(|_| Error::MetricOverflow)?;
    let mut updated = value.clone();
    updated.byte_length = updated
        .byte_length
        .checked_add(inserted_length)
        .ok_or(Error::MetricOverflow)?;
    if let Some(utf16_length) = &mut updated.utf16_length {
        let inserted_utf16 = u64::try_from(
            std::str::from_utf8(&edit.insert_bytes)
                .map_err(|_| Error::InvalidUtf8)?
                .encode_utf16()
                .count(),
        )
        .map_err(|_| Error::MetricOverflow)?;
        *utf16_length = utf16_length
            .checked_add(inserted_utf16)
            .ok_or(Error::MetricOverflow)?;
    }
    let mut edit = edit;
    edit.insert_utf16_length = updated
        .utf16_length
        .zip(value.utf16_length)
        .and_then(|(next, previous)| next.checked_sub(previous))
        .unwrap_or(0);
    updated.edit_tail.push(edit);
    if value.edit_tail.len() >= MAX_EDIT_COUNT || next_tail_bytes > MAX_EDIT_TAIL_BYTES {
        Ok(TailAppendOutcome::ConsolidationRequired(updated))
    } else {
        Ok(TailAppendOutcome::Updated(updated))
    }
}

/// Add one canonical replacement to the bounded tail. Bytes edits need no
/// source reads. Text edits request the deleted logical range so UTF-16
/// metrics remain exact; missing chunks suspend through the ordinary evaluator
/// request set. JSON currently admits only complete validated replacement.
pub(crate) fn replace_tail_attempt(
    value: &LargeValueRef,
    offset: u64,
    delete_length: u64,
    insert_bytes: Vec<u8>,
    inputs: &mut EvaluationInputs,
) -> Result<TailEditOutcome, IvmRuntimeError> {
    replace_tail_with_bounds_attempt(value, offset, delete_length, insert_bytes, inputs, true)
}

fn replace_tail_with_bounds_attempt(
    value: &LargeValueRef,
    offset: u64,
    delete_length: u64,
    insert_bytes: Vec<u8>,
    inputs: &mut EvaluationInputs,
    enforce_tail_bounds: bool,
) -> Result<TailEditOutcome, IvmRuntimeError> {
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
    let next_tail_bytes = value
        .edit_tail
        .iter()
        .try_fold(0_usize, |total, edit| {
            total
                .checked_add(edit.insert_bytes.len())
                .and_then(|total| total.checked_add(24))
                .ok_or(Error::MetricOverflow)
        })?
        .checked_add(insert_bytes.len())
        .and_then(|total| total.checked_add(24))
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
            let delete_utf16_length =
                u64::try_from(deleted.encode_utf16().count()).map_err(|_| Error::MetricOverflow)?;
            let insert_utf16_length = u64::try_from(inserted.encode_utf16().count())
                .map_err(|_| Error::MetricOverflow)?;
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
            let insert_utf16_length = u64::try_from(
                std::str::from_utf8(&insert_bytes)
                    .map_err(|_| Error::InvalidUtf8)?
                    .encode_utf16()
                    .count(),
            )
            .map_err(|_| Error::MetricOverflow)?;
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
    updated.edit_tail.push(ReplaceEdit {
        offset,
        delete_length,
        insert_bytes,
        utf16_offset,
        delete_utf16_length,
        insert_utf16_length,
    });
    if enforce_tail_bounds
        && (value.edit_tail.len() >= MAX_EDIT_COUNT || next_tail_bytes > MAX_EDIT_TAIL_BYTES)
    {
        Ok(TailEditOutcome::ConsolidationRequired(updated))
    } else {
        Ok(TailEditOutcome::Updated(updated))
    }
}

/// Consolidate an append-only tail by walking only the old tree's right
/// spine. At each level the final content-defined group is rebuilt; sibling
/// subtrees outside that group retain their exact authenticated `NodeRef`.
pub(crate) fn consolidate_appends_attempt(
    value: &LargeValueRef,
    inputs: &mut EvaluationInputs,
    mut fresh_locator: impl FnMut(ContentHash) -> Locator,
) -> Result<PreparedLargeValue, IvmRuntimeError> {
    validate_descriptor_shape(value)?;
    let base_byte_length = base_length(value.byte_length, &value.edit_tail)?;
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

    let mut existing = std::collections::BTreeMap::<ContentHash, Locator>::new();
    let mut spine = Vec::<Vec<BranchChild>>::new();
    let mut node_ref = value.root.clone();
    let mut expected_logical_hash = value.logical_hash;
    let mut depth = 0_usize;
    let last_leaf = loop {
        if depth > MAX_TREE_DEPTH {
            return Err(Error::InvalidTree.into());
        }
        existing.insert(node_ref.object_hash, node_ref.locator);
        let node = load_authenticated_node_attempt(
            value.format_version,
            value.kind,
            &node_ref,
            expected_logical_hash,
            inputs,
        )?;
        match node {
            ChunkNode::Leaf { bytes, .. } => break bytes,
            ChunkNode::Branch { children, .. } => {
                let last = children.last().ok_or(Error::MalformedNode)?;
                node_ref = last.node_ref.clone();
                expected_logical_hash = last.logical_hash;
                spine.push(children);
                depth += 1;
            }
        }
    };

    let mut logical_suffix = last_leaf;
    logical_suffix.extend_from_slice(&appended);
    let mut staged_chunks = Vec::new();
    let mut replacement = Vec::new();
    for range in leaf_ranges(value.kind, &logical_suffix)? {
        let bytes = logical_suffix[range].to_vec();
        let node = ChunkNode::Leaf {
            format: FORMAT_VERSION,
            kind: value.kind,
            bytes,
        };
        replacement.push(stage_node_reusing(
            value.kind,
            node,
            &existing,
            &mut fresh_locator,
            &mut staged_chunks,
        )?);
    }

    for children in spine.into_iter().rev() {
        let mut level = children;
        level.pop();
        let mut rebuilt = level
            .into_iter()
            .map(|child| BuiltNode {
                node_ref: child.node_ref,
                metrics: child.metrics,
                structural_hash: child.logical_hash,
            })
            .collect::<Vec<_>>();
        rebuilt.append(&mut replacement);
        replacement = Vec::new();
        for range in branch_ranges(value.kind, &rebuilt) {
            let children = rebuilt[range].to_vec();
            let node = ChunkNode::Branch {
                format: FORMAT_VERSION,
                kind: value.kind,
                children: children
                    .iter()
                    .map(|child| BranchChild {
                        node_ref: child.node_ref.clone(),
                        metrics: child.metrics,
                        logical_hash: child.structural_hash,
                    })
                    .collect(),
            };
            replacement.push(stage_node_reusing(
                value.kind,
                node,
                &existing,
                &mut fresh_locator,
                &mut staged_chunks,
            )?);
        }
    }

    let mut extra_depth = depth;
    while replacement.len() > 1 {
        extra_depth += 1;
        if extra_depth > MAX_TREE_DEPTH {
            return Err(Error::InvalidTree.into());
        }
        let mut next = Vec::new();
        for range in branch_ranges(value.kind, &replacement) {
            let children = replacement[range].to_vec();
            let node = ChunkNode::Branch {
                format: FORMAT_VERSION,
                kind: value.kind,
                children: children
                    .iter()
                    .map(|child| BranchChild {
                        node_ref: child.node_ref.clone(),
                        metrics: child.metrics,
                        logical_hash: child.structural_hash,
                    })
                    .collect(),
            };
            next.push(stage_node_reusing(
                value.kind,
                node,
                &existing,
                &mut fresh_locator,
                &mut staged_chunks,
            )?);
        }
        replacement = next;
    }
    let root = replacement.pop().ok_or(Error::MalformedNode)?;
    if root.metrics.byte_length != value.byte_length
        || root.metrics.utf16_length != value.utf16_length
    {
        return Err(Error::DescriptorMismatch.into());
    }
    Ok(PreparedLargeValue {
        value_ref: LargeValueRef {
            kind: value.kind,
            format_version: FORMAT_VERSION,
            logical_hash: root.structural_hash,
            root: root.node_ref,
            byte_length: root.metrics.byte_length,
            utf16_length: root.metrics.utf16_length,
            edit_tail: Vec::new(),
        },
        staged_chunks,
    })
}

/// Consolidate one replacement against an otherwise tail-free base. The
/// algorithm walks to the first affected leaf, advances until FastCDC
/// boundaries resynchronize, and rebuilds only the zipper between those two
/// leaves. This is the general middle-of-tree locality primitive; batching
/// several tail entries applies the same splice from right to left.
pub(crate) fn consolidate_single_edit_attempt(
    value: &LargeValueRef,
    inputs: &mut EvaluationInputs,
    mut fresh_locator: impl FnMut(ContentHash) -> Locator,
) -> Result<PreparedLargeValue, IvmRuntimeError> {
    validate_descriptor_shape(value)?;
    let [edit] = value.edit_tail.as_slice() else {
        return Err(Error::MalformedScalar.into());
    };
    let base_len = base_length(value.byte_length, &value.edit_tail)?;
    let edit_end = edit
        .offset
        .checked_add(edit.delete_length)
        .ok_or(Error::MetricOverflow)?;
    if edit_end > base_len {
        return Err(Error::MalformedScalar.into());
    }
    if edit.offset == base_len {
        return consolidate_appends_attempt(value, inputs, fresh_locator);
    }

    let first = locate_leaf_attempt(value, edit.offset, base_len, inputs)?;
    let mut leaves = vec![first];
    let mut covered_end = leaves[0]
        .start
        .checked_add(leaves[0].bytes.len() as u64)
        .ok_or(Error::MetricOverflow)?;
    while covered_end < edit_end {
        let next = next_leaf_attempt(
            value.format_version,
            value.kind,
            leaves.last().unwrap(),
            inputs,
        )?
        .ok_or(Error::DescriptorMismatch)?;
        covered_end = next
            .start
            .checked_add(next.bytes.len() as u64)
            .ok_or(Error::MetricOverflow)?;
        leaves.push(next);
    }

    loop {
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
        let resynchronized = ranges.last().is_some_and(|range| {
            range.start > 0 && segment[range.clone()] == leaves.last().unwrap().bytes
        });
        if resynchronized || covered_end == base_len {
            break;
        }
        let next = next_leaf_attempt(
            value.format_version,
            value.kind,
            leaves.last().unwrap(),
            inputs,
        )?
        .ok_or(Error::DescriptorMismatch)?;
        covered_end = next
            .start
            .checked_add(next.bytes.len() as u64)
            .ok_or(Error::MetricOverflow)?;
        leaves.push(next);
    }

    let left_path = leaves.first().unwrap().path.clone();
    let right_path = leaves.last().unwrap().path.clone();
    if left_path.len() != right_path.len() {
        return Err(Error::InvalidTree.into());
    }
    let mut existing = std::collections::BTreeMap::<ContentHash, Locator>::new();
    for leaf in &leaves {
        existing.insert(leaf.node_ref.object_hash, leaf.node_ref.locator);
        for frame in &leaf.path {
            existing.insert(frame.node_ref.object_hash, frame.node_ref.locator);
        }
    }
    let segment_start = leaves[0].start;
    let mut segment = leaves
        .iter()
        .flat_map(|leaf| leaf.bytes.iter().copied())
        .collect::<Vec<_>>();
    let local_start =
        usize::try_from(edit.offset - segment_start).map_err(|_| Error::MetricOverflow)?;
    let local_end = usize::try_from(edit_end - segment_start).map_err(|_| Error::MetricOverflow)?;
    segment.splice(local_start..local_end, edit.insert_bytes.iter().copied());

    if value.byte_length == 0 {
        if !segment.is_empty() {
            return Err(Error::DescriptorMismatch.into());
        }
        return Ok(prepare_with_locator(value.kind, &segment, fresh_locator)?);
    }

    let mut staged_chunks = Vec::new();
    let mut replacement = Vec::new();
    if !segment.is_empty() {
        for range in leaf_ranges(value.kind, &segment)? {
            replacement.push(stage_node_reusing(
                value.kind,
                ChunkNode::Leaf {
                    format: FORMAT_VERSION,
                    kind: value.kind,
                    bytes: segment[range].to_vec(),
                },
                &existing,
                &mut fresh_locator,
                &mut staged_chunks,
            )?);
        }
    }

    for depth in (0..left_path.len()).rev() {
        let left = &left_path[depth];
        let right = &right_path[depth];
        let mut level = left.children[..left.selected]
            .iter()
            .cloned()
            .map(built_from_child)
            .collect::<Vec<_>>();
        level.append(&mut replacement);
        level.extend(
            right.children[right.selected + 1..]
                .iter()
                .cloned()
                .map(built_from_child),
        );
        let higher_path_has_siblings = left_path[..depth]
            .iter()
            .zip(&right_path[..depth])
            .any(|(left, right)| left.selected > 0 || right.selected + 1 < right.children.len());
        replacement = if level.len() == 1 && !higher_path_has_siblings {
            level
        } else {
            stage_branch_level_reusing(
                value.kind,
                &level,
                &existing,
                &mut fresh_locator,
                &mut staged_chunks,
            )?
        };
    }
    let mut tree_depth = left_path.len();
    while replacement.len() > 1 {
        tree_depth += 1;
        if tree_depth > MAX_TREE_DEPTH {
            return Err(Error::InvalidTree.into());
        }
        replacement = stage_branch_level_reusing(
            value.kind,
            &replacement,
            &existing,
            &mut fresh_locator,
            &mut staged_chunks,
        )?;
    }
    let root = replacement.pop().ok_or(Error::MalformedNode)?;
    if root.metrics.byte_length != value.byte_length
        || root.metrics.utf16_length != value.utf16_length
    {
        return Err(Error::DescriptorMismatch.into());
    }
    Ok(PreparedLargeValue {
        value_ref: LargeValueRef {
            kind: value.kind,
            format_version: FORMAT_VERSION,
            logical_hash: root.structural_hash,
            root: root.node_ref,
            byte_length: root.metrics.byte_length,
            utf16_length: root.metrics.utf16_length,
            edit_tail: Vec::new(),
        },
        staged_chunks,
    })
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum Error {
    #[error("large-value upload batch limit must be non-zero")]
    InvalidUploadBatchLimit,
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
    Chunk(#[from] crate::chunks::ChunkError),
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
pub(crate) struct PhysicalTraversalNodeBudget {
    remaining: usize,
}

impl PhysicalTraversalNodeBudget {
    pub(crate) fn new() -> Self {
        Self::with_limit(MAX_PHYSICAL_TRAVERSAL_NODES)
    }

    fn with_limit(limit: usize) -> Self {
        Self { remaining: limit }
    }

    pub(crate) fn consume(&mut self) -> Result<(), Error> {
        self.remaining = self
            .remaining
            .checked_sub(1)
            .ok_or(Error::PhysicalTraversalNodeLimitExceeded)?;
        Ok(())
    }

    pub(crate) fn consume_many(&mut self, count: usize) -> Result<(), Error> {
        self.remaining = self
            .remaining
            .checked_sub(count)
            .ok_or(Error::PhysicalTraversalNodeLimitExceeded)?;
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct LogicalTraversalBudget {
    remaining: usize,
}

impl LogicalTraversalBudget {
    fn new() -> Self {
        Self {
            remaining: MAX_LOGICAL_TRAVERSAL_STEPS,
        }
    }

    fn consume(&mut self) -> Result<(), Error> {
        self.consume_many(1)
    }

    fn consume_many(&mut self, count: usize) -> Result<(), Error> {
        self.remaining = self
            .remaining
            .checked_sub(count)
            .ok_or(Error::TraversalWorkLimitExceeded)?;
        Ok(())
    }
}

/// Authenticate and visit every immutable node reachable from one descriptor.
///
/// The traversal is depth-first and retains only a bounded branch frontier;
/// Jazz can therefore feed each visited exact locator into its own mark set
/// without learning or reimplementing Groove's node encoding.
pub async fn visit_reachable_chunks(
    value: &LargeValueRef,
    provider: &dyn crate::chunks::ChunkProvider,
    mut visit: impl FnMut(&ChunkRequest),
) -> Result<u64, ReachabilityError> {
    check_format(value.format_version)?;
    let root_metrics = value.edit_tail.is_empty().then_some(NodeMetrics {
        byte_length: value.byte_length,
        utf16_length: value.utf16_length,
    });
    let mut traversal =
        PhysicalTraversal::new(value.root.clone(), root_metrics, value.logical_hash);
    let mut visited = 0_u64;
    while let Some(entry) = traversal.pop() {
        let node_ref = &entry.node_ref;
        let request = ChunkRequest {
            object_hash: node_ref.object_hash.0,
            locator: node_ref.locator,
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

/// Bounded-memory exporter for the push-before-row sync path.
pub struct LargeValueUploadCursor {
    format_version: u8,
    kind: LargeValueKind,
    provider: crate::chunks::OwnedChunkProvider,
    traversal: PhysicalTraversal,
}

impl LargeValueUploadCursor {
    pub(crate) fn new(
        value: &LargeValueRef,
        provider: crate::chunks::OwnedChunkProvider,
    ) -> Result<Self, Error> {
        check_format(value.format_version)?;
        let root_metrics = value.edit_tail.is_empty().then_some(NodeMetrics {
            byte_length: value.byte_length,
            utf16_length: value.utf16_length,
        });
        Ok(Self {
            format_version: value.format_version,
            kind: value.kind,
            provider,
            traversal: PhysicalTraversal::new(value.root.clone(), root_metrics, value.logical_hash),
        })
    }

    /// Read and authenticate at most `limit` nodes. `limit` must be non-zero.
    /// An empty result means the graph is complete; the cursor retains the
    /// branch frontier plus one authenticated expectation per distinct physical
    /// node.
    pub async fn next_batch(
        &mut self,
        limit: usize,
    ) -> Result<Vec<StagedChunk>, ReachabilityError> {
        if limit == 0 {
            return Err(Error::InvalidUploadBatchLimit.into());
        }
        let mut batch = Vec::new();
        while batch.len() < limit {
            let Some(entry) = self.traversal.pop() else {
                break;
            };
            let node_ref = &entry.node_ref;
            let request = ChunkRequest {
                object_hash: node_ref.object_hash.0,
                locator: node_ref.locator,
            };
            let encoded = self.provider.get(request).await?;
            let node = decode_node_for_format(
                self.format_version,
                self.kind,
                node_ref.object_hash,
                encoded.bytes(),
            )?;
            self.traversal.validate_node(self.kind, node_ref, &node)?;
            if let ChunkNode::Branch { children, .. } = node {
                self.traversal.discover_children(&entry, children)?;
            }
            batch.push(StagedChunk {
                node_ref: entry.node_ref,
                encoded: encoded.bytes().to_vec(),
            });
        }
        if self.traversal.pending.is_empty() {
            self.traversal.finish()?;
        }
        Ok(batch)
    }
}

/// Authenticate the locally present prefix of a descriptor's tree and return
/// its current missing frontier. Children are discovered only from verified
/// branch nodes, so an empty result is a proof of graph closure.
pub(crate) async fn missing_upload_frontier(
    value: &LargeValueRef,
    reader: crate::chunks::LocalChunkReader,
    limit: usize,
) -> Result<Vec<NodeRef>, ReachabilityError> {
    check_format(value.format_version)?;
    let root_metrics = value.edit_tail.is_empty().then_some(NodeMetrics {
        byte_length: value.byte_length,
        utf16_length: value.utf16_length,
    });
    let mut traversal =
        PhysicalTraversal::new(value.root.clone(), root_metrics, value.logical_hash);
    let mut missing = Vec::new();
    while let Some(entry) = traversal.pop() {
        let node_ref = &entry.node_ref;
        let encoded = match reader.get(node_ref.locator, node_ref.object_hash).await {
            Ok(encoded) => encoded,
            Err(crate::chunks::ChunkStorageError::Unavailable) => {
                missing.push(entry.node_ref);
                if missing.len() >= limit.max(1) {
                    break;
                }
                continue;
            }
            Err(error) => return Err(crate::chunks::ChunkError::from(error).into()),
        };
        let node = decode_node_for_format(
            value.format_version,
            value.kind,
            node_ref.object_hash,
            &encoded,
        )?;
        traversal.validate_node(value.kind, node_ref, &node)?;
        if let ChunkNode::Branch { children, .. } = node {
            traversal.discover_children(&entry, children)?;
        }
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
pub(crate) async fn validate_finalized_upload(
    value: &LargeValueRef,
    reader: crate::chunks::LocalChunkReader,
    uploaded_chunks: &std::collections::BTreeSet<NodeRef>,
    descriptor_was_bound_before_completion: bool,
) -> Result<(), ReachabilityError> {
    validate_descriptor(value)?;
    let root_metrics = value.edit_tail.is_empty().then_some(NodeMetrics {
        byte_length: value.byte_length,
        utf16_length: value.utf16_length,
    });
    let mut traversal =
        PhysicalTraversal::new(value.root.clone(), root_metrics, value.logical_hash);
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
        let encoded = reader
            .get(node_ref.locator, node_ref.object_hash)
            .await
            .map_err(crate::chunks::ChunkError::from)?;
        let node = decode_node_for_format(
            value.format_version,
            value.kind,
            node_ref.object_hash,
            &encoded,
        )?;
        traversal.validate_node(value.kind, node_ref, &node)?;
        if let ChunkNode::Branch { children, .. } = node {
            traversal.discover_children(&entry, children)?;
        }
    }
    traversal.finish()?;
    Ok(())
}

#[derive(Clone)]
struct BuiltNode {
    node_ref: NodeRef,
    metrics: NodeMetrics,
    structural_hash: ContentHash,
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

/// Construct the canonical tree, assigning a fresh opaque locator to each new
/// immutable node. Boundary decisions never inspect those locators.
fn prepare_with_locator(
    kind: LargeValueKind,
    logical_bytes: &[u8],
    mut locator_for: impl FnMut(ContentHash) -> Locator,
) -> Result<PreparedLargeValue, Error> {
    validate_logical(kind, logical_bytes)?;
    let mut staged_chunks = Vec::new();
    let mut level = Vec::new();
    for range in leaf_ranges(kind, logical_bytes)? {
        let bytes = logical_bytes[range].to_vec();
        let metrics = metrics(kind, &bytes)?;
        let node = ChunkNode::Leaf {
            format: FORMAT_VERSION,
            kind,
            bytes,
        };
        level.push(stage_node(
            node,
            metrics,
            &mut locator_for,
            &mut staged_chunks,
        )?);
    }

    let mut depth = 0;
    while level.len() > 1 {
        depth += 1;
        if depth > MAX_TREE_DEPTH {
            return Err(Error::MalformedNode);
        }
        let mut next = Vec::new();
        for range in branch_ranges(kind, &level) {
            let group = &level[range];
            let mut group_metrics = group.iter().map(|child| child.metrics);
            let first_metrics = group_metrics.next().ok_or(Error::MalformedNode)?;
            let metrics = group_metrics.try_fold(first_metrics, add_metrics)?;
            let node = ChunkNode::Branch {
                format: FORMAT_VERSION,
                kind,
                children: group
                    .iter()
                    .map(|child| BranchChild {
                        node_ref: child.node_ref.clone(),
                        metrics: child.metrics,
                        logical_hash: child.structural_hash,
                    })
                    .collect(),
            };
            next.push(stage_node(
                node,
                metrics,
                &mut locator_for,
                &mut staged_chunks,
            )?);
        }
        level = next;
    }
    let root = level.pop().expect("empty values have one leaf");
    Ok(PreparedLargeValue {
        value_ref: LargeValueRef {
            kind,
            format_version: FORMAT_VERSION,
            logical_hash: root.structural_hash,
            root: root.node_ref,
            byte_length: root.metrics.byte_length,
            utf16_length: root.metrics.utf16_length,
            edit_tail: Vec::new(),
        },
        staged_chunks,
    })
}

/// Construct a canonical tree with fresh random capabilities for every new
/// immutable node.
pub fn prepare(kind: LargeValueKind, logical_bytes: &[u8]) -> Result<PreparedLargeValue, Error> {
    prepare_with_locator(kind, logical_bytes, |_| Locator::random())
}

/// Rebuild while preserving the exact retrieval identity of every byte-equal
/// node from a previous preparation. Local consolidation uses the same reuse
/// rule while avoiding traversal of unaffected subtrees.
pub fn prepare_reusing(
    kind: LargeValueKind,
    logical_bytes: &[u8],
    previous: &[StagedChunk],
) -> Result<PreparedLargeValue, Error> {
    let existing = previous
        .iter()
        .map(|chunk| (chunk.node_ref.object_hash, chunk.node_ref.locator))
        .collect::<std::collections::BTreeMap<_, _>>();
    prepare_with_locator(kind, logical_bytes, |hash| {
        existing.get(&hash).cloned().unwrap_or_else(Locator::random)
    })
}

/// Encode the internal stored-scalar enum using Groove's ordinary enum and
/// record codecs. The containing schema supplies the declared kind, including
/// the backing primitive type and the expected kind of every referenced node.
pub fn encode_stored_scalar(kind: LargeValueKind, value: &StoredScalar) -> Result<Vec<u8>, Error> {
    #[cfg(test)]
    STORED_SCALAR_ENCODE_CALLS.with(|calls| calls.set(calls.get() + 1));
    let schema = stored_scalar_schema(kind);
    let enum_value = match value {
        StoredScalar::Primitive(bytes) => {
            validate_logical(kind, bytes)?;
            let fields = primitive_payload_schema(kind)
                .ordered_values([(PRIMITIVE_VALUE_FIELD, primitive_value(kind, bytes.clone()))])?;
            EnumValue::create(
                2,
                schema.case(2).map_err(|_| Error::MalformedScalar)?.payload,
                &fields,
            )
            .map_err(|_| Error::MalformedScalar)?
        }
        StoredScalar::Chunked(value) => {
            if value.kind != kind {
                return Err(Error::DescriptorMismatch);
            }
            validate_descriptor(value)?;
            EnumValue::create(
                3,
                schema.case(3).map_err(|_| Error::MalformedScalar)?.payload,
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
    static STORED_SCALAR_ENCODE_CALLS: Cell<usize> = const { Cell::new(0) };
}

#[cfg(test)]
fn reset_stored_scalar_encode_calls() {
    STORED_SCALAR_ENCODE_CALLS.with(|calls| calls.set(0));
}

#[cfg(test)]
fn stored_scalar_encode_calls() -> usize {
    STORED_SCALAR_ENCODE_CALLS.with(|calls| calls.get())
}

/// Decode and canonically validate the internal stored-scalar enum. The
/// declared kind is supplied by the containing schema. Primitive payloads are
/// interpreted directly through that schema; indirect values authenticate the
/// expected kind when their content-addressed nodes are decoded.
pub fn decode_stored_scalar(kind: LargeValueKind, encoded: &[u8]) -> Result<StoredScalar, Error> {
    let decoded =
        crate::records::decode_single_field_value(encoded, stored_scalar_value_type(kind))
            .map_err(|error| match error {
                crate::records::Error::InvalidUtf8 => Error::InvalidUtf8,
                _ => Error::MalformedScalar,
            })?;
    let Value::Enum(value) = decoded else {
        return Err(Error::MalformedScalar);
    };
    let canonical = crate::records::encode_single_field_value(
        &Value::Enum(value.clone()),
        stored_scalar_value_type(kind),
    )
    .map_err(|_| Error::MalformedScalar)?;
    if canonical != encoded {
        return Err(Error::MalformedScalar);
    }
    let values = value
        .record()
        .to_values()
        .map_err(|_| Error::MalformedScalar)?;
    match value.tag() {
        2 => {
            let mut fields = primitive_payload_schema(kind).decode_values(&values)?;
            let value = take_durable_large_value_field(&mut fields, PRIMITIVE_VALUE_FIELD)?;
            primitive_bytes(kind, &value).map(StoredScalar::Primitive)
        }
        3 => decode_chunked_values(kind, &values).map(StoredScalar::Chunked),
        _ => Err(Error::MalformedScalar),
    }
}

pub fn inline_scalar_bytes(kind: LargeValueKind, encoded: &[u8]) -> Result<&[u8], Error> {
    let schema = stored_scalar_schema(kind);
    let (tag, payload) =
        crate::records::split_variant_record(encoded).map_err(|_| Error::MalformedScalar)?;
    match tag {
        2 => {
            let descriptor = schema.case(2).map_err(|_| Error::MalformedScalar)?.payload;
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
        3 => {
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

pub(crate) fn node_ref_descriptor() -> RecordDescriptor {
    node_ref_record_schema().descriptor
}

fn node_ref_record_schema() -> &'static DurableLargeValueRecordSchema {
    static SCHEMA: OnceLock<DurableLargeValueRecordSchema> = OnceLock::new();
    SCHEMA.get_or_init(|| {
        durable_large_value_record_descriptor([
            (
                NODE_REF_OBJECT_HASH_FIELD,
                "object_hash",
                ValueType::raw_bytes(),
            ),
            (NODE_REF_LOCATOR_FIELD, "locator", ValueType::raw_bytes()),
        ])
    })
}

pub(crate) fn node_ref_value(node_ref: &NodeRef) -> Value {
    let descriptor = node_ref_descriptor();
    Value::Record(crate::records::OwnedRecord::new(
        encode_node_ref(node_ref).expect("NodeRef fields always match their physical descriptor"),
        descriptor,
    ))
}

pub(crate) fn node_ref_from_value(value: &Value) -> Result<NodeRef, Error> {
    let Value::Record(record) = value else {
        return Err(Error::MalformedScalar);
    };
    let values = record.to_values().map_err(|_| Error::MalformedScalar)?;
    node_ref_from_values(&values)
}

fn node_ref_from_values(values: &[Value]) -> Result<NodeRef, Error> {
    let mut fields = node_ref_record_schema().decode_values(values)?;
    Ok(NodeRef {
        object_hash: ContentHash(raw_bytes(&take_durable_large_value_field(
            &mut fields,
            NODE_REF_OBJECT_HASH_FIELD,
        )?)?),
        locator: Locator(raw_bytes(&take_durable_large_value_field(
            &mut fields,
            NODE_REF_LOCATOR_FIELD,
        )?)?),
    })
}

/// Encode a physical chunk identity through the same canonical Groove record
/// used by indirect scalar roots.
pub(crate) fn encode_node_ref(node_ref: &NodeRef) -> Result<Vec<u8>, Error> {
    let values = node_ref_record_schema().ordered_values([
        (
            NODE_REF_OBJECT_HASH_FIELD,
            Value::Bytes(node_ref.object_hash.0.to_vec()),
        ),
        (
            NODE_REF_LOCATOR_FIELD,
            Value::Bytes(node_ref.locator.0.to_vec()),
        ),
    ])?;
    node_ref_descriptor()
        .create(&values)
        .map_err(|_| Error::MalformedScalar)
}

/// Decode and canonically validate a physical chunk identity.
pub(crate) fn decode_node_ref(encoded: &[u8]) -> Result<NodeRef, Error> {
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
    let chunked = large_value_ref_payload_descriptor();
    EnumSchema::new(
        match kind {
            LargeValueKind::Bytes => "groove.internal.stored_scalar.bytes",
            LargeValueKind::String => "groove.internal.stored_scalar.string",
            LargeValueKind::Json => "groove.internal.stored_scalar.json",
        },
        [
            // Tags 0 and 1 belonged to the pre-v13 private scalar codec. Keep
            // them reserved and reject them at the scalar boundary so no
            // legacy byte sequence can be accepted with a different meaning. In particular, legacy
            // inline bytes were `[0] + payload`, which can otherwise collide
            // exactly with a length-prefixed canonical record.
            EnumCase::new(
                "ReservedLegacyPrimitive",
                RecordDescriptor::new(Vec::<(String, ValueType)>::new()),
            ),
            EnumCase::new(
                "ReservedLegacyChunked",
                RecordDescriptor::new(Vec::<(String, ValueType)>::new()),
            ),
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

fn large_value_edit_descriptor() -> RecordDescriptor {
    large_value_edit_schema().descriptor
}

fn large_value_edit_schema() -> &'static DurableLargeValueRecordSchema {
    static SCHEMA: OnceLock<DurableLargeValueRecordSchema> = OnceLock::new();
    SCHEMA.get_or_init(|| {
        durable_large_value_record_descriptor([
            (EDIT_OFFSET_FIELD, "offset", ValueType::U64),
            (EDIT_DELETE_LENGTH_FIELD, "delete_length", ValueType::U64),
            (
                EDIT_INSERT_BYTES_FIELD,
                "insert_bytes",
                ValueType::raw_bytes(),
            ),
            (EDIT_UTF16_OFFSET_FIELD, "utf16_offset", ValueType::U64),
            (
                EDIT_DELETE_UTF16_LENGTH_FIELD,
                "delete_utf16_length",
                ValueType::U64,
            ),
            (
                EDIT_INSERT_UTF16_LENGTH_FIELD,
                "insert_utf16_length",
                ValueType::U64,
            ),
        ])
    })
}

fn large_value_ref_payload_descriptor() -> RecordDescriptor {
    large_value_ref_payload_schema().descriptor
}

fn large_value_ref_payload_schema() -> &'static DurableLargeValueRecordSchema {
    static SCHEMA: OnceLock<DurableLargeValueRecordSchema> = OnceLock::new();
    SCHEMA.get_or_init(|| {
        durable_large_value_record_descriptor([
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
                LARGE_VALUE_REF_UTF16_LENGTH_FIELD,
                "utf16_length",
                ValueType::Nullable(Box::new(ValueType::U64)),
            ),
            (
                LARGE_VALUE_REF_EDIT_TAIL_FIELD,
                "edit_tail",
                ValueType::Array(Box::new(ValueType::Record(Box::new(
                    large_value_edit_descriptor(),
                )))),
            ),
        ])
    })
}

const NODE_REF_OBJECT_HASH_FIELD: u16 = 1;
const NODE_REF_LOCATOR_FIELD: u16 = 2;
const PRIMITIVE_VALUE_FIELD: u16 = 1;
const EDIT_OFFSET_FIELD: u16 = 1;
const EDIT_DELETE_LENGTH_FIELD: u16 = 2;
const EDIT_INSERT_BYTES_FIELD: u16 = 3;
const EDIT_UTF16_OFFSET_FIELD: u16 = 4;
const EDIT_DELETE_UTF16_LENGTH_FIELD: u16 = 5;
const EDIT_INSERT_UTF16_LENGTH_FIELD: u16 = 6;
const LARGE_VALUE_REF_FORMAT_VERSION_FIELD: u16 = 1;
const LARGE_VALUE_REF_LOGICAL_HASH_FIELD: u16 = 2;
const LARGE_VALUE_REF_ROOT_FIELD: u16 = 3;
const LARGE_VALUE_REF_BYTE_LENGTH_FIELD: u16 = 4;
const LARGE_VALUE_REF_UTF16_LENGTH_FIELD: u16 = 5;
const LARGE_VALUE_REF_EDIT_TAIL_FIELD: u16 = 6;

#[derive(Clone)]
struct DurableLargeValueRecordSchema {
    slots: Vec<DurableLargeValueRecordSlot>,
    descriptor: RecordDescriptor,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DurableLargeValueRecordSlot {
    Known(u16),
    Reserved(u16),
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
        let mut ordered = Vec::with_capacity(self.slots.len());
        for slot in &self.slots {
            match slot {
                DurableLargeValueRecordSlot::Known(id) => {
                    ordered.push(values.remove(id).ok_or(Error::MalformedScalar)?)
                }
                DurableLargeValueRecordSlot::Reserved(_) => {
                    ordered.push(Value::Nullable(None));
                }
            }
        }
        if values.is_empty() {
            Ok(ordered)
        } else {
            Err(Error::MalformedScalar)
        }
    }

    fn decode_values(&self, values: &[Value]) -> Result<BTreeMap<u16, Value>, Error> {
        if values.len() != self.slots.len() {
            return Err(Error::MalformedScalar);
        }
        let mut fields = BTreeMap::new();
        for (slot, value) in self.slots.iter().zip(values) {
            match slot {
                DurableLargeValueRecordSlot::Known(id) => {
                    fields.insert(*id, value.clone());
                }
                DurableLargeValueRecordSlot::Reserved(_)
                    if matches!(value, Value::Nullable(None)) => {}
                DurableLargeValueRecordSlot::Reserved(_) => return Err(Error::MalformedScalar),
            }
        }
        Ok(fields)
    }
}

fn take_durable_large_value_field(
    fields: &mut BTreeMap<u16, Value>,
    id: u16,
) -> Result<Value, Error> {
    fields.remove(&id).ok_or(Error::MalformedScalar)
}

/// Construct a fixed engine-owned record layout from permanent, one-based
/// positional field IDs. Source declaration order is normalized, but numeric
/// IDs are actual record ordinals: skipped IDs become canonical empty nullable
/// slots and are never compacted away.
fn durable_large_value_record_descriptor(
    fields: impl IntoIterator<Item = (u16, &'static str, ValueType)>,
) -> DurableLargeValueRecordSchema {
    let mut fields = fields.into_iter().collect::<Vec<_>>();
    fields.sort_by_key(|(id, _, _)| *id);
    assert!(
        fields.iter().all(|(id, _, _)| *id != 0),
        "large-value durable field IDs start at one"
    );
    assert!(
        fields.windows(2).all(|fields| fields[0].0 != fields[1].0),
        "large-value record has duplicate durable field IDs"
    );
    let max_field_id = fields
        .last()
        .expect("large-value record must have at least one field")
        .0;
    let mut fields = fields.into_iter().peekable();
    let mut slots = Vec::with_capacity(usize::from(max_field_id));
    let mut descriptor_fields = Vec::with_capacity(usize::from(max_field_id));
    for slot_id in 1..=max_field_id {
        match fields.peek() {
            Some((id, _, _)) if *id == slot_id => {
                let (_, name, value_type) = fields.next().expect("peeked field must exist");
                slots.push(DurableLargeValueRecordSlot::Known(slot_id));
                descriptor_fields.push((format!("f{slot_id:04}_{name}"), value_type));
            }
            _ => {
                slots.push(DurableLargeValueRecordSlot::Reserved(slot_id));
                descriptor_fields.push((
                    format!("f{slot_id:04}_reserved"),
                    ValueType::Nullable(Box::new(ValueType::raw_bytes())),
                ));
            }
        }
    }
    debug_assert!(fields.next().is_none());
    DurableLargeValueRecordSchema {
        slots,
        descriptor: RecordDescriptor::new(descriptor_fields),
    }
}

const LARGE_VALUE_REF_BYTES_TAG: u8 = 0;
const LARGE_VALUE_REF_STRING_TAG: u8 = 1;
const LARGE_VALUE_REF_JSON_TAG: u8 = 2;

fn large_value_ref_schema() -> &'static EnumSchema {
    static SCHEMA: OnceLock<EnumSchema> = OnceLock::new();
    SCHEMA.get_or_init(|| {
        let payload = large_value_ref_payload_descriptor();
        let cases = [
            (LARGE_VALUE_REF_BYTES_TAG, EnumCase::new("Bytes", payload)),
            (LARGE_VALUE_REF_STRING_TAG, EnumCase::new("String", payload)),
            (LARGE_VALUE_REF_JSON_TAG, EnumCase::new("Json", payload)),
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

pub(crate) fn large_value_ref_value_type() -> &'static ValueType {
    static VALUE_TYPE: OnceLock<ValueType> = OnceLock::new();
    VALUE_TYPE.get_or_init(|| ValueType::Enum(Box::new(large_value_ref_schema().clone())))
}

pub(crate) fn large_value_ref_value(value: &LargeValueRef) -> Result<Value, Error> {
    validate_descriptor(value)?;
    let tag = u32::from(large_value_kind_tag(value.kind));
    let payload = large_value_ref_schema()
        .case(tag)
        .map_err(|_| Error::MalformedScalar)?
        .payload;
    let value = EnumValue::create(tag, payload, &chunked_values(value))
        .map_err(|_| Error::MalformedScalar)?;
    Ok(Value::Enum(value))
}

pub(crate) fn large_value_ref_from_value(value: &Value) -> Result<LargeValueRef, Error> {
    let Value::Enum(value) = value else {
        return Err(Error::MalformedScalar);
    };
    let kind =
        large_value_kind_from_tag(u8::try_from(value.tag()).map_err(|_| Error::MalformedScalar)?)?;
    let values = value
        .record()
        .to_values()
        .map_err(|_| Error::MalformedScalar)?;
    decode_chunked_values(kind, &values)
}

pub(crate) fn encode_large_value_ref(value: &LargeValueRef) -> Result<Vec<u8>, Error> {
    crate::records::encode_single_field_value(
        &large_value_ref_value(value)?,
        large_value_ref_value_type(),
    )
    .map_err(|_| Error::MalformedScalar)
}

#[cfg(test)]
pub(crate) fn decode_large_value_ref(encoded: &[u8]) -> Result<LargeValueRef, Error> {
    let value = crate::records::decode_single_field_value(encoded, large_value_ref_value_type())
        .map_err(|_| Error::MalformedScalar)?;
    let decoded = large_value_ref_from_value(&value)?;
    if encode_large_value_ref(&decoded)? != encoded {
        return Err(Error::MalformedScalar);
    }
    Ok(decoded)
}

fn primitive_value(kind: LargeValueKind, bytes: Vec<u8>) -> Value {
    match kind {
        LargeValueKind::Bytes => Value::Bytes(bytes),
        LargeValueKind::String | LargeValueKind::Json => {
            Value::String(String::from_utf8(bytes).expect("validated logical text/JSON primitive"))
        }
    }
}

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
    large_value_ref_payload_schema()
        .ordered_values([
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
                LARGE_VALUE_REF_UTF16_LENGTH_FIELD,
                Value::Nullable(value.utf16_length.map(|value| Box::new(Value::U64(value)))),
            ),
            (
                LARGE_VALUE_REF_EDIT_TAIL_FIELD,
                Value::Array(
                    value
                        .edit_tail
                        .iter()
                        .map(|edit_value| {
                            let fields = large_value_edit_schema()
                                .ordered_values([
                                    (EDIT_OFFSET_FIELD, Value::U64(edit_value.offset)),
                                    (
                                        EDIT_DELETE_LENGTH_FIELD,
                                        Value::U64(edit_value.delete_length),
                                    ),
                                    (
                                        EDIT_INSERT_BYTES_FIELD,
                                        Value::Bytes(edit_value.insert_bytes.clone()),
                                    ),
                                    (EDIT_UTF16_OFFSET_FIELD, Value::U64(edit_value.utf16_offset)),
                                    (
                                        EDIT_DELETE_UTF16_LENGTH_FIELD,
                                        Value::U64(edit_value.delete_utf16_length),
                                    ),
                                    (
                                        EDIT_INSERT_UTF16_LENGTH_FIELD,
                                        Value::U64(edit_value.insert_utf16_length),
                                    ),
                                ])
                                .expect("all internal edit fields are present");
                            Value::Record(crate::records::OwnedRecord::new(
                                large_value_edit_descriptor()
                                    .create(&fields)
                                    .expect("internal edit record"),
                                large_value_edit_descriptor(),
                            ))
                        })
                        .collect(),
                ),
            ),
        ])
        .expect("all internal large-value reference fields are present")
}

fn decode_chunked_values(kind: LargeValueKind, values: &[Value]) -> Result<LargeValueRef, Error> {
    let mut fields = large_value_ref_payload_schema().decode_values(values)?;
    let Value::U8(format_version) =
        take_durable_large_value_field(&mut fields, LARGE_VALUE_REF_FORMAT_VERSION_FIELD)?
    else {
        return Err(Error::MalformedScalar);
    };
    let logical_hash =
        take_durable_large_value_field(&mut fields, LARGE_VALUE_REF_LOGICAL_HASH_FIELD)?;
    let root = take_durable_large_value_field(&mut fields, LARGE_VALUE_REF_ROOT_FIELD)?;
    let Value::U64(byte_length) =
        take_durable_large_value_field(&mut fields, LARGE_VALUE_REF_BYTE_LENGTH_FIELD)?
    else {
        return Err(Error::MalformedScalar);
    };
    let Value::Nullable(utf16_length) =
        take_durable_large_value_field(&mut fields, LARGE_VALUE_REF_UTF16_LENGTH_FIELD)?
    else {
        return Err(Error::MalformedScalar);
    };
    let Value::Array(edits) =
        take_durable_large_value_field(&mut fields, LARGE_VALUE_REF_EDIT_TAIL_FIELD)?
    else {
        return Err(Error::MalformedScalar);
    };
    let utf16_length = match utf16_length.as_deref() {
        None => None,
        Some(Value::U64(value)) => Some(*value),
        _ => return Err(Error::MalformedScalar),
    };
    let mut edit_tail = Vec::with_capacity(edits.len());
    for value in edits {
        let Value::Record(edit) = value else {
            return Err(Error::MalformedScalar);
        };
        let values = edit.to_values().map_err(|_| Error::MalformedScalar)?;
        let mut fields = large_value_edit_schema().decode_values(&values)?;
        let Value::U64(offset) = take_durable_large_value_field(&mut fields, EDIT_OFFSET_FIELD)?
        else {
            return Err(Error::MalformedScalar);
        };
        let Value::U64(delete_length) =
            take_durable_large_value_field(&mut fields, EDIT_DELETE_LENGTH_FIELD)?
        else {
            return Err(Error::MalformedScalar);
        };
        let Value::Bytes(insert_bytes) =
            take_durable_large_value_field(&mut fields, EDIT_INSERT_BYTES_FIELD)?
        else {
            return Err(Error::MalformedScalar);
        };
        let Value::U64(utf16_offset) =
            take_durable_large_value_field(&mut fields, EDIT_UTF16_OFFSET_FIELD)?
        else {
            return Err(Error::MalformedScalar);
        };
        let Value::U64(delete_utf16_length) =
            take_durable_large_value_field(&mut fields, EDIT_DELETE_UTF16_LENGTH_FIELD)?
        else {
            return Err(Error::MalformedScalar);
        };
        let Value::U64(insert_utf16_length) =
            take_durable_large_value_field(&mut fields, EDIT_INSERT_UTF16_LENGTH_FIELD)?
        else {
            return Err(Error::MalformedScalar);
        };
        edit_tail.push(ReplaceEdit {
            offset,
            delete_length,
            insert_bytes,
            utf16_offset,
            delete_utf16_length,
            insert_utf16_length,
        });
    }
    let value = LargeValueRef {
        kind,
        format_version,
        logical_hash: ContentHash(raw_bytes(&logical_hash)?),
        root: node_ref_from_value(&root)?,
        byte_length,
        utf16_length,
        edit_tail,
    };
    validate_descriptor(&value)?;
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
    validate_descriptor_shape(value)?;
    if value.edit_tail.len() > MAX_EDIT_COUNT {
        return Err(Error::MalformedScalar);
    }
    let mut tail_bytes = 0_usize;
    for edit in &value.edit_tail {
        tail_bytes = tail_bytes
            .checked_add(edit.insert_bytes.len())
            .and_then(|total| total.checked_add(24))
            .ok_or(Error::MetricOverflow)?;
        if tail_bytes > MAX_EDIT_TAIL_BYTES {
            return Err(Error::MalformedScalar);
        }
    }
    Ok(())
}

fn validate_descriptor_shape(value: &LargeValueRef) -> Result<(), Error> {
    check_format(value.format_version)?;
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

/// Reconstruct and replay an untrusted descriptor's tail against its immutable
/// base tree. Shape validation alone cannot prove that text UTF-16 coordinates
/// describe the same byte splice, or that a JSON edit is a whole-value replace.
pub(crate) fn validate_edit_tail_attempt(
    value: &LargeValueRef,
    inputs: &mut EvaluationInputs,
) -> Result<(), IvmRuntimeError> {
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

fn stage_node(
    node: ChunkNode,
    metrics: NodeMetrics,
    locator_for: &mut impl FnMut(ContentHash) -> Locator,
    staged_chunks: &mut Vec<StagedChunk>,
) -> Result<BuiltNode, Error> {
    validate_untyped_node_structure(&node)?;
    if node_metrics(node_kind(&node), &node)? != metrics {
        return Err(Error::DescriptorMismatch);
    }
    let structural_hash = node_logical_hash(&node);
    let encoded = encode_node(&node)?;
    let object_hash = object_hash(&encoded);
    let node_ref = NodeRef {
        object_hash,
        locator: locator_for(object_hash),
    };
    staged_chunks.push(StagedChunk {
        node_ref: node_ref.clone(),
        encoded,
    });
    Ok(BuiltNode {
        node_ref,
        metrics,
        structural_hash,
    })
}

fn stage_node_reusing(
    kind: LargeValueKind,
    node: ChunkNode,
    existing: &std::collections::BTreeMap<ContentHash, Locator>,
    fresh_locator: &mut impl FnMut(ContentHash) -> Locator,
    staged_chunks: &mut Vec<StagedChunk>,
) -> Result<BuiltNode, Error> {
    validate_untyped_node_structure(&node)?;
    let metrics = node_metrics(kind, &node)?;
    let structural_hash = node_logical_hash(&node);
    let encoded = encode_node(&node)?;
    let object_hash = object_hash(&encoded);
    if let Some(locator) = existing.get(&object_hash) {
        return Ok(BuiltNode {
            node_ref: NodeRef {
                object_hash,
                locator: *locator,
            },
            metrics,
            structural_hash,
        });
    }
    let node_ref = NodeRef {
        object_hash,
        locator: fresh_locator(object_hash),
    };
    staged_chunks.push(StagedChunk {
        node_ref: node_ref.clone(),
        encoded,
    });
    Ok(BuiltNode {
        node_ref,
        metrics,
        structural_hash,
    })
}

fn built_from_child(child: BranchChild) -> BuiltNode {
    BuiltNode {
        node_ref: child.node_ref,
        metrics: child.metrics,
        structural_hash: child.logical_hash,
    }
}

fn stage_branch_level_reusing(
    kind: LargeValueKind,
    level: &[BuiltNode],
    existing: &std::collections::BTreeMap<ContentHash, Locator>,
    fresh_locator: &mut impl FnMut(ContentHash) -> Locator,
    staged_chunks: &mut Vec<StagedChunk>,
) -> Result<Vec<BuiltNode>, Error> {
    let mut next = Vec::new();
    for range in branch_ranges(kind, level) {
        let children = level[range]
            .iter()
            .map(|child| BranchChild {
                node_ref: child.node_ref.clone(),
                metrics: child.metrics,
                logical_hash: child.structural_hash,
            })
            .collect();
        next.push(stage_node_reusing(
            kind,
            ChunkNode::Branch {
                format: FORMAT_VERSION,
                kind,
                children,
            },
            existing,
            fresh_locator,
            staged_chunks,
        )?);
    }
    Ok(next)
}

fn load_authenticated_node_attempt(
    format_version: u8,
    kind: LargeValueKind,
    node_ref: &NodeRef,
    expected_logical_hash: ContentHash,
    inputs: &mut EvaluationInputs,
) -> Result<ChunkNode, IvmRuntimeError> {
    let request = ChunkRequest {
        object_hash: node_ref.object_hash.0,
        locator: node_ref.locator,
    };
    let encoded = inputs.chunk(request.clone())?;
    let node = decode_node_for_format(format_version, kind, node_ref.object_hash, encoded)?;
    if node_logical_hash(&node) != expected_logical_hash {
        return Err(Error::DescriptorMismatch.into());
    }
    Ok(node)
}

fn locate_leaf_attempt(
    value: &LargeValueRef,
    offset: u64,
    base_length: u64,
    inputs: &mut EvaluationInputs,
) -> Result<LocatedLeaf, IvmRuntimeError> {
    if offset >= base_length {
        return Err(Error::MalformedScalar.into());
    }
    let mut node_ref = value.root.clone();
    let mut expected_hash = value.logical_hash;
    let mut node_start = 0_u64;
    let mut path = Vec::new();
    loop {
        if path.len() > MAX_TREE_DEPTH {
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
                if offset >= node_start + bytes.len() as u64 {
                    return Err(Error::DescriptorMismatch.into());
                }
                return Ok(LocatedLeaf {
                    node_ref,
                    bytes,
                    start: node_start,
                    path,
                });
            }
            ChunkNode::Branch { children, .. } => {
                let mut child_start = node_start;
                let selected = children
                    .iter()
                    .position(|child| {
                        let end = child_start.saturating_add(child.metrics.byte_length);
                        if offset < end {
                            true
                        } else {
                            child_start = end;
                            false
                        }
                    })
                    .ok_or(Error::DescriptorMismatch)?;
                let child = children[selected].clone();
                path.push(PathFrame {
                    node_ref,
                    children,
                    selected,
                });
                node_ref = child.node_ref;
                expected_hash = child.logical_hash;
                node_start = child_start;
            }
        }
    }
}

fn next_leaf_attempt(
    format_version: u8,
    kind: LargeValueKind,
    current: &LocatedLeaf,
    inputs: &mut EvaluationInputs,
) -> Result<Option<LocatedLeaf>, IvmRuntimeError> {
    let mut path = current.path.clone();
    let mut next = None;
    while let Some(frame) = path.last_mut() {
        if frame.selected + 1 < frame.children.len() {
            frame.selected += 1;
            next = Some(frame.children[frame.selected].clone());
            break;
        }
        path.pop();
    }
    let Some(mut child) = next else {
        return Ok(None);
    };
    let start = current
        .start
        .checked_add(current.bytes.len() as u64)
        .ok_or(Error::MetricOverflow)?;
    loop {
        match load_authenticated_node_attempt(
            format_version,
            kind,
            &child.node_ref,
            child.logical_hash,
            inputs,
        )? {
            ChunkNode::Leaf { bytes, .. } => {
                return Ok(Some(LocatedLeaf {
                    node_ref: child.node_ref,
                    bytes,
                    start,
                    path,
                }));
            }
            ChunkNode::Branch { children, .. } => {
                let first = children.first().cloned().ok_or(Error::MalformedNode)?;
                path.push(PathFrame {
                    node_ref: child.node_ref,
                    children,
                    selected: 0,
                });
                child = first;
            }
        }
    }
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
    let node = LargeValueFormat::from_version(format_version)?.decode_node(encoded)?;
    let encoded_kind = match &node {
        ChunkNode::Leaf { kind, .. } | ChunkNode::Branch { kind, .. } => *kind,
    };
    if encoded_kind != kind {
        return Err(Error::DescriptorMismatch);
    }
    Ok(node)
}

pub(crate) fn decode_authenticated_node(
    expected_hash: ContentHash,
    encoded: &[u8],
) -> Result<ChunkNode, Error> {
    decode_node_untyped_authenticated(expected_hash, encoded)
}

/// Authenticate and canonically decode an immutable-node envelope before a
/// referencing descriptor is available. Descriptor-bound reads additionally
/// apply [`decode_node`]'s kind check.
pub(crate) fn decode_node_untyped_authenticated(
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
    // Durable metadata has no owner descriptor. V2's canonical record has a
    // fixed structural envelope, so decode it only to recover its committed
    // format and then dispatch that exact codec. No fallback or try-current
    // decoding is permitted here.
    let node = decode_canonical_node_v2(encoded)?;
    LargeValueFormat::from_version(node_format(&node))?.decode_node(encoded)
}

/// Encode a chunk node using Groove's ordinary canonical enum/record algebra.
pub fn encode_node(node: &ChunkNode) -> Result<Vec<u8>, Error> {
    LargeValueFormat::from_version(node_format(node))?.encode_node(node)
}

/// The frozen v2 node codec. Future codecs remain separate functions selected
/// by [`LargeValueFormat`], even if they initially share record machinery.
fn encode_node_v2(node: &ChunkNode) -> Result<Vec<u8>, Error> {
    let schema = chunk_node_schema();
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
            let child_schema = chunk_node_child_schema();
            let children = children
                .iter()
                .map(|child| {
                    child_schema
                        .create(&[
                            Value::Bytes(child.node_ref.object_hash.0.to_vec()),
                            Value::Bytes(child.node_ref.locator.0.to_vec()),
                            Value::U64(child.metrics.byte_length),
                            Value::Nullable(
                                child
                                    .metrics
                                    .utf16_length
                                    .map(|value| Box::new(Value::U64(value))),
                            ),
                            Value::Bytes(child.logical_hash.0.to_vec()),
                        ])
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

fn chunk_node_child_schema() -> RecordDescriptor {
    RecordDescriptor::new([
        ("object_hash", ValueType::raw_bytes()),
        ("locator", ValueType::raw_bytes()),
        ("byte_length", ValueType::U64),
        (
            "utf16_length",
            ValueType::Nullable(Box::new(ValueType::U64)),
        ),
        ("logical_hash", ValueType::raw_bytes()),
    ])
}

fn chunk_node_schema() -> EnumSchema {
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
                            chunk_node_child_schema(),
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
fn decode_canonical_node_v2(encoded: &[u8]) -> Result<ChunkNode, Error> {
    if encoded.len() > MAX_ENCODED_NODE_BYTES {
        return Err(Error::MalformedNode);
    }
    let schema = chunk_node_schema();
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
        (1, [Value::U8(format), Value::U8(kind), Value::Array(children)]) => {
            let children = children
                .iter()
                .map(|child| {
                    let Value::Record(child) = child else {
                        return Err(Error::MalformedNode);
                    };
                    let fields = child.to_values().map_err(|_| Error::MalformedNode)?;
                    let [
                        object_hash,
                        locator,
                        Value::U64(byte_length),
                        Value::Nullable(utf16_length),
                        logical_hash,
                    ] = fields.as_slice()
                    else {
                        return Err(Error::MalformedNode);
                    };
                    let utf16_length = match utf16_length.as_deref() {
                        None => None,
                        Some(Value::U64(value)) => Some(*value),
                        _ => return Err(Error::MalformedNode),
                    };
                    Ok(BranchChild {
                        node_ref: NodeRef {
                            object_hash: ContentHash(
                                raw_bytes(object_hash).map_err(|_| Error::MalformedNode)?,
                            ),
                            locator: Locator(raw_bytes(locator).map_err(|_| Error::MalformedNode)?),
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
                kind: large_value_kind_from_tag(*kind).map_err(|_| Error::MalformedNode)?,
                children,
            }
        }
        _ => return Err(Error::MalformedNode),
    };
    let canonical = encode_node_v2(&node)?;
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
pub(crate) fn decode_canonical_node(encoded: &[u8]) -> Result<ChunkNode, Error> {
    let node = decode_canonical_node_v2(encoded)?;
    LargeValueFormat::from_version(node_format(&node))?.decode_node(encoded)
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
        b"groove-large-logical-kind-format-v2",
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
pub(crate) fn materialize_attempt(
    value: &LargeValueRef,
    inputs: &mut EvaluationInputs,
) -> Result<Vec<u8>, IvmRuntimeError> {
    check_format(value.format_version)?;
    let root_metrics = value.edit_tail.is_empty().then_some(NodeMetrics {
        byte_length: value.byte_length,
        utf16_length: value.utf16_length,
    });
    let mut pending = vec![(
        value.root.clone(),
        0_usize,
        root_metrics,
        value.logical_hash,
    )];
    let mut leaves = Vec::<(NodeRef, Vec<u8>)>::new();
    let mut blocked = false;
    let mut budget = LogicalTraversalBudget::new();
    while let Some((node_ref, depth, expected_metrics, expected_logical_hash)) = pending.pop() {
        budget.consume()?;
        if depth > MAX_TREE_DEPTH {
            return Err(Error::InvalidTree.into());
        }
        let node = match load_authenticated_node_attempt(
            value.format_version,
            value.kind,
            &node_ref,
            expected_logical_hash,
            inputs,
        ) {
            Ok(node) => node,
            Err(IvmRuntimeError::EvaluationBlocked) => {
                blocked = true;
                continue;
            }
            Err(error) => return Err(error),
        };
        match node {
            ChunkNode::Leaf { bytes, .. } => {
                if expected_metrics
                    .is_some_and(|expected| metrics(value.kind, &bytes).ok() != Some(expected))
                {
                    return Err(Error::DescriptorMismatch.into());
                }
                leaves.push((node_ref, bytes));
            }
            ChunkNode::Branch { children, .. } => {
                if let Some(expected) = expected_metrics {
                    let mut child_metrics = children.iter().map(|child| child.metrics);
                    let Some(first) = child_metrics.next() else {
                        return Err(Error::MalformedNode.into());
                    };
                    if child_metrics.try_fold(first, add_metrics)? != expected {
                        return Err(Error::DescriptorMismatch.into());
                    }
                }
                budget.consume_many(children.len())?;
                for child in children.into_iter().rev() {
                    pending.push((
                        child.node_ref,
                        depth + 1,
                        Some(child.metrics),
                        child.logical_hash,
                    ));
                }
            }
        }
    }
    if blocked {
        return Err(IvmRuntimeError::EvaluationBlocked);
    }
    let mut bytes =
        Vec::with_capacity(usize::try_from(value.byte_length).map_err(|_| Error::MetricOverflow)?);
    for (_, leaf) in leaves {
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
    Ok(bytes)
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

pub(crate) fn materialize_record_attempt(
    descriptor: &RecordDescriptor,
    raw: &[u8],
    inputs: &mut EvaluationInputs,
) -> Result<Vec<u8>, IvmRuntimeError> {
    let mut values = descriptor.bind(raw).to_values()?;
    let mut blocked = false;
    let mut changed = false;
    for value in &mut values {
        changed |= materialize_value_attempt(value, inputs, &mut blocked)?;
    }
    if blocked {
        return Err(IvmRuntimeError::EvaluationBlocked);
    }
    if changed {
        Ok(descriptor.create(&values)?)
    } else {
        Ok(raw.to_vec())
    }
}

/// Materialize only the logical fields an operator will inspect. Unselected
/// indirect arms remain encoded as descriptors in the rebuilt record.
pub(crate) fn materialize_record_fields_attempt(
    descriptor: &RecordDescriptor,
    raw: &[u8],
    field_indices: &[usize],
    inputs: &mut EvaluationInputs,
) -> Result<Vec<u8>, IvmRuntimeError> {
    let mut values = descriptor.bind(raw).to_values()?;
    let mut blocked = false;
    let mut changed = false;
    for index in field_indices {
        let value = values
            .get_mut(*index)
            .ok_or(crate::records::Error::FieldIndexOutOfBounds {
                index: *index,
                len: descriptor.fields().len(),
            })?;
        changed |= materialize_value_attempt(value, inputs, &mut blocked)?;
    }
    if blocked {
        return Err(IvmRuntimeError::EvaluationBlocked);
    }
    if changed {
        Ok(descriptor.create(&values)?)
    } else {
        Ok(raw.to_vec())
    }
}

fn materialize_value_attempt(
    value: &mut Value,
    inputs: &mut EvaluationInputs,
    blocked: &mut bool,
) -> Result<bool, IvmRuntimeError> {
    match value {
        Value::Large(large) => match materialize_attempt(large, inputs) {
            Ok(bytes) => {
                *value = match large.kind {
                    LargeValueKind::Bytes => Value::Bytes(bytes),
                    // Groove currently exposes JSON source through its string
                    // scalar type. Validation remains kind-specific at the
                    // indirect representation boundary.
                    LargeValueKind::String | LargeValueKind::Json => {
                        Value::String(String::from_utf8(bytes).map_err(|_| Error::InvalidUtf8)?)
                    }
                };
                Ok(true)
            }
            Err(IvmRuntimeError::EvaluationBlocked) => {
                *blocked = true;
                Ok(false)
            }
            Err(error) => Err(error),
        },
        Value::Tuple(values) | Value::Array(values) => {
            let mut changed = false;
            for value in values {
                changed |= materialize_value_attempt(value, inputs, blocked)?;
            }
            Ok(changed)
        }
        Value::Nullable(Some(value)) => materialize_value_attempt(value, inputs, blocked),
        Value::Record(record) => {
            let descriptor = *record.descriptor();
            let materialized = materialize_record_attempt(&descriptor, record.raw(), inputs);
            match materialized {
                Ok(raw) => {
                    let changed = raw.as_slice() != record.raw();
                    *record = crate::records::OwnedRecord::new(raw, descriptor);
                    Ok(changed)
                }
                Err(IvmRuntimeError::EvaluationBlocked) => {
                    *blocked = true;
                    Ok(false)
                }
                Err(error) => Err(error),
            }
        }
        Value::Enum(enum_value) => {
            let tag = enum_value.tag();
            let descriptor = *enum_value.record().descriptor();
            match materialize_record_attempt(&descriptor, enum_value.record().raw(), inputs) {
                Ok(raw) => {
                    let changed = raw.as_slice() != enum_value.record().raw();
                    *enum_value = crate::records::EnumValue::new(
                        tag,
                        crate::records::OwnedRecord::new(raw, descriptor),
                    );
                    Ok(changed)
                }
                Err(IvmRuntimeError::EvaluationBlocked) => {
                    *blocked = true;
                    Ok(false)
                }
                Err(error) => Err(error),
            }
        }
        _ => Ok(false),
    }
}

#[derive(Clone, Debug)]
enum RangePiece {
    Base(std::ops::Range<u64>),
    Inserted(Vec<u8>),
}

#[derive(Clone, Debug)]
enum Utf16RangePiece {
    Base(std::ops::Range<u64>),
    Inserted(Vec<u8>),
}

pub(crate) fn utf16_range_attempt(
    value: &LargeValueRef,
    range: std::ops::Range<u64>,
    inputs: &mut EvaluationInputs,
) -> Result<Vec<u8>, IvmRuntimeError> {
    let length = value.utf16_length.ok_or(Error::MalformedScalar)?;
    if range.start > range.end || range.end > length {
        return Err(Error::MalformedScalar.into());
    }
    let pieces = map_final_utf16_range_to_base(range, &value.edit_tail)?;
    let base_utf16 = value.edit_tail.iter().rev().try_fold(length, |n, edit| {
        n.checked_sub(edit.insert_utf16_length)
            .and_then(|n| n.checked_add(edit.delete_utf16_length))
            .ok_or(Error::MetricOverflow)
    })?;
    let mut outputs = Vec::with_capacity(pieces.len());
    let mut blocked = false;
    for piece in pieces {
        match piece {
            Utf16RangePiece::Inserted(bytes) => outputs.push(bytes),
            Utf16RangePiece::Base(range) => {
                match base_utf16_range_attempt(value, base_utf16, range, inputs) {
                    Ok(bytes) => outputs.push(bytes),
                    Err(IvmRuntimeError::EvaluationBlocked) => blocked = true,
                    Err(error) => return Err(error),
                }
            }
        }
    }
    if blocked {
        Err(IvmRuntimeError::EvaluationBlocked)
    } else {
        Ok(outputs.into_iter().flatten().collect())
    }
}

/// Resolve a final logical UTF-16 boundary to its final logical byte offset
/// without materializing the preceding text.  Prefix lengths are computed from
/// authenticated branch metrics, with only the logarithmically many boundary
/// leaves requested.  The binary search is over UTF-8 code-point boundaries so
/// a position inside a surrogate pair fails rather than being rounded.
pub(crate) fn utf16_offset_to_byte_attempt(
    value: &LargeValueRef,
    offset: u64,
    inputs: &mut EvaluationInputs,
) -> Result<u64, IvmRuntimeError> {
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
    inputs: &mut EvaluationInputs,
) -> Result<u64, IvmRuntimeError> {
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
    inputs: &mut EvaluationInputs,
) -> Result<u64, IvmRuntimeError> {
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

fn map_final_utf16_range_to_base(
    range: std::ops::Range<u64>,
    edits: &[ReplaceEdit],
) -> Result<Vec<Utf16RangePiece>, Error> {
    let mut pieces = vec![Utf16RangePiece::Base(range)];
    for edit in edits.iter().rev() {
        let inserted_end = edit
            .utf16_offset
            .checked_add(edit.insert_utf16_length)
            .ok_or(Error::MetricOverflow)?;
        let mut previous = Vec::new();
        for piece in pieces {
            let Utf16RangePiece::Base(range) = piece else {
                previous.push(piece);
                continue;
            };
            if range.start < edit.utf16_offset {
                previous.push(Utf16RangePiece::Base(
                    range.start..range.end.min(edit.utf16_offset),
                ));
            }
            let overlap_start = range.start.max(edit.utf16_offset);
            let overlap_end = range.end.min(inserted_end);
            if overlap_start < overlap_end {
                previous.push(Utf16RangePiece::Inserted(slice_utf16(
                    &edit.insert_bytes,
                    (overlap_start - edit.utf16_offset)..(overlap_end - edit.utf16_offset),
                )?));
            }
            if range.end > inserted_end {
                let start = range.start.max(inserted_end);
                let map = |position: u64| {
                    position
                        .checked_sub(edit.insert_utf16_length)
                        .and_then(|n| n.checked_add(edit.delete_utf16_length))
                        .ok_or(Error::MetricOverflow)
                };
                previous.push(Utf16RangePiece::Base(map(start)?..map(range.end)?));
            }
        }
        pieces = previous;
    }
    Ok(pieces)
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

fn base_utf16_range_attempt(
    value: &LargeValueRef,
    base_utf16: u64,
    range: std::ops::Range<u64>,
    inputs: &mut EvaluationInputs,
) -> Result<Vec<u8>, IvmRuntimeError> {
    if range.end > base_utf16 {
        return Err(Error::MalformedScalar.into());
    }
    if range.is_empty() {
        return Ok(Vec::new());
    }
    let mut pending = vec![(
        value.root.clone(),
        value.logical_hash,
        0_u64,
        base_utf16,
        0_usize,
    )];
    let mut slices = Vec::new();
    let mut blocked = false;
    let mut budget = LogicalTraversalBudget::new();
    while let Some((node_ref, hash, start, length, depth)) = pending.pop() {
        budget.consume()?;
        if depth > MAX_TREE_DEPTH {
            return Err(Error::InvalidTree.into());
        }
        let end = start.checked_add(length).ok_or(Error::MetricOverflow)?;
        if range.start >= end || range.end <= start {
            continue;
        }
        let node = match load_authenticated_node_attempt(
            value.format_version,
            value.kind,
            &node_ref,
            hash,
            inputs,
        ) {
            Ok(node) => node,
            Err(IvmRuntimeError::EvaluationBlocked) => {
                blocked = true;
                continue;
            }
            Err(error) => return Err(error),
        };
        match node {
            ChunkNode::Leaf { bytes, .. } => {
                let part = slice_utf16(
                    &bytes,
                    (range.start.max(start) - start)..(range.end.min(end) - start),
                )?;
                slices.push((start, part));
            }
            ChunkNode::Branch { children, .. } => {
                budget.consume_many(children.len())?;
                let mut child_start = start;
                let mut next = Vec::with_capacity(children.len());
                for child in children {
                    let child_length = child
                        .metrics
                        .utf16_length
                        .ok_or(Error::DescriptorMismatch)?;
                    next.push((
                        child.node_ref,
                        child.logical_hash,
                        child_start,
                        child_length,
                        depth + 1,
                    ));
                    child_start = child_start
                        .checked_add(child_length)
                        .ok_or(Error::MetricOverflow)?;
                }
                if child_start != end {
                    return Err(Error::DescriptorMismatch.into());
                }
                pending.extend(next.into_iter().rev());
            }
        }
    }
    if blocked {
        return Err(IvmRuntimeError::EvaluationBlocked);
    }
    slices.sort_by_key(|(start, _)| *start);
    Ok(slices.into_iter().flat_map(|(_, bytes)| bytes).collect())
}

/// Read a byte range from the final logical value. Edit-tail insertions are
/// served directly and only intersecting base-tree paths are requested.
pub(crate) fn byte_range_attempt(
    value: &LargeValueRef,
    range: std::ops::Range<u64>,
    inputs: &mut EvaluationInputs,
) -> Result<Vec<u8>, IvmRuntimeError> {
    if range.start > range.end || range.end > value.byte_length {
        return Err(Error::MalformedScalar.into());
    }
    let pieces = map_final_range_to_base(range, &value.edit_tail)?;
    let base_length = base_length(value.byte_length, &value.edit_tail)?;
    let mut outputs = Vec::with_capacity(pieces.len());
    let mut blocked = false;
    for piece in pieces {
        match piece {
            RangePiece::Inserted(bytes) => outputs.push(bytes),
            RangePiece::Base(range) => {
                match base_range_attempt(value, base_length, range, inputs) {
                    Ok(bytes) => outputs.push(bytes),
                    Err(IvmRuntimeError::EvaluationBlocked) => blocked = true,
                    Err(error) => return Err(error),
                }
            }
        }
    }
    if blocked {
        return Err(IvmRuntimeError::EvaluationBlocked);
    }
    Ok(outputs.into_iter().flatten().collect())
}

/// Count UTF-16 units in a byte-coordinate range without hydrating complete
/// subtrees. Fully covered children contribute their authenticated aggregate
/// metric; only boundary leaves are decoded.
fn utf16_length_for_byte_range_attempt(
    value: &LargeValueRef,
    range: std::ops::Range<u64>,
    inputs: &mut EvaluationInputs,
) -> Result<u64, IvmRuntimeError> {
    if value.kind == LargeValueKind::Bytes
        || range.start > range.end
        || range.end > value.byte_length
    {
        return Err(Error::MalformedScalar.into());
    }
    let pieces = map_final_range_to_base(range, &value.edit_tail)?;
    let base_length = base_length(value.byte_length, &value.edit_tail)?;
    let mut total = 0_u64;
    let mut blocked = false;
    for piece in pieces {
        let result = match piece {
            RangePiece::Inserted(bytes) => std::str::from_utf8(&bytes)
                .map_err(|_| IvmRuntimeError::from(Error::InvalidUtf8))
                .and_then(|text| {
                    u64::try_from(text.encode_utf16().count())
                        .map_err(|_| Error::MetricOverflow.into())
                }),
            RangePiece::Base(range) => {
                base_utf16_length_for_byte_range_attempt(value, base_length, range, inputs)
            }
        };
        match result {
            Ok(length) => total = total.checked_add(length).ok_or(Error::MetricOverflow)?,
            Err(IvmRuntimeError::EvaluationBlocked) => blocked = true,
            Err(error) => return Err(error),
        }
    }
    if blocked {
        Err(IvmRuntimeError::EvaluationBlocked)
    } else {
        Ok(total)
    }
}

fn base_utf16_length_for_byte_range_attempt(
    value: &LargeValueRef,
    base_length: u64,
    range: std::ops::Range<u64>,
    inputs: &mut EvaluationInputs,
) -> Result<u64, IvmRuntimeError> {
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
    let mut pending = vec![(
        value.root.clone(),
        value.logical_hash,
        0_u64,
        base_length,
        base_utf16,
        0_usize,
    )];
    let mut total = 0_u64;
    let mut blocked = false;
    let mut budget = LogicalTraversalBudget::new();
    while let Some((node_ref, expected_hash, start, bytes_len, utf16_len, depth)) = pending.pop() {
        budget.consume()?;
        if depth > MAX_TREE_DEPTH {
            return Err(Error::InvalidTree.into());
        }
        let end = start.checked_add(bytes_len).ok_or(Error::MetricOverflow)?;
        if range.start >= end || range.end <= start {
            continue;
        }
        if range.start <= start && end <= range.end {
            total = total.checked_add(utf16_len).ok_or(Error::MetricOverflow)?;
            continue;
        }
        let node = match load_authenticated_node_attempt(
            value.format_version,
            value.kind,
            &node_ref,
            expected_hash,
            inputs,
        ) {
            Ok(node) => node,
            Err(IvmRuntimeError::EvaluationBlocked) => {
                blocked = true;
                continue;
            }
            Err(error) => return Err(error),
        };
        match node {
            ChunkNode::Leaf { bytes, .. } => {
                let a = usize::try_from(range.start.max(start) - start)
                    .map_err(|_| Error::MetricOverflow)?;
                let b = usize::try_from(range.end.min(end) - start)
                    .map_err(|_| Error::MetricOverflow)?;
                let text = std::str::from_utf8(&bytes[a..b]).map_err(|_| Error::InvalidUtf8)?;
                total = total
                    .checked_add(
                        u64::try_from(text.encode_utf16().count())
                            .map_err(|_| Error::MetricOverflow)?,
                    )
                    .ok_or(Error::MetricOverflow)?;
            }
            ChunkNode::Branch { children, .. } => {
                budget.consume_many(children.len())?;
                let mut child_start = start;
                let mut next = Vec::with_capacity(children.len());
                for child in children {
                    let child_utf16 = child
                        .metrics
                        .utf16_length
                        .ok_or(Error::DescriptorMismatch)?;
                    next.push((
                        child.node_ref,
                        child.logical_hash,
                        child_start,
                        child.metrics.byte_length,
                        child_utf16,
                        depth + 1,
                    ));
                    child_start = child_start
                        .checked_add(child.metrics.byte_length)
                        .ok_or(Error::MetricOverflow)?;
                }
                pending.extend(next.into_iter().rev());
            }
        }
    }
    if blocked {
        Err(IvmRuntimeError::EvaluationBlocked)
    } else {
        Ok(total)
    }
}

/// Compare an indirect logical scalar to inline source bytes, requesting one
/// bounded window at a time and stopping at the first decisive mismatch.
pub(crate) fn compare_inline_attempt(
    value: &LargeValueRef,
    inline: &[u8],
    inputs: &mut EvaluationInputs,
) -> Result<std::cmp::Ordering, IvmRuntimeError> {
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
pub(crate) enum JsonPointerPrefix {
    Found(Option<serde_json::Value>),
    NeedMore,
    RequiresFullDocument,
}

/// Resolve pointers whose first component is an array index from a source
/// prefix. Once that complete array element has parsed, later source bytes
/// cannot change its value. Object-root pointers deliberately use the full
/// document path because serde_json's literal semantics retain the last
/// duplicate key.
pub(crate) fn json_pointer_prefix(
    source: &[u8],
    pointer: &str,
) -> Result<JsonPointerPrefix, Error> {
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

fn base_length(final_length: u64, edits: &[ReplaceEdit]) -> Result<u64, Error> {
    edits.iter().rev().try_fold(final_length, |length, edit| {
        let inserted = u64::try_from(edit.insert_bytes.len()).map_err(|_| Error::MetricOverflow)?;
        length
            .checked_sub(inserted)
            .and_then(|length| length.checked_add(edit.delete_length))
            .ok_or(Error::MetricOverflow)
    })
}

fn map_final_range_to_base(
    range: std::ops::Range<u64>,
    edits: &[ReplaceEdit],
) -> Result<Vec<RangePiece>, Error> {
    let mut pieces = vec![RangePiece::Base(range)];
    for edit in edits.iter().rev() {
        let inserted_length =
            u64::try_from(edit.insert_bytes.len()).map_err(|_| Error::MetricOverflow)?;
        let inserted_end = edit
            .offset
            .checked_add(inserted_length)
            .ok_or(Error::MetricOverflow)?;
        let shift_after = if edit.delete_length >= inserted_length {
            (true, edit.delete_length - inserted_length)
        } else {
            (false, inserted_length - edit.delete_length)
        };
        let mut previous = Vec::new();
        for piece in pieces {
            let RangePiece::Base(range) = piece else {
                previous.push(piece);
                continue;
            };
            if range.start < edit.offset {
                previous.push(RangePiece::Base(range.start..range.end.min(edit.offset)));
            }
            let overlap_start = range.start.max(edit.offset);
            let overlap_end = range.end.min(inserted_end);
            if overlap_start < overlap_end {
                let start = usize::try_from(overlap_start - edit.offset)
                    .map_err(|_| Error::MetricOverflow)?;
                let end = usize::try_from(overlap_end - edit.offset)
                    .map_err(|_| Error::MetricOverflow)?;
                previous.push(RangePiece::Inserted(edit.insert_bytes[start..end].to_vec()));
            }
            if range.end > inserted_end {
                let start = range.start.max(inserted_end);
                let map = |position: u64| -> Result<u64, Error> {
                    if shift_after.0 {
                        position
                            .checked_add(shift_after.1)
                            .ok_or(Error::MetricOverflow)
                    } else {
                        position
                            .checked_sub(shift_after.1)
                            .ok_or(Error::MetricOverflow)
                    }
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
    inputs: &mut EvaluationInputs,
) -> Result<Vec<u8>, IvmRuntimeError> {
    if range.end > base_length {
        return Err(Error::MalformedScalar.into());
    }
    if range.is_empty() {
        return Ok(Vec::new());
    }
    let mut pending = vec![(
        value.root.clone(),
        value.logical_hash,
        0_u64,
        base_length,
        0_usize,
    )];
    let mut slices = Vec::<(u64, Vec<u8>)>::new();
    let mut blocked = false;
    let mut budget = LogicalTraversalBudget::new();
    while let Some((node_ref, expected_hash, start, length, depth)) = pending.pop() {
        budget.consume()?;
        if depth > MAX_TREE_DEPTH {
            return Err(Error::InvalidTree.into());
        }
        let end = start.checked_add(length).ok_or(Error::MetricOverflow)?;
        if range.start >= end || range.end <= start {
            continue;
        }
        let node = match load_authenticated_node_attempt(
            value.format_version,
            value.kind,
            &node_ref,
            expected_hash,
            inputs,
        ) {
            Ok(node) => node,
            Err(IvmRuntimeError::EvaluationBlocked) => {
                blocked = true;
                continue;
            }
            Err(error) => return Err(error),
        };
        match node {
            ChunkNode::Leaf { bytes, .. } => {
                if u64::try_from(bytes.len()).map_err(|_| Error::MetricOverflow)? != length {
                    return Err(Error::DescriptorMismatch.into());
                }
                let slice_start = usize::try_from(range.start.max(start) - start)
                    .map_err(|_| Error::MetricOverflow)?;
                let slice_end = usize::try_from(range.end.min(end) - start)
                    .map_err(|_| Error::MetricOverflow)?;
                slices.push((start, bytes[slice_start..slice_end].to_vec()));
            }
            ChunkNode::Branch { children, .. } => {
                let total = children.iter().try_fold(0_u64, |total, child| {
                    total
                        .checked_add(child.metrics.byte_length)
                        .ok_or(Error::MetricOverflow)
                })?;
                if total != length {
                    return Err(Error::DescriptorMismatch.into());
                }
                budget.consume_many(children.len())?;
                let mut child_start = start;
                let mut children_with_offsets = Vec::with_capacity(children.len());
                for child in children {
                    children_with_offsets.push((
                        child.node_ref,
                        child.logical_hash,
                        child_start,
                        child.metrics.byte_length,
                        depth + 1,
                    ));
                    child_start = child_start
                        .checked_add(child.metrics.byte_length)
                        .ok_or(Error::MetricOverflow)?;
                }
                pending.extend(children_with_offsets.into_iter().rev());
            }
        }
    }
    if blocked {
        return Err(IvmRuntimeError::EvaluationBlocked);
    }
    slices.sort_by_key(|(start, _)| *start);
    Ok(slices.into_iter().flat_map(|(_, bytes)| bytes).collect())
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
    let mut ranges = Vec::new();
    let mut start = 0;
    while start < bytes.len() {
        let hard_end = (start + LEAF_MAX_BYTES).min(bytes.len());
        let mut hash = 0_u64;
        let mut end = hard_end;
        for (offset, byte) in bytes[start..hard_end].iter().enumerate() {
            hash = hash.wrapping_shl(1).wrapping_add(gear(*byte));
            let length = offset + 1;
            let boundary = if length < LEAF_MIN_BYTES {
                false
            } else if length < LEAF_TARGET_BYTES {
                hash & (LEAF_TARGET_BYTES as u64 * 2 - 1) == 0
            } else {
                hash & (LEAF_TARGET_BYTES as u64 / 2 - 1) == 0
            };
            if boundary {
                end = start + length;
                break;
            }
        }
        if let Some(text) = text {
            while end > start && !text.is_char_boundary(end) {
                end -= 1;
            }
            if end == start {
                return Err(Error::MalformedNode);
            }
        }
        ranges.push(start..end);
        start = end;
    }
    Ok(ranges)
}

fn branch_ranges(kind: LargeValueKind, nodes: &[BuiltNode]) -> Vec<std::ops::Range<usize>> {
    let mut ranges = Vec::new();
    let mut start = 0;
    while start < nodes.len() {
        let hard_end = (start + BRANCH_MAX_CHILDREN).min(nodes.len());
        let mut hash = 0_u64;
        let mut end = hard_end;
        for (offset, child) in nodes[start..hard_end].iter().enumerate() {
            for byte in grouping_hash_from_logical(FORMAT_VERSION, kind, child.structural_hash).0 {
                hash = hash.wrapping_shl(1).wrapping_add(gear(byte));
            }
            let length = offset + 1;
            let boundary = if length < BRANCH_MIN_CHILDREN {
                false
            } else if length < BRANCH_TARGET_CHILDREN {
                hash & (BRANCH_TARGET_CHILDREN as u64 * 2 - 1) == 0
            } else {
                hash & (BRANCH_TARGET_CHILDREN as u64 / 2 - 1) == 0
            };
            if boundary {
                end = start + length;
                break;
            }
        }
        ranges.push(start..end);
        start = end;
    }
    ranges
}

fn gear(byte: u8) -> u64 {
    let mut value = u64::from(byte).wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
fn stage_unvalidated_fixture_node(
    node: ChunkNode,
    locator_for: &mut impl FnMut(ContentHash) -> Locator,
    staged_chunks: &mut Vec<StagedChunk>,
) -> BuiltNode {
    let kind = node_kind(&node);
    let metrics = node_metrics(kind, &node).unwrap();
    let structural_hash = node_logical_hash(&node);
    let encoded = encode_node(&node).unwrap();
    let object_hash = object_hash(&encoded);
    let node_ref = NodeRef {
        object_hash,
        locator: locator_for(object_hash),
    };
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

/// Build an explicitly malformed bottom-up DAG whose every branch repeats one
/// zero-metric child. This fixture deliberately bypasses canonical staging so
/// authenticated historical/wire nodes can exercise fail-closed admission.
#[cfg(test)]
pub(crate) fn zero_metric_repeated_child_dag_fixture(
    depth: usize,
    fanout: usize,
) -> PreparedLargeValue {
    assert!(depth <= MAX_TREE_DEPTH);
    assert!((1..=BRANCH_MAX_CHILDREN).contains(&fanout));
    let mut staged_chunks = Vec::new();
    let mut nonce = 0_u64;
    let mut locator = |hash: ContentHash| {
        let mut seed = hash.0.to_vec();
        seed.extend_from_slice(&nonce.to_le_bytes());
        nonce += 1;
        Locator::from_seed(&seed)
    };
    let leaf = ChunkNode::Leaf {
        format: FORMAT_VERSION,
        kind: LargeValueKind::Bytes,
        bytes: Vec::new(),
    };
    let mut current = stage_node(
        leaf.clone(),
        node_metrics(LargeValueKind::Bytes, &leaf).unwrap(),
        &mut locator,
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
        current = stage_unvalidated_fixture_node(node, &mut locator, &mut staged_chunks);
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
pub(crate) fn positive_repeated_child_dag_fixture(
    depth: usize,
    fanout: usize,
) -> PreparedLargeValue {
    assert!(depth <= MAX_TREE_DEPTH);
    assert!((1..=BRANCH_MAX_CHILDREN).contains(&fanout));
    let mut staged_chunks = Vec::new();
    let mut nonce = 0_u64;
    let mut locator = |hash: ContentHash| {
        let mut seed = hash.0.to_vec();
        seed.extend_from_slice(&nonce.to_le_bytes());
        nonce += 1;
        Locator::from_seed(&seed)
    };
    let leaf = ChunkNode::Leaf {
        format: FORMAT_VERSION,
        kind: LargeValueKind::Bytes,
        bytes: vec![0x5a],
    };
    let mut current = stage_node(
        leaf.clone(),
        node_metrics(LargeValueKind::Bytes, &leaf).unwrap(),
        &mut locator,
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
            &mut locator,
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

/// Build a four-node diamond in which two distinct physical branch nodes share
/// one leaf. The branches intentionally use distinct locators even though their
/// immutable bytes match, exercising ownership through distinct `NodeRef`s.
#[cfg(test)]
pub(crate) fn shared_child_dag_fixture() -> PreparedLargeValue {
    let mut staged_chunks = Vec::new();
    let mut nonce = 0_u64;
    let mut locator = |hash: ContentHash| {
        let mut seed = hash.0.to_vec();
        seed.extend_from_slice(&nonce.to_le_bytes());
        nonce += 1;
        Locator::from_seed(&seed)
    };
    let mut stage = |node: ChunkNode| {
        stage_node(
            node.clone(),
            node_metrics(LargeValueKind::Bytes, &node).unwrap(),
            &mut locator,
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
    drop(stage);
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
            locator: Locator::from_seed(b"canonical NodeRef codec"),
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
            .create(&[Value::Bytes(vec![0; 31]), Value::Bytes(vec![0; 32])])
            .unwrap();
        assert_eq!(decode_node_ref(&malformed), Err(Error::MalformedScalar));
    }

    // This is intentionally an internal physical-codec receipt. Public rows
    // only see logical primitives; the exact bytes here freeze the engine's
    // descriptor-led v2 boundary and make a future codec addition explicit.
    #[test]
    fn v2_codec_golden_bytes_decode_semantically_and_reject_alternates() {
        fn hex(bytes: &[u8]) -> String {
            bytes.iter().map(|byte| format!("{byte:02x}")).collect()
        }

        let leaf = ChunkNode::Leaf {
            format: FORMAT_VERSION,
            kind: LargeValueKind::Bytes,
            bytes: b"v2-fixture".to_vec(),
        };
        let node_bytes = encode_node(&leaf).unwrap();
        let node_hash = object_hash(&node_bytes);
        assert_eq!(
            hex(&node_bytes),
            "00020076322d66697874757265",
            "v2 node bytes are a reviewed storage fixture"
        );
        assert_eq!(
            hex(&node_hash.0),
            "a8f6ec8e407e168b63923c3b2fa558d390672a0db53338497fd4257245918978",
            "object hashes commit to the exact v2 node bytes"
        );
        assert_eq!(
            hex(&node_logical_hash(&leaf).0),
            "7ddfe3b3961b5d41459b122dd696fa07867c754d797939efd7b7e09c81a3bfbb",
            "logical hashes bind the v2 format and semantic kind"
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
                locator: Locator([0x44; 32]),
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
            "00020a000000000000000000000000000000003a0000007e0000007ddfe3b3961b5d41459b122dd696fa07867c754d797939efd7b7e09c81a3bfbb24000000a8f6ec8e407e168b63923c3b2fa558d390672a0db53338497fd42572459189784444444444444444444444444444444444444444444444444444444444444444010000000900000000000000010000000000000000000000000000000000000000000000000000000000000065",
            "v2 descriptor bytes are a reviewed storage fixture"
        );
        let decoded_ref = decode_large_value_ref(&descriptor_bytes).unwrap();
        assert_eq!(decoded_ref, value_ref);
        let mut inputs = EvaluationInputs::default();
        inputs.install_chunk(
            ChunkRequest {
                object_hash: node_hash.0,
                locator: Locator([0x44; 32]),
            },
            bytes::Bytes::from(node_bytes.clone()),
        );
        assert_eq!(
            materialize_attempt(&decoded_ref, &mut inputs).unwrap(),
            b"v2-fixture",
            "decoded fixture reaches the same logical scalar receipt"
        );

        let scalar = StoredScalar::Chunked(value_ref);
        let scalar_bytes = encode_stored_scalar(LargeValueKind::Bytes, &scalar).unwrap();
        assert_eq!(
            hex(&scalar_bytes),
            "03020a000000000000000000000000000000003a0000007e0000007ddfe3b3961b5d41459b122dd696fa07867c754d797939efd7b7e09c81a3bfbb24000000a8f6ec8e407e168b63923c3b2fa558d390672a0db53338497fd42572459189784444444444444444444444444444444444444444444444444444444444444444010000000900000000000000010000000000000000000000000000000000000000000000000000000000000065",
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
                locator: Locator([0x66; 32]),
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
            decode_node_for_format(3, LargeValueKind::Bytes, node_hash, &node_bytes),
            Err(Error::UnsupportedFormat(3)),
            "a descriptor never falls back to the current codec"
        );

        let text = ChunkNode::Leaf {
            format: FORMAT_VERSION,
            kind: LargeValueKind::String,
            bytes: "v2-🙂".as_bytes().to_vec(),
        };
        let json = ChunkNode::Leaf {
            format: FORMAT_VERSION,
            kind: LargeValueKind::Json,
            bytes: br#"{"n":-0}"#.to_vec(),
        };
        for (node, expected_bytes, expected_object_hash, expected_logical_hash, expected_metrics) in [
            (
                text,
                "00020176322df09f9982",
                "678e46c71b86713680adea8f58bda0ead55aa464331f72dae2bc89c9de37382c",
                "7c1bf3f4b3db7ef7f523bcfd24dd10dc421d41c248f1811ca1d35367f5a5d247",
                NodeMetrics {
                    byte_length: 7,
                    utf16_length: Some(5),
                },
            ),
            (
                json,
                "0002027b226e223a2d307d",
                "b73917ac4decd2f0698b805c22cdb5f10ba1a16447f10221c15ca2d34d4c051e",
                "b4f699c671ee5f343a5b14ebd2a1b0811118f056e5fee14e458ecea7bb345baf",
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

    // This is an intentionally internal physical-codec receipt. Application
    // rows cannot construct these engine-owned descriptors; the test freezes
    // the slot contract that makes future storage-format changes explicit.
    #[test]
    fn durable_large_value_field_ids_are_physical_slots() {
        fn encode(
            schema: &DurableLargeValueRecordSchema,
            fields: impl IntoIterator<Item = (u16, Value)>,
        ) -> Vec<u8> {
            let values = schema.ordered_values(fields).unwrap();
            schema.descriptor.create(&values).unwrap()
        }
        fn to_hex(bytes: &[u8]) -> String {
            bytes.iter().map(|byte| format!("{byte:02x}")).collect()
        }
        fn assert_renumber_is_physical(
            before_schema: &DurableLargeValueRecordSchema,
            after_schema: &DurableLargeValueRecordSchema,
            before_fields: Vec<(u16, Value)>,
            after_fields: Vec<(u16, Value)>,
        ) -> (Vec<u8>, Vec<u8>) {
            let before = encode(before_schema, before_fields);
            let after = encode(after_schema, after_fields);
            assert_ne!(before, after, "renumbering changes canonical bytes");
            assert!(
                after_schema
                    .descriptor
                    .bind(&before)
                    .to_values()
                    .map_err(|_| ())
                    .and_then(|old_values| after_schema.decode_values(&old_values).map_err(|_| ()))
                    .is_err(),
                "a renumbered schema must reject the old record slots"
            );
            (before, after)
        }

        let locator_at_two = durable_large_value_record_descriptor([
            (1, "object_hash", ValueType::U64),
            (2, "locator", ValueType::U64),
        ]);
        let locator_at_two_reordered = durable_large_value_record_descriptor([
            (2, "locator", ValueType::U64),
            (1, "object_hash", ValueType::U64),
        ]);
        let locator_at_three = durable_large_value_record_descriptor([
            (1, "object_hash", ValueType::U64),
            (3, "locator", ValueType::U64),
        ]);
        let locator_two = encode(&locator_at_two, [(1, Value::U64(7)), (2, Value::U64(11))]);
        assert_eq!(
            locator_two,
            encode(
                &locator_at_two_reordered,
                [(2, Value::U64(11)), (1, Value::U64(7))],
            ),
            "source declaration order is not physical"
        );
        let (locator_two, locator_three) = assert_renumber_is_physical(
            &locator_at_two,
            &locator_at_three,
            vec![(1, Value::U64(7)), (2, Value::U64(11))],
            vec![(1, Value::U64(7)), (3, Value::U64(11))],
        );
        assert_eq!(to_hex(&locator_two), "07000000000000000b00000000000000");
        assert_eq!(to_hex(&locator_three), "07000000000000000b0000000000000000");

        let edit_at_three = durable_large_value_record_descriptor([
            (1, "offset", ValueType::U64),
            (2, "delete_length", ValueType::U64),
            (3, "insert_bytes", ValueType::U64),
        ]);
        let edit_at_four = durable_large_value_record_descriptor([
            (1, "offset", ValueType::U64),
            (2, "delete_length", ValueType::U64),
            (4, "insert_bytes", ValueType::U64),
        ]);
        let (edit_three, edit_four) = assert_renumber_is_physical(
            &edit_at_three,
            &edit_at_four,
            vec![
                (1, Value::U64(17)),
                (2, Value::U64(19)),
                (3, Value::U64(23)),
            ],
            vec![
                (1, Value::U64(17)),
                (2, Value::U64(19)),
                (4, Value::U64(23)),
            ],
        );
        assert_eq!(
            to_hex(&edit_three),
            "110000000000000013000000000000001700000000000000"
        );
        assert_eq!(
            to_hex(&edit_four),
            "11000000000000001300000000000000170000000000000000"
        );

        let reference_at_three = durable_large_value_record_descriptor([
            (1, "format_version", ValueType::U64),
            (2, "logical_hash", ValueType::U64),
            (3, "root", ValueType::U64),
        ]);
        let reference_at_four = durable_large_value_record_descriptor([
            (1, "format_version", ValueType::U64),
            (2, "logical_hash", ValueType::U64),
            (4, "root", ValueType::U64),
        ]);
        let (reference_three, reference_four) = assert_renumber_is_physical(
            &reference_at_three,
            &reference_at_four,
            vec![
                (1, Value::U64(29)),
                (2, Value::U64(31)),
                (3, Value::U64(37)),
            ],
            vec![
                (1, Value::U64(29)),
                (2, Value::U64(31)),
                (4, Value::U64(37)),
            ],
        );
        assert_eq!(
            to_hex(&reference_three),
            "1d000000000000001f000000000000002500000000000000"
        );
        assert_eq!(
            to_hex(&reference_four),
            "1d000000000000001f00000000000000250000000000000000"
        );

        let nonempty_reserved = locator_at_three
            .descriptor
            .create(&[
                Value::U64(7),
                Value::Nullable(Some(Box::new(Value::Bytes(vec![1])))),
                Value::U64(11),
            ])
            .unwrap();
        let values = locator_at_three
            .descriptor
            .bind(&nonempty_reserved)
            .to_values()
            .unwrap();
        assert!(
            locator_at_three.decode_values(&values).is_err(),
            "reserved slots must remain canonical empty nulls"
        );
    }

    struct PreparedProvider {
        chunks: std::collections::BTreeMap<Locator, (ContentHash, bytes::Bytes)>,
    }

    impl PreparedProvider {
        fn new(prepared: &PreparedLargeValue) -> Self {
            Self {
                chunks: prepared
                    .staged_chunks
                    .iter()
                    .map(|chunk| {
                        (
                            chunk.node_ref.locator,
                            (
                                chunk.node_ref.object_hash,
                                bytes::Bytes::copy_from_slice(&chunk.encoded),
                            ),
                        )
                    })
                    .collect(),
            }
        }
    }

    impl crate::chunks::ChunkProvider for PreparedProvider {
        fn get(
            &self,
            request: ChunkRequest,
        ) -> crate::chunks::ChunkFuture<'_, Result<bytes::Bytes, crate::chunks::ChunkError>>
        {
            Box::pin(async move {
                self.chunks
                    .get(&request.locator)
                    .filter(|(hash, _)| hash.0 == request.object_hash)
                    .map(|(_, bytes)| bytes.clone())
                    .ok_or(crate::chunks::ChunkError::Unavailable)
            })
        }
    }

    #[test]
    fn physical_graph_completion_accepts_sharing_but_rejects_cycles_and_long_paths() {
        let node_ref = |id: u8| NodeRef {
            object_hash: ContentHash([id; 32]),
            locator: Locator::from_seed(&[id]),
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
            locator: root.node_ref.locator,
        };
        assert!(
            visited.is_empty(),
            "an authenticated malformed root must not expose its child frontier"
        );

        let mut inputs = EvaluationInputs::default();
        inputs.install_chunk(root_request, bytes::Bytes::copy_from_slice(&root.encoded));
        assert!(matches!(
            materialize_attempt(&prepared.value_ref, &mut inputs),
            Err(IvmRuntimeError::LargeValue(Error::MalformedNode))
        ));
        assert!(
            inputs.take_missing_chunks().is_empty(),
            "materialization must reject the root before requesting descendants"
        );
    }

    #[test]
    fn canonical_empty_value_is_one_empty_root_leaf() {
        let prepared =
            prepare_with_locator(LargeValueKind::Bytes, b"", deterministic_locator).unwrap();
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

        let mut inputs = EvaluationInputs::default();
        inputs.install_chunk(
            ChunkRequest {
                object_hash: root.node_ref.object_hash.0,
                locator: root.node_ref.locator,
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

        let owned = crate::chunks::OwnedChunkProvider::new(std::rc::Rc::new(
            PreparedProvider::new(&prepared),
        ));
        let mut cursor = LargeValueUploadCursor::new(&prepared.value_ref, owned).unwrap();
        assert!(matches!(
            futures::executor::block_on(cursor.next_batch(0)),
            Err(ReachabilityError::LargeValue(
                Error::InvalidUploadBatchLimit
            ))
        ));
        let mut uploaded = Vec::new();
        loop {
            let batch = futures::executor::block_on(cursor.next_batch(7)).unwrap();
            if batch.is_empty() {
                break;
            }
            uploaded.extend(batch);
        }
        assert_eq!(uploaded.len(), prepared.staged_chunks.len());
        assert_eq!(
            uploaded
                .iter()
                .map(|chunk| chunk.node_ref.clone())
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            prepared.staged_chunks.len()
        );

        let mut inputs = EvaluationInputs::default();
        for chunk in &prepared.staged_chunks {
            inputs.install_chunk(
                ChunkRequest {
                    object_hash: chunk.node_ref.object_hash.0,
                    locator: chunk.node_ref.locator,
                },
                bytes::Bytes::copy_from_slice(&chunk.encoded),
            );
        }
        assert_eq!(
            materialize_attempt(&prepared.value_ref, &mut inputs).unwrap(),
            vec![0x5a; fanout.pow(depth as u32)]
        );
    }

    fn deterministic_locator(hash: ContentHash) -> Locator {
        Locator(hash.0)
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

    fn encode_v12_primitive_bytes(bytes: &[u8]) -> Vec<u8> {
        let primitive = RecordDescriptor::new([("value", ValueType::raw_bytes())]);
        let chunked = RecordDescriptor::new(Vec::<(String, ValueType)>::new());
        let schema = EnumSchema::new(
            "groove.internal.stored_scalar.bytes",
            [
                EnumCase::new("Primitive", primitive),
                EnumCase::new("Chunked", chunked),
            ],
        )
        .unwrap();
        let value = EnumValue::create(0, primitive, &[Value::Bytes(bytes.to_vec())]).unwrap();
        crate::records::encode_single_field_value(
            &Value::Enum(value),
            &ValueType::Enum(Box::new(schema)),
        )
        .unwrap()
    }

    fn encode_v12_chunked_scalar(value: &LargeValueRef) -> Vec<u8> {
        let primitive = RecordDescriptor::new([("value", ValueType::raw_bytes())]);
        let root = RecordDescriptor::new([
            ("object_hash", ValueType::raw_bytes()),
            ("locator", ValueType::raw_bytes()),
        ]);
        let edit = RecordDescriptor::new([
            ("offset", ValueType::U64),
            ("delete_length", ValueType::U64),
            ("insert_bytes", ValueType::raw_bytes()),
            ("utf16_offset", ValueType::U64),
            ("delete_utf16_length", ValueType::U64),
            ("insert_utf16_length", ValueType::U64),
        ]);
        let chunked = RecordDescriptor::new([
            ("format_version", ValueType::U8),
            ("logical_hash", ValueType::raw_bytes()),
            ("root", ValueType::Record(Box::new(root))),
            ("byte_length", ValueType::U64),
            (
                "utf16_length",
                ValueType::Nullable(Box::new(ValueType::U64)),
            ),
            (
                "edit_tail",
                ValueType::Array(Box::new(ValueType::Record(Box::new(edit)))),
            ),
        ]);
        let schema = EnumSchema::new(
            "groove.internal.stored_scalar.bytes",
            [
                EnumCase::new("Primitive", primitive),
                EnumCase::new("Chunked", chunked),
            ],
        )
        .unwrap();
        let mut values = chunked_values(value);
        values[0] = Value::U8(1);
        // This deliberately constructs the superseded v12 descriptor, whose
        // field names predate the current numeric field identities. The
        // receipt must prove those legacy bytes still fail closed rather than
        // accidentally reusing current helper descriptors.
        values[2] = Value::Record(crate::records::OwnedRecord::new(
            root.create(&[
                Value::Bytes(value.root.object_hash.0.to_vec()),
                Value::Bytes(value.root.locator.0.to_vec()),
            ])
            .unwrap(),
            root,
        ));
        let value = EnumValue::create(1, schema.case(1).unwrap().payload, &values).unwrap();
        crate::records::encode_single_field_value(
            &Value::Enum(value),
            &ValueType::Enum(Box::new(schema)),
        )
        .unwrap()
    }

    #[test]
    fn public_preparation_allocates_fresh_full_width_capabilities() {
        let prepare_signature: fn(LargeValueKind, &[u8]) -> Result<PreparedLargeValue, Error> =
            prepare;
        let first = prepare_signature(LargeValueKind::Bytes, b"same logical bytes").unwrap();
        let second = prepare_signature(LargeValueKind::Bytes, b"same logical bytes").unwrap();

        assert_eq!(first.value_ref.logical_hash, second.value_ref.logical_hash);
        assert_ne!(first.value_ref.root.locator, second.value_ref.root.locator);

        let samples = (0..256).map(|_| Locator::random()).collect::<Vec<_>>();
        for byte in 0..LOCATOR_BYTES {
            for bit in 0..8 {
                let mask = 1 << bit;
                assert!(samples.iter().any(|locator| locator.0[byte] & mask == 0));
                assert!(samples.iter().any(|locator| locator.0[byte] & mask != 0));
            }
        }
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
        let prepared =
            prepare_with_locator(LargeValueKind::Bytes, &logical, deterministic_locator).unwrap();
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
            let expected = prepare_with_locator(kind, &bytes, deterministic_locator).unwrap();
            for seed in 1..=12 {
                let mut staged = Vec::new();
                let (actual, stats) = prepare_streaming_with_locator(
                    kind,
                    WindowReader {
                        bytes: &bytes,
                        offset: 0,
                        state: seed,
                    },
                    deterministic_locator,
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
    fn push_streaming_json_does_not_buffer_one_large_string_token() {
        let bytes = format!("{{\"body\":\"{}\"}}", "x".repeat(LEAF_MAX_BYTES * 8)).into_bytes();
        let expected =
            prepare_with_locator(LargeValueKind::Json, &bytes, deterministic_locator).unwrap();
        let mut staged = Vec::new();
        let mut preparation = PushStreamingPreparation::new_with_locator(
            LargeValueKind::Json,
            deterministic_locator,
            |chunk| {
                staged.push(chunk);
                Ok(())
            },
        );
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
        let result = prepare_streaming_with_locator(
            LargeValueKind::Json,
            std::io::Cursor::new(&invalid_json),
            deterministic_locator,
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
        let result = prepare_streaming_with_locator(
            LargeValueKind::Bytes,
            std::io::Cursor::new(vec![7; LEAF_MAX_BYTES * 3]),
            deterministic_locator,
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
        let first = prepare_with_locator(
            LargeValueKind::String,
            text.as_bytes(),
            deterministic_locator,
        )
        .unwrap();
        let second = prepare_with_locator(
            LargeValueKind::String,
            text.as_bytes(),
            deterministic_locator,
        )
        .unwrap();

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
        let prepared = prepare_with_locator(
            LargeValueKind::String,
            base.as_bytes(),
            deterministic_locator,
        )
        .unwrap();
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

    #[test]
    fn append_tail_reports_consolidation_before_exceeding_its_hard_bound() {
        let prepared =
            prepare_with_locator(LargeValueKind::Bytes, b"base", deterministic_locator).unwrap();
        let mut value = prepared.value_ref;
        for _ in 0..MAX_EDIT_COUNT {
            let TailAppendOutcome::Updated(updated) = append_tail(&value, vec![1]).unwrap() else {
                panic!("the configured edit-count bound should be admissible");
            };
            value = updated;
        }
        assert!(matches!(
            append_tail(&value, vec![2]).unwrap(),
            TailAppendOutcome::ConsolidationRequired(_)
        ));
    }

    #[test]
    fn byte_replacement_enters_tail_without_fetching_the_base_tree() {
        let prepared = prepare_with_locator(
            LargeValueKind::Bytes,
            &vec![3; LEAF_TARGET_BYTES * 4],
            deterministic_locator,
        )
        .unwrap();
        let mut inputs = EvaluationInputs::default();
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
        let prepared = prepare_with_locator(
            LargeValueKind::String,
            base.as_bytes(),
            deterministic_locator,
        )
        .unwrap();
        let start = base.find("😀").unwrap() as u64;
        let end = start + "😀".len() as u64;
        let available = prepared
            .staged_chunks
            .iter()
            .map(|chunk| {
                (
                    ChunkRequest {
                        object_hash: chunk.node_ref.object_hash.0,
                        locator: chunk.node_ref.locator,
                    },
                    bytes::Bytes::copy_from_slice(&chunk.encoded),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut inputs = EvaluationInputs::default();
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
                Err(IvmRuntimeError::EvaluationBlocked) => {
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
        let prepared = prepare_with_locator(LargeValueKind::Bytes, &base, |hash| {
            let mut locator = b"old/".to_vec();
            locator.extend_from_slice(&hash.0[..20]);
            Locator::from_seed(&locator)
        })
        .unwrap();
        let suffix = vec![0x5a; LEAF_TARGET_BYTES * 3];
        let TailAppendOutcome::Updated(with_tail) =
            append_tail(&prepared.value_ref, suffix.clone()).unwrap()
        else {
            panic!("test append must fit");
        };
        let available = prepared
            .staged_chunks
            .iter()
            .map(|chunk| {
                (
                    ChunkRequest {
                        object_hash: chunk.node_ref.object_hash.0,
                        locator: chunk.node_ref.locator,
                    },
                    bytes::Bytes::copy_from_slice(&chunk.encoded),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut inputs = EvaluationInputs::default();
        let mut requested = std::collections::BTreeSet::new();
        let consolidated = loop {
            match consolidate_appends_attempt(&with_tail, &mut inputs, |hash| {
                let mut locator = b"new/".to_vec();
                locator.extend_from_slice(&hash.0[..20]);
                Locator::from_seed(&locator)
            }) {
                Ok(value) => break value,
                Err(IvmRuntimeError::EvaluationBlocked) => {
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
        let fresh = prepare_with_locator(LargeValueKind::Bytes, &final_bytes, |hash| {
            let mut locator = b"fresh/".to_vec();
            locator.extend_from_slice(&hash.0[..20]);
            Locator::from_seed(&locator)
        })
        .unwrap();

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
        let prepared = prepare_with_locator(LargeValueKind::Bytes, &base, |hash| {
            let mut locator = b"old/".to_vec();
            locator.extend_from_slice(&hash.0[..20]);
            Locator::from_seed(&locator)
        })
        .unwrap();
        let offset = (base.len() / 2 + 137) as u64;
        let delete_length = 31_u64;
        let insert = b"localized replacement payload".to_vec();
        let mut no_reads = EvaluationInputs::default();
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
                        locator: chunk.node_ref.locator,
                    },
                    bytes::Bytes::copy_from_slice(&chunk.encoded),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut inputs = EvaluationInputs::default();
        let mut requested = std::collections::BTreeSet::new();
        let consolidated = loop {
            match consolidate_single_edit_attempt(&with_tail, &mut inputs, |hash| {
                let mut locator = b"new/".to_vec();
                locator.extend_from_slice(&hash.0[..20]);
                Locator::from_seed(&locator)
            }) {
                Ok(value) => break value,
                Err(IvmRuntimeError::EvaluationBlocked) => {
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
        let fresh = prepare_with_locator(LargeValueKind::Bytes, &expected, |hash| {
            let mut locator = b"fresh/".to_vec();
            locator.extend_from_slice(&hash.0[..20]);
            Locator::from_seed(&locator)
        })
        .unwrap();

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
        let prepared =
            prepare_with_locator(LargeValueKind::Bytes, &base, deterministic_locator).unwrap();
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
                        locator: chunk.node_ref.locator,
                    },
                    bytes::Bytes::copy_from_slice(&chunk.encoded),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut edit_inputs = EvaluationInputs::default();
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

        let mut inputs = EvaluationInputs::default();
        let consolidated = loop {
            match consolidate_single_edit_attempt(&with_tail, &mut inputs, deterministic_locator) {
                Ok(value) => break value,
                Err(IvmRuntimeError::EvaluationBlocked) => {
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

        let mut materialize_inputs = EvaluationInputs::default();
        materialize_inputs.install_chunk(
            ChunkRequest {
                object_hash: root.node_ref.object_hash.0,
                locator: root.node_ref.locator,
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
        let mut locator = deterministic_locator;

        let retained_node = ChunkNode::Leaf {
            format: FORMAT_VERSION,
            kind: LargeValueKind::Bytes,
            bytes: retained.clone(),
        };
        let retained_leaf = stage_node(
            retained_node.clone(),
            node_metrics(LargeValueKind::Bytes, &retained_node).unwrap(),
            &mut locator,
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
            &mut locator,
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
            &mut locator,
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
            &mut locator,
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
                        locator: chunk.node_ref.locator,
                    },
                    bytes::Bytes::copy_from_slice(&chunk.encoded),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut edit_inputs = EvaluationInputs::default();
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

        let mut inputs = EvaluationInputs::default();
        let consolidated = loop {
            match consolidate_single_edit_attempt(&with_tail, &mut inputs, deterministic_locator) {
                Ok(value) => break value,
                Err(IvmRuntimeError::EvaluationBlocked) => {
                    for request in inputs.take_missing_chunks() {
                        inputs.install_chunk(request.clone(), available[&request].clone());
                    }
                }
                Err(error) => panic!("unexpected suffix-deletion failure: {error}"),
            }
        };
        let fresh =
            prepare_with_locator(LargeValueKind::Bytes, &retained, deterministic_locator).unwrap();
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

        let mut materialize_inputs = EvaluationInputs::default();
        for chunk in &prepared.staged_chunks {
            materialize_inputs.install_chunk(
                ChunkRequest {
                    object_hash: chunk.node_ref.object_hash.0,
                    locator: chunk.node_ref.locator,
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
        let prepared =
            prepare_with_locator(LargeValueKind::Bytes, &base, deterministic_locator).unwrap();
        let available = prepared
            .staged_chunks
            .iter()
            .map(|chunk| {
                (
                    ChunkRequest {
                        object_hash: chunk.node_ref.object_hash.0,
                        locator: chunk.node_ref.locator,
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
            let mut edit_inputs = EvaluationInputs::default();
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
            let mut inputs = EvaluationInputs::default();
            let consolidated = loop {
                match consolidate_single_edit_attempt(
                    &with_tail,
                    &mut inputs,
                    deterministic_locator,
                ) {
                    Ok(value) => break value,
                    Err(IvmRuntimeError::EvaluationBlocked) => {
                        for request in inputs.take_missing_chunks() {
                            inputs.install_chunk(request.clone(), available[&request].clone());
                        }
                    }
                    Err(error) => panic!("case {case} failed: {error}"),
                }
            };
            let mut expected = base.clone();
            expected.splice(offset..offset + delete, insert);
            let fresh =
                prepare_with_locator(LargeValueKind::Bytes, &expected, deterministic_locator)
                    .unwrap();
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
        let prepared =
            prepare_with_locator(LargeValueKind::Bytes, &base, deterministic_locator).unwrap();
        let available = prepared
            .staged_chunks
            .iter()
            .map(|chunk| {
                (
                    ChunkRequest {
                        object_hash: chunk.node_ref.object_hash.0,
                        locator: chunk.node_ref.locator,
                    },
                    bytes::Bytes::copy_from_slice(&chunk.encoded),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut logical = base;
        let mut with_tail = prepared.value_ref;
        let mut no_reads = EvaluationInputs::default();
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
        let mut inputs = EvaluationInputs::default();
        let mut max_completed_before_block = 0_usize;
        let consolidated = loop {
            match continuation.step(&mut inputs) {
                Ok(Some(value)) => break value,
                Ok(None) => unreachable!(),
                Err(IvmRuntimeError::EvaluationBlocked) => {
                    max_completed_before_block =
                        max_completed_before_block.max(continuation.next_edit);
                    for request in inputs.take_missing_chunks() {
                        inputs.install_chunk(request.clone(), available[&request].clone());
                    }
                }
                Err(error) => panic!("unexpected continuation failure: {error}"),
            }
        };
        let fresh =
            prepare_with_locator(LargeValueKind::Bytes, &logical, deterministic_locator).unwrap();

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
    fn locator_changes_do_not_change_logical_identity_or_tree_shape() {
        let bytes = vec![42; LEAF_MAX_BYTES * 3];
        let first =
            prepare_with_locator(LargeValueKind::Bytes, &bytes, deterministic_locator).unwrap();
        let second = prepare_with_locator(LargeValueKind::Bytes, &bytes, |hash| {
            let mut locator = hash.0;
            locator[0] ^= 0xff;
            Locator::from_seed(&locator)
        })
        .unwrap();

        assert_eq!(first.value_ref.logical_hash, second.value_ref.logical_hash);
        assert_eq!(first.value_ref.byte_length, second.value_ref.byte_length);
        assert_eq!(first.staged_chunks.len(), second.staged_chunks.len());
        assert_ne!(first.value_ref.root, second.value_ref.root);
        let first_shapes = first
            .staged_chunks
            .iter()
            .map(|chunk| {
                match decode_node(
                    LargeValueKind::Bytes,
                    chunk.node_ref.object_hash,
                    &chunk.encoded,
                )
                .unwrap()
                {
                    ChunkNode::Leaf { bytes, .. } => (0, bytes.len()),
                    ChunkNode::Branch { children, .. } => (1, children.len()),
                }
            })
            .collect::<Vec<_>>();
        let second_shapes = second
            .staged_chunks
            .iter()
            .map(|chunk| {
                match decode_node(
                    LargeValueKind::Bytes,
                    chunk.node_ref.object_hash,
                    &chunk.encoded,
                )
                .unwrap()
                {
                    ChunkNode::Leaf { bytes, .. } => (0, bytes.len()),
                    ChunkNode::Branch { children, .. } => (1, children.len()),
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(first_shapes, second_shapes);
    }

    #[test]
    fn object_hash_authenticates_locator_bearing_branch_bytes() {
        let bytes = vec![9; LEAF_MAX_BYTES * 2];
        let prepared =
            prepare_with_locator(LargeValueKind::Bytes, &bytes, deterministic_locator).unwrap();
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
        let prepared =
            prepare_with_locator(LargeValueKind::Bytes, b"canonical", deterministic_locator)
                .unwrap();
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
        let mut inputs = EvaluationInputs::default();
        inputs.install_chunk(
            ChunkRequest {
                object_hash: appended_hash.0,
                locator: forged.root.locator,
            },
            bytes::Bytes::from(encoded),
        );
        assert!(matches!(
            materialize_attempt(&forged, &mut inputs),
            Err(IvmRuntimeError::LargeValue(Error::DescriptorMismatch))
        ));
    }

    #[test]
    fn branch_decode_rejects_unused_bytes_under_a_recomputed_hash() {
        let prepared = prepare_with_locator(
            LargeValueKind::Bytes,
            &vec![7; LEAF_MAX_BYTES * 2],
            deterministic_locator,
        )
        .unwrap();
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

    #[test]
    fn oversized_branch_fanout_is_rejected_before_metadata_decode() {
        let child = BranchChild {
            node_ref: NodeRef {
                object_hash: ContentHash([1; 32]),
                locator: Locator([2; 32]),
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
            preflight_node_bounds(&encoded, &chunk_node_schema()),
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
    fn staged_batch_rejects_malformed_standard_enum_nodes_before_publication() {
        let prepared =
            prepare_with_locator(LargeValueKind::Bytes, b"canonical", deterministic_locator)
                .unwrap();
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
        let prepared =
            prepare_with_locator(LargeValueKind::Json, source, deterministic_locator).unwrap();
        assert_ne!(prepared.value_ref.logical_hash, ContentHash([0; 32]));
        assert_eq!(
            prepare_with_locator(LargeValueKind::Json, b"{broken", deterministic_locator),
            Err(Error::InvalidJson)
        );
    }

    #[test]
    fn materialization_discovers_authenticated_tree_in_request_rounds() {
        let logical = (0..(LEAF_MAX_BYTES * 3))
            .map(|index| (index.wrapping_mul(31) & 0xff) as u8)
            .collect::<Vec<_>>();
        let prepared =
            prepare_with_locator(LargeValueKind::Bytes, &logical, deterministic_locator).unwrap();
        let available = prepared
            .staged_chunks
            .iter()
            .map(|chunk| {
                (
                    ChunkRequest {
                        object_hash: chunk.node_ref.object_hash.0,
                        locator: chunk.node_ref.locator,
                    },
                    bytes::Bytes::copy_from_slice(&chunk.encoded),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut inputs = EvaluationInputs::default();
        let mut rounds = 0;
        loop {
            match materialize_attempt(&prepared.value_ref, &mut inputs) {
                Ok(materialized) => {
                    assert_eq!(materialized, logical);
                    break;
                }
                Err(IvmRuntimeError::EvaluationBlocked) => {
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
    fn reconstruction_reuses_exact_locators_for_unchanged_nodes() {
        let original = (0..(LEAF_MAX_BYTES * 12))
            .map(|index| (index.wrapping_mul(17) & 0xff) as u8)
            .collect::<Vec<_>>();
        let first = prepare_with_locator(LargeValueKind::Bytes, &original, |hash| {
            let mut locator = hash.0;
            locator[0] ^= 0x55;
            Locator::from_seed(&locator)
        })
        .unwrap();
        let mut edited = original;
        edited.splice(LEAF_MAX_BYTES * 6..LEAF_MAX_BYTES * 6, [1, 2, 3, 4]);
        let second = prepare_reusing(LargeValueKind::Bytes, &edited, &first.staged_chunks).unwrap();

        let old = first
            .staged_chunks
            .iter()
            .map(|chunk| (&chunk.node_ref.object_hash, &chunk.node_ref.locator))
            .collect::<std::collections::BTreeMap<_, _>>();
        let reused = second
            .staged_chunks
            .iter()
            .filter(|chunk| {
                old.get(&chunk.node_ref.object_hash)
                    .is_some_and(|locator| **locator == chunk.node_ref.locator)
            })
            .count();
        assert!(reused > 0, "the edit must preserve unaffected nodes");
        for chunk in &second.staged_chunks {
            if let Some(locator) = old.get(&chunk.node_ref.object_hash) {
                assert_eq!(
                    **locator, chunk.node_ref.locator,
                    "a byte-identical node must retain its exact locator"
                );
            }
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

    #[test]
    fn materializing_inline_projected_fields_does_not_reencode_the_current_row() {
        // Point reads in policy evaluation and projected subscriptions use this
        // path for each current row. Inline scalar values cannot need chunk
        // resolution, so rebuilding the surrounding record would only replay
        // its scalar codec once per historical update.
        let descriptor = RecordDescriptor::new([
            ("title", ValueType::String),
            ("body", ValueType::String),
            ("revision", ValueType::U64),
        ]);
        let raw = descriptor
            .create(&[
                Value::String("current title".to_owned()),
                Value::String("current body".to_owned()),
                Value::U64(500),
            ])
            .unwrap();
        let mut inputs = EvaluationInputs::default();

        reset_stored_scalar_encode_calls();
        let materialized =
            materialize_record_fields_attempt(&descriptor, &raw, &[0], &mut inputs).unwrap();

        assert_eq!(materialized, raw);
        assert_eq!(
            stored_scalar_encode_calls(),
            0,
            "an already-inline current row must pass through without scalar re-encoding"
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
            assert!(matches!(generic, Value::Enum(ref value) if value.tag() == 2));
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
        // The exact v12 generic enum used tag 0.
        let legacy_primitive = encode_v12_primitive_bytes(b"abc");
        assert_eq!(legacy_primitive[0], 0);
        assert_eq!(
            decode_stored_scalar(LargeValueKind::Bytes, &legacy_primitive),
            Err(Error::MalformedScalar)
        );
        assert_eq!(
            inline_scalar_bytes(LargeValueKind::Bytes, &legacy_primitive),
            Err(Error::MalformedScalar)
        );

        // The v12 generic Chunked case used tag 1 and the same ordinary record
        // payload.
        let prepared = prepare_with_locator(
            LargeValueKind::Bytes,
            &vec![7; LEAF_MAX_BYTES + 1],
            deterministic_locator,
        )
        .unwrap();
        let legacy_chunked = encode_v12_chunked_scalar(&prepared.value_ref);
        assert_eq!(legacy_chunked[0], 1);
        assert_eq!(
            decode_stored_scalar(LargeValueKind::Bytes, &legacy_chunked),
            Err(Error::MalformedScalar)
        );
        assert_eq!(
            inline_scalar_bytes(LargeValueKind::Bytes, &legacy_chunked),
            Err(Error::MalformedScalar)
        );
    }

    #[test]
    fn stored_scalar_chunked_round_trips_through_generic_enum_records_and_tail() {
        for (kind, logical) in [
            (LargeValueKind::Bytes, b"bytes root".as_slice()),
            (LargeValueKind::String, "text root 🙂".as_bytes()),
            (LargeValueKind::Json, br#"{"title":"json root"}"#.as_slice()),
        ] {
            let prepared = prepare_with_locator(kind, logical, deterministic_locator).unwrap();
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
            assert!(matches!(generic, Value::Enum(ref value) if value.tag() == 3));
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
        let prepared =
            prepare_with_locator(LargeValueKind::Bytes, logical, deterministic_locator).unwrap();
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

            let mut inputs = EvaluationInputs::default();
            inputs.install_chunk(
                ChunkRequest {
                    object_hash: root.node_ref.object_hash.0,
                    locator: root.node_ref.locator,
                },
                bytes::Bytes::copy_from_slice(&root.encoded),
            );
            assert!(matches!(
                materialize_attempt(&forged, &mut inputs),
                Err(IvmRuntimeError::LargeValue(Error::DescriptorMismatch))
            ));
        }
    }

    #[test]
    fn every_node_kind_witness_rejects_multi_leaf_replay() {
        // Forge a target-kind branch root with correct target metrics so the
        // traversal reaches an original bytes child. The child witness, not
        // merely the descriptor/root witness, must stop the replay.
        let logical = format!(r#"{{"body":"{}"}}"#, "x".repeat(LEAF_MAX_BYTES * 3));
        let prepared = prepare_with_locator(
            LargeValueKind::Bytes,
            logical.as_bytes(),
            deterministic_locator,
        )
        .unwrap();
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
            locator: original_root.node_ref.locator,
        };
        let mut forged = prepared.value_ref.clone();
        forged.kind = LargeValueKind::Json;
        forged.root = forged_root_ref.clone();
        forged.logical_hash = node_logical_hash(&forged_root);
        forged.utf16_length = Some(logical.encode_utf16().count() as u64);

        let mut inputs = EvaluationInputs::default();
        let mut supplied_original_child = false;
        loop {
            match materialize_attempt(&forged, &mut inputs) {
                Err(IvmRuntimeError::EvaluationBlocked) => {
                    for request in inputs.take_missing_chunks() {
                        if request.object_hash == forged_root_ref.object_hash.0
                            && request.locator == forged_root_ref.locator
                        {
                            inputs.install_chunk(
                                request,
                                bytes::Bytes::copy_from_slice(&forged_root_encoded),
                            );
                            continue;
                        }
                        let chunk = prepared
                            .staged_chunks
                            .iter()
                            .find(|chunk| {
                                chunk.node_ref.object_hash.0 == request.object_hash
                                    && chunk.node_ref.locator == request.locator
                            })
                            .expect("forged root may reveal only original children");
                        supplied_original_child = true;
                        inputs
                            .install_chunk(request, bytes::Bytes::copy_from_slice(&chunk.encoded));
                    }
                }
                Err(IvmRuntimeError::LargeValue(Error::DescriptorMismatch)) => break,
                result => panic!("unexpected replay result: {result:?}"),
            }
        }
        assert!(
            supplied_original_child,
            "the forged branch must pass before a child witness rejects replay"
        );
    }

    #[test]
    fn candidate_format_one_nodes_fail_closed() {
        // This local type is the exact candidate format-1 leaf shape. Keeping
        // the receipt independent of the current enum ensures a future serde
        // layout change cannot accidentally turn old content into a v2 node.
        #[derive(Serialize)]
        enum CandidateFormatOneNode {
            Leaf { format: u8, bytes: Vec<u8> },
        }

        for bytes in [
            Vec::new(),
            b"plain utf8".to_vec(),
            br#"{"valid":"json"}"#.to_vec(),
            vec![0, 1, 2, 3],
        ] {
            let encoded =
                postcard::to_allocvec(&CandidateFormatOneNode::Leaf { format: 1, bytes }).unwrap();
            let hash = object_hash(&encoded);
            for expected_kind in [
                LargeValueKind::Bytes,
                LargeValueKind::String,
                LargeValueKind::Json,
            ] {
                assert!(
                    decode_node(expected_kind, hash, &encoded).is_err(),
                    "format-1 leaf must fail closed as {expected_kind:?}"
                );
            }
        }
    }

    #[test]
    fn same_kind_prepare_reuse_and_append_consolidation_remain_deterministic() {
        // Internal construction coverage is necessary because deterministic
        // object/logical identity and locator reuse are representation
        // invariants intentionally hidden from public queries.
        let logical = (0..LEAF_MAX_BYTES * 5)
            .map(|index| (index.wrapping_mul(37) & 0xff) as u8)
            .collect::<Vec<_>>();
        let first =
            prepare_with_locator(LargeValueKind::Bytes, &logical, deterministic_locator).unwrap();
        let second =
            prepare_with_locator(LargeValueKind::Bytes, &logical, deterministic_locator).unwrap();
        assert_eq!(first, second);

        let reused =
            prepare_reusing(LargeValueKind::Bytes, &logical, &first.staged_chunks).unwrap();
        assert_eq!(
            reused, first,
            "same-kind reconstruction must retain every exact node and locator"
        );

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
                        locator: chunk.node_ref.locator,
                    },
                    bytes::Bytes::copy_from_slice(&chunk.encoded),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let consolidate = || {
            let mut inputs = EvaluationInputs::default();
            loop {
                match consolidate_appends_attempt(&with_tail, &mut inputs, deterministic_locator) {
                    Ok(prepared) => break prepared,
                    Err(IvmRuntimeError::EvaluationBlocked) => {
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
        let fresh =
            prepare_with_locator(LargeValueKind::Bytes, &expected, deterministic_locator).unwrap();
        assert_eq!(consolidated.value_ref, fresh.value_ref);

        let shared = br#"{"same":"valid bytes, text, and json"}"#;
        let cross_kind_hashes = [
            LargeValueKind::Bytes,
            LargeValueKind::String,
            LargeValueKind::Json,
        ]
        .map(|kind| {
            prepare_with_locator(kind, shared, deterministic_locator)
                .unwrap()
                .value_ref
                .logical_hash
        });
        assert_ne!(cross_kind_hashes[0], cross_kind_hashes[1]);
        assert_ne!(cross_kind_hashes[0], cross_kind_hashes[2]);
        assert_ne!(cross_kind_hashes[1], cross_kind_hashes[2]);

        let current_node = ChunkNode::Leaf {
            format: FORMAT_VERSION,
            kind: LargeValueKind::Bytes,
            bytes: shared.to_vec(),
        };
        let candidate_old_node = ChunkNode::Leaf {
            format: 1,
            kind: LargeValueKind::Bytes,
            bytes: shared.to_vec(),
        };
        assert_ne!(
            node_logical_hash(&current_node),
            node_logical_hash(&candidate_old_node),
            "node format must participate in locator-independent identity"
        );
    }

    #[test]
    fn schema_derived_stored_scalar_kind_rejects_a_mismatched_descriptor() {
        let json = prepare_with_locator(
            LargeValueKind::Json,
            br#"{"same":"bytes"}"#,
            deterministic_locator,
        )
        .unwrap()
        .value_ref;
        let text_cell =
            RecordDescriptor::new([("cell", physical_storage_value_type(LargeValueKind::String))]);
        assert!(text_cell.create(&[Value::Large(json)]).is_err());
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
        let first =
            prepare_with_locator(LargeValueKind::Bytes, &original, deterministic_locator).unwrap();
        let mut edited = original;
        edited.splice(
            edited.len() / 2..edited.len() / 2,
            b"localized edit".iter().copied(),
        );
        let second =
            prepare_with_locator(LargeValueKind::Bytes, &edited, deterministic_locator).unwrap();
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
        let prepared =
            prepare_with_locator(LargeValueKind::Bytes, &base, deterministic_locator).unwrap();
        let available = prepared
            .staged_chunks
            .iter()
            .map(|chunk| {
                (
                    ChunkRequest {
                        object_hash: chunk.node_ref.object_hash.0,
                        locator: chunk.node_ref.locator,
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
        let mut inputs = EvaluationInputs::default();
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
                Err(IvmRuntimeError::EvaluationBlocked) => {
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
                let pieces =
                    map_final_range_to_base(range.start as u64..range.end as u64, &edits).unwrap();
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
