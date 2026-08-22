//! Generic binary storage used by large built-in values.
//!
//! The module deliberately owns no row identity, history, policy, or sync
//! state. An [`LargeValue`] is one ordinary atomic cell whose large arm
//! references immutable, domain-scoped objects.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use groove::records::Value as GrooveValue;
use groove::storage::OrderedKvStorage;

// Stable alpha-format domains retain their original bytes across terminology changes.
const CONTENT_ID_DOMAIN: &[u8] = b"jazz-adaptive-content-v1";
const OBJECT_FORMAT_VERSION: u8 = 2;
const CELL_ENVELOPE: &[u8] = b"JAZZ-LARGE-VALUE-V3\0";
const PATCH_FRAME_HEADER_BYTES: usize = 5 * std::mem::size_of::<u64>() + 1;
/// Physical column family containing domain-scoped immutable content objects.
pub const CONTENT_OBJECTS_CF: &str = "jazz_content_objects";

/// Stable identifier of one immutable content object.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ContentId([u8; 32]);

impl ContentId {
    /// Return the identifier bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Authorization and encryption domain included in every object identity.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ContentDomain(Vec<u8>);

impl ContentDomain {
    /// Construct a non-empty domain identifier.
    pub fn new(bytes: impl Into<Vec<u8>>) -> Result<Self, ContentError> {
        let bytes = bytes.into();
        if bytes.is_empty() {
            return Err(ContentError::EmptyDomain);
        }
        Ok(Self(bytes))
    }

    fn bytes(&self) -> &[u8] {
        &self.0
    }
}

/// One child in an immutable branch node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChildDescriptor {
    /// Child object identity.
    pub id: ContentId,
    /// Exact materialized byte length below the child.
    pub byte_len: u64,
    /// Exact UTF-16 code-unit length for text trees.
    pub utf16_len: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ContentObject {
    Leaf {
        bytes: Vec<u8>,
        utf16_len: Option<u64>,
    },
    Branch(Vec<ChildDescriptor>),
}

/// Immutable content-addressed object storage.
pub trait ImmutableContentStore {
    /// Load canonical object bytes.
    fn get(&self, id: ContentId) -> Result<Option<Vec<u8>>, ContentError>;

    /// Insert an absent object or verify byte identity with the existing one.
    fn put_if_absent_or_identical(
        &mut self,
        id: ContentId,
        canonical_bytes: &[u8],
    ) -> Result<(), ContentError>;
}

/// Small deterministic store useful for embedded runtimes and tests.
#[derive(Clone, Debug, Default)]
pub struct MemoryContentStore {
    objects: BTreeMap<ContentId, Vec<u8>>,
}

impl MemoryContentStore {
    /// Number of immutable objects currently retained.
    pub fn len(&self) -> usize {
        self.objects.len()
    }

    /// Whether the store is empty.
    pub fn is_empty(&self) -> bool {
        self.objects.is_empty()
    }
}

impl ImmutableContentStore for MemoryContentStore {
    fn get(&self, id: ContentId) -> Result<Option<Vec<u8>>, ContentError> {
        Ok(self.objects.get(&id).cloned())
    }

    fn put_if_absent_or_identical(
        &mut self,
        id: ContentId,
        canonical_bytes: &[u8],
    ) -> Result<(), ContentError> {
        match self.objects.get(&id) {
            Some(existing) if existing != canonical_bytes => {
                Err(ContentError::ImmutableCollision(id))
            }
            Some(_) => Ok(()),
            None => {
                self.objects.insert(id, canonical_bytes.to_vec());
                Ok(())
            }
        }
    }
}

/// Adapter over Jazz's ordinary ordered key/value storage.
///
/// This adapter verifies absent-or-identical behavior within one serialized
/// writer or storage transaction. It does not manufacture compare-and-set
/// semantics: integrations with concurrent writers must use a transaction
/// conflict boundary that covers this column family.
pub struct KvContentStore<'a, S> {
    storage: &'a S,
}

impl<'a, S> KvContentStore<'a, S> {
    /// Wrap one storage or storage transaction.
    pub fn new(storage: &'a S) -> Self {
        Self { storage }
    }
}

impl<S: OrderedKvStorage> ImmutableContentStore for KvContentStore<'_, S> {
    fn get(&self, id: ContentId) -> Result<Option<Vec<u8>>, ContentError> {
        self.storage
            .get(CONTENT_OBJECTS_CF, id.as_bytes())
            .map_err(|error| ContentError::Storage(error.to_string()))
    }

    fn put_if_absent_or_identical(
        &mut self,
        id: ContentId,
        canonical_bytes: &[u8],
    ) -> Result<(), ContentError> {
        match self.get(id)? {
            Some(existing) if existing != canonical_bytes => {
                Err(ContentError::ImmutableCollision(id))
            }
            Some(_) => Ok(()),
            None => self
                .storage
                .set(CONTENT_OBJECTS_CF, id.as_bytes(), canonical_bytes)
                .map_err(|error| ContentError::Storage(error.to_string())),
        }
    }
}

/// Versioned recursive content-defined chunking parameters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChunkingProfile {
    /// Minimum leaf bytes before a content boundary is eligible.
    pub min_leaf_bytes: usize,
    /// Target leaf bytes; must be a power of two.
    pub target_leaf_bytes: usize,
    /// Hard maximum leaf bytes.
    pub max_leaf_bytes: usize,
    /// Minimum child descriptors before an internal boundary is eligible.
    pub min_children: usize,
    /// Target children per branch; must be a power of two.
    pub target_children: usize,
    /// Hard maximum children per branch.
    pub max_children: usize,
}

impl Default for ChunkingProfile {
    fn default() -> Self {
        Self {
            min_leaf_bytes: 4 * 1024,
            target_leaf_bytes: 16 * 1024,
            max_leaf_bytes: 64 * 1024,
            min_children: 16,
            target_children: 64,
            max_children: 128,
        }
    }
}

impl ChunkingProfile {
    /// Validate format bounds.
    pub fn validate(self) -> Result<Self, ContentError> {
        let leaf_ok = self.min_leaf_bytes > 0
            && self.min_leaf_bytes <= self.target_leaf_bytes
            && self.target_leaf_bytes <= self.max_leaf_bytes
            && self.target_leaf_bytes.is_power_of_two();
        let branch_ok = self.min_children > 0
            && self.min_children <= self.target_children
            && self.target_children <= self.max_children
            && self.max_children >= 2
            && self.target_children.is_power_of_two();
        if !leaf_ok || !branch_ok {
            return Err(ContentError::InvalidChunkingProfile);
        }
        Ok(self)
    }
}

/// One ordered byte-range replacement.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BytePatch {
    /// Byte offset in the result of all preceding patches.
    pub offset: u64,
    /// Number of bytes to remove at `offset`.
    pub delete_len: u64,
    /// Replacement bytes inserted at `offset`.
    #[serde(with = "serde_bytes")]
    pub insert: Vec<u8>,
    /// UTF-16 effects when this patch belongs to a text value.
    pub text_metrics: Option<TextPatchMetrics>,
}

/// UTF-16 effects of one universal byte patch in a text edit tail.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TextPatchMetrics {
    /// UTF-16 code units removed by the patch.
    pub delete_len: u64,
    /// UTF-16 code units inserted by the patch.
    pub insert_len: u64,
}

impl BytePatch {
    /// Insert bytes without deleting existing content.
    pub fn insert(offset: u64, bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            offset,
            delete_len: 0,
            insert: bytes.into(),
            text_metrics: None,
        }
    }

    /// Delete one byte range.
    pub fn delete(offset: u64, delete_len: u64) -> Self {
        Self {
            offset,
            delete_len,
            insert: Vec::new(),
            text_metrics: None,
        }
    }

    /// Replace one byte range.
    pub fn replace(offset: u64, delete_len: u64, bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            offset,
            delete_len,
            insert: bytes.into(),
            text_metrics: None,
        }
    }

    fn with_text_metrics(mut self, delete_len: u64, insert_len: u64) -> Self {
        self.text_metrics = Some(TextPatchMetrics {
            delete_len,
            insert_len,
        });
        self
    }
}

/// Bounds for the ordered inline patch tail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TailBounds {
    /// Maximum operation count.
    pub max_entries: usize,
    /// Maximum canonical encoded bytes across all operations.
    pub max_encoded_bytes: usize,
}

impl Default for TailBounds {
    fn default() -> Self {
        Self {
            max_entries: 64,
            max_encoded_bytes: 16 * 1024,
        }
    }
}

/// Chunked physical arm of a large-value cell.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkedValue {
    /// Root of the immutable recursive byte tree.
    pub root: ContentId,
    /// Materialized root length before applying the tail.
    pub root_byte_len: u64,
    /// Materialized UTF-16 length before the tail for text values.
    pub root_utf16_len: Option<u64>,
    /// Ordered byte replacements.
    pub edit_tail: Vec<BytePatch>,
}

/// One ordinary value cell with transparent inline/chunked representation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LargeValue {
    /// Direct bytes for small values.
    Inline(#[serde(with = "serde_bytes")] Vec<u8>),
    /// Immutable byte tree plus a bounded ordered patch tail.
    Chunked(ChunkedValue),
}

/// Built-in semantic interpretation of large-value bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValueKind {
    /// Uninterpreted bytes.
    Bytes,
    /// UTF-8 text.
    String,
    /// UTF-8 JSON source.
    Json,
}

/// Schema-stable storage policy for one built-in large value column.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LargeValueSchema {
    /// Logical interpretation retained at every public/query boundary.
    pub kind: ValueKind,
    /// Largest newly authored value that remains inline.
    pub inline_up_to: u32,
    /// Maximum ordered patch count before synchronous consolidation.
    pub max_tail_entries: u16,
    /// Maximum canonical patch bytes before synchronous consolidation.
    pub max_tail_bytes: u32,
    /// Version of the immutable tree and chunking profile.
    pub tree_format: u16,
}

impl LargeValueSchema {
    /// Built-in alpha defaults for one logical kind.
    pub fn built_in(kind: ValueKind) -> Self {
        Self {
            kind,
            inline_up_to: 8 * 1024,
            max_tail_entries: 64,
            max_tail_bytes: 16 * 1024,
            tree_format: u16::from(OBJECT_FORMAT_VERSION),
        }
    }

    /// Runtime tail bounds declared by this schema.
    pub fn tail_bounds(&self) -> TailBounds {
        TailBounds {
            max_entries: usize::from(self.max_tail_entries),
            max_encoded_bytes: usize::try_from(self.max_tail_bytes)
                .expect("u32 tail bound fits usize on supported targets"),
        }
    }

    /// Validate that this runtime understands the declared physical format.
    pub fn validate(&self) -> Result<(), ContentError> {
        if self.tree_format != u16::from(OBJECT_FORMAT_VERSION) {
            return Err(ContentError::UnsupportedTreeFormat(self.tree_format));
        }
        if self.max_tail_entries == 0 || self.max_tail_bytes == 0 {
            return Err(ContentError::TailTooLarge);
        }
        Ok(())
    }
}

/// Immutable query projection over one large value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ValueSelection {
    /// Materialize the complete native value.
    Value,
    /// Return one absolute byte range.
    ByteRange {
        /// Inclusive byte start.
        offset: u64,
        /// Requested byte count.
        len: u64,
    },
    /// Return one UTF-8 byte range from text.
    TextUtf8Range {
        /// Inclusive UTF-8 byte start.
        offset: u64,
        /// Requested UTF-8 byte count.
        len: u64,
    },
    /// Return one UTF-16 code-unit range from text.
    TextUtf16Range {
        /// Inclusive UTF-16 start.
        offset: u64,
        /// Requested UTF-16 code-unit count.
        len: u64,
    },
    /// Return the detached JSON value selected by one RFC 6901 pointer.
    JsonPointer(String),
}

/// Native immutable result of a value query projection.
#[derive(Clone, Debug, PartialEq)]
pub enum ValueSelectionResult {
    /// Complete or ranged bytes.
    Bytes(Vec<u8>),
    /// Complete or ranged UTF-8 text.
    String(String),
    /// Complete or projected detached JSON.
    Json(serde_json::Value),
}

/// Declarative update authored against an ordinary immutable row snapshot.
#[derive(Clone, Debug, PartialEq)]
pub enum ValueEdit {
    /// Replace an absolute byte range.
    Bytes(BytePatch),
    /// Replace a UTF-8 range relative to a selected UTF-8 text slice.
    TextUtf8 {
        /// Absolute selected-slice start in UTF-8 bytes.
        slice_offset: u64,
        /// Selected-slice length in UTF-8 bytes.
        slice_len: u64,
        /// Relative UTF-8 offset within the slice.
        offset: u64,
        /// UTF-8 bytes to remove.
        delete_len: u64,
        /// Replacement text.
        insert: String,
    },
    /// Replace a UTF-16 range relative to a selected UTF-16 text slice.
    TextUtf16 {
        /// Absolute selected-slice start in UTF-16 code units.
        slice_offset: u64,
        /// Selected-slice length in UTF-16 code units.
        slice_len: u64,
        /// Relative UTF-16 offset within the slice.
        offset: u64,
        /// UTF-16 code units to remove.
        delete_len: u64,
        /// Replacement text.
        insert: String,
    },
    /// Append bytes to a stream or byte value.
    Append(Vec<u8>),
    /// Replace JSON with one arbitrary native value.
    Json(serde_json::Value),
}

/// Origin of one semantic JSON change in a three-way merge.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JsonSide {
    /// First candidate.
    A,
    /// Second candidate.
    B,
}

/// One semantic JSON replacement labelled by its authoring side.
#[derive(Clone, Debug, PartialEq)]
pub struct AttributedJsonChange {
    /// Candidate that authored the change.
    pub side: JsonSide,
    /// RFC 6901 pointer. The empty pointer denotes the document root.
    pub pointer: String,
    /// Base value, or absence for an insertion.
    pub before: Option<serde_json::Value>,
    /// Candidate value, or absence for a deletion.
    pub after: Option<serde_json::Value>,
}

/// Conservative semantic analysis delivered to a JSON merge strategy.
#[derive(Clone, Debug, PartialEq)]
pub struct JsonMergeAnalysis {
    /// Changes labelled by their authoring side whose paths do not overlap incompatibly.
    pub independent: Vec<AttributedJsonChange>,
    /// Pairs of overlapping or disagreeing candidate changes.
    pub conflicts: Vec<(AttributedJsonChange, AttributedJsonChange)>,
}

/// Parse base and candidate JSON bytes and compute a conservative semantic,
/// three-way change analysis labelled by authoring side.
pub fn analyze_json_merge(
    base: &[u8],
    side_a: &[u8],
    side_b: &[u8],
) -> Result<JsonMergeAnalysis, ContentError> {
    let base = parse_json(base)?;
    let side_a = parse_json(side_a)?;
    let side_b = parse_json(side_b)?;
    let mut changes_a = Vec::new();
    let mut changes_b = Vec::new();
    collect_json_changes("", &base, &side_a, JsonSide::A, &mut changes_a);
    collect_json_changes("", &base, &side_b, JsonSide::B, &mut changes_b);

    let mut conflicts = Vec::new();
    let mut conflicted_a = vec![false; changes_a.len()];
    let mut conflicted_b = vec![false; changes_b.len()];
    for (a_index, a) in changes_a.iter().enumerate() {
        for (b_index, b) in changes_b.iter().enumerate() {
            if json_paths_overlap(&a.pointer, &b.pointer) && a.after != b.after {
                conflicted_a[a_index] = true;
                conflicted_b[b_index] = true;
                conflicts.push((a.clone(), b.clone()));
            }
        }
    }
    let independent = changes_a
        .into_iter()
        .zip(conflicted_a)
        .chain(changes_b.into_iter().zip(conflicted_b))
        .filter_map(|(change, conflicted)| (!conflicted).then_some(change))
        .collect();
    Ok(JsonMergeAnalysis {
        independent,
        conflicts,
    })
}

fn parse_json(bytes: &[u8]) -> Result<serde_json::Value, ContentError> {
    serde_json::from_slice(bytes).map_err(|error| ContentError::InvalidJson(error.to_string()))
}

fn collect_json_changes(
    pointer: &str,
    before: &serde_json::Value,
    after: &serde_json::Value,
    side: JsonSide,
    out: &mut Vec<AttributedJsonChange>,
) {
    if before == after {
        return;
    }
    match (before, after) {
        (serde_json::Value::Object(before), serde_json::Value::Object(after)) => {
            let keys = before
                .keys()
                .chain(after.keys())
                .collect::<std::collections::BTreeSet<_>>();
            for key in keys {
                let child = format!("{pointer}/{}", escape_json_pointer(key));
                match (before.get(key), after.get(key)) {
                    (Some(before), Some(after)) => {
                        collect_json_changes(&child, before, after, side, out)
                    }
                    (before, after) => out.push(AttributedJsonChange {
                        side,
                        pointer: child,
                        before: before.cloned(),
                        after: after.cloned(),
                    }),
                }
            }
        }
        // Without persistent element identity, arrays are intentionally one
        // semantic replacement. Merge strategies may inspect both values and
        // resolve them, but core does not invent move/position intent.
        _ => out.push(AttributedJsonChange {
            side,
            pointer: pointer.to_owned(),
            before: Some(before.clone()),
            after: Some(after.clone()),
        }),
    }
}

fn escape_json_pointer(segment: &str) -> String {
    segment.replace('~', "~0").replace('/', "~1")
}

fn json_paths_overlap(left: &str, right: &str) -> bool {
    left == right
        || left.is_empty()
        || right.is_empty()
        || right
            .strip_prefix(left)
            .is_some_and(|suffix| suffix.starts_with('/'))
        || left
            .strip_prefix(right)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

impl ValueKind {
    /// Validate one complete materialized value.
    pub fn validate(self, bytes: &[u8]) -> Result<(), ContentError> {
        match self {
            Self::Bytes => Ok(()),
            Self::String => std::str::from_utf8(bytes)
                .map(|_| ())
                .map_err(|_| ContentError::InvalidUtf8),
            Self::Json => {
                let text = std::str::from_utf8(bytes).map_err(|_| ContentError::InvalidUtf8)?;
                serde_json::from_str::<serde_json::Value>(text)
                    .map(|_| ())
                    .map_err(|error| ContentError::InvalidJson(error.to_string()))
            }
        }
    }

    /// Convert one existing logical Groove value to its canonical source
    /// bytes before large-value physical encoding.
    pub fn logical_bytes(self, value: &GrooveValue) -> Result<Vec<u8>, ContentError> {
        match (self, value) {
            (Self::Bytes, GrooveValue::Bytes(bytes)) => Ok(bytes.clone()),
            (Self::String | Self::Json, GrooveValue::String(text)) => {
                let bytes = text.as_bytes().to_vec();
                self.validate(&bytes)?;
                Ok(bytes)
            }
            _ => Err(ContentError::LogicalTypeMismatch),
        }
    }

    /// Convert complete materialized bytes back to the existing logical Groove
    /// value consumed by queries, policies, indices, and bindings.
    pub fn logical_value(self, bytes: Vec<u8>) -> Result<GrooveValue, ContentError> {
        self.validate(&bytes)?;
        match self {
            Self::Bytes => Ok(GrooveValue::Bytes(bytes)),
            Self::String | Self::Json => String::from_utf8(bytes)
                .map(GrooveValue::String)
                .map_err(|_| ContentError::InvalidUtf8),
        }
    }
}

/// Materialization or immutable-integrity failure.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ContentError {
    /// Content domains cannot be empty.
    #[error("content domain cannot be empty")]
    EmptyDomain,
    /// Chunk parameters violate format bounds.
    #[error("invalid content chunking profile")]
    InvalidChunkingProfile,
    /// The schema declares an immutable-tree format this runtime cannot read.
    #[error("unsupported large-value content tree format {0}")]
    UnsupportedTreeFormat(u16),
    /// Large value cell envelope is unknown.
    #[error("unknown large value cell format")]
    UnknownCellFormat,
    /// Large value cell payload is malformed.
    #[error("malformed large value cell: {0}")]
    MalformedCell(String),
    /// Ordered key/value persistence failed.
    #[error("large-value content storage failed: {0}")]
    Storage(String),
    /// An immutable identity was observed with different bytes.
    #[error("immutable content collision for {0:?}")]
    ImmutableCollision(ContentId),
    /// A referenced immutable object is absent.
    #[error("missing immutable content object {0:?}")]
    MissingObject(ContentId),
    /// Canonical bytes do not hash to their referenced id.
    #[error("immutable object id mismatch for {0:?}")]
    ObjectIdMismatch(ContentId),
    /// Canonical object encoding is malformed.
    #[error("malformed immutable content object: {0}")]
    MalformedObject(String),
    /// A replacement range exceeds the current value.
    #[error("byte patch range {offset}..{end} exceeds value length {value_len}")]
    PatchOutOfBounds {
        /// Starting byte offset.
        offset: u64,
        /// Checked exclusive end.
        end: u64,
        /// Current value length.
        value_len: u64,
    },
    /// Integer conversion or aggregate length overflowed.
    #[error("content length overflow")]
    LengthOverflow,
    /// Patch tail exceeds its operation or byte bound.
    #[error("content edit tail exceeds configured bounds")]
    TailTooLarge,
    /// Text is not valid UTF-8.
    #[error("large-value string is not valid UTF-8")]
    InvalidUtf8,
    /// JSON source is invalid.
    #[error("large-value JSON is invalid: {0}")]
    InvalidJson(String),
    /// A query projection does not apply to the logical value kind.
    #[error("query selection does not apply to this value kind")]
    InvalidSelection,
    /// An update operation does not apply to the logical value kind.
    #[error("edit operation does not apply to this value kind")]
    InvalidEdit,
    /// A Groove value does not match the column's logical value kind.
    #[error("logical value does not match large-value kind")]
    LogicalTypeMismatch,
    /// A text position exceeds the selected coordinate length or splits an encoded character.
    #[error("text {encoding} offset {offset} is invalid for length {text_len}")]
    TextOffsetOutOfBounds {
        /// Coordinate encoding.
        encoding: &'static str,
        /// Requested offset.
        offset: u64,
        /// Current length in that encoding.
        text_len: u64,
    },
}

/// Apply ordered patches to complete base bytes.
pub fn apply_patches(base: &[u8], patches: &[BytePatch]) -> Result<Vec<u8>, ContentError> {
    let mut value = base.to_vec();
    for patch in patches {
        let start = usize::try_from(patch.offset).map_err(|_| ContentError::LengthOverflow)?;
        let delete = usize::try_from(patch.delete_len).map_err(|_| ContentError::LengthOverflow)?;
        let end = start
            .checked_add(delete)
            .ok_or(ContentError::LengthOverflow)?;
        if end > value.len() {
            return Err(ContentError::PatchOutOfBounds {
                offset: patch.offset,
                end: u64::try_from(end).unwrap_or(u64::MAX),
                value_len: u64::try_from(value.len()).unwrap_or(u64::MAX),
            });
        }
        value.splice(start..end, patch.insert.iter().copied());
    }
    Ok(value)
}

/// Construct and read recursive content-defined immutable byte trees.
#[derive(Clone, Copy, Debug)]
pub struct ContentTree {
    profile: ChunkingProfile,
}

impl ContentTree {
    /// Construct a tree implementation for one validated format profile.
    pub fn new(profile: ChunkingProfile) -> Result<Self, ContentError> {
        Ok(Self {
            profile: profile.validate()?,
        })
    }

    /// Persist bytes and return the deterministic root and length.
    pub fn build<S: ImmutableContentStore>(
        &self,
        domain: &ContentDomain,
        bytes: &[u8],
        store: &mut S,
    ) -> Result<(ContentId, u64), ContentError> {
        self.build_with_metrics(domain, bytes, false, store)
            .map(|descriptor| (descriptor.id, descriptor.byte_len))
    }

    /// Persist UTF-8 text with aggregate UTF-16 metrics on every tree edge.
    pub fn build_text<S: ImmutableContentStore>(
        &self,
        domain: &ContentDomain,
        text: &str,
        store: &mut S,
    ) -> Result<(ContentId, u64, u64), ContentError> {
        let descriptor = self.build_with_metrics(domain, text.as_bytes(), true, store)?;
        Ok((
            descriptor.id,
            descriptor.byte_len,
            descriptor.utf16_len.expect("text tree has UTF-16 metrics"),
        ))
    }

    fn build_with_metrics<S: ImmutableContentStore>(
        &self,
        domain: &ContentDomain,
        bytes: &[u8],
        text: bool,
        store: &mut S,
    ) -> Result<ChildDescriptor, ContentError> {
        let chunks = if text {
            text_leaf_ranges(
                std::str::from_utf8(bytes).map_err(|_| ContentError::InvalidUtf8)?,
                self.profile,
            )
        } else {
            leaf_ranges(bytes, self.profile)
        };
        let mut level = Vec::with_capacity(chunks.len());
        for range in chunks {
            let payload = bytes[range].to_vec();
            let utf16_len = text
                .then(|| {
                    count_utf16(std::str::from_utf8(&payload).expect("text chunks are aligned"))
                })
                .transpose()?;
            let object = ContentObject::Leaf {
                bytes: payload,
                utf16_len,
            };
            level.push(self.persist_object(domain, object, store)?);
        }
        if level.is_empty() {
            level.push(self.persist_object(
                domain,
                ContentObject::Leaf {
                    bytes: Vec::new(),
                    utf16_len: text.then_some(0),
                },
                store,
            )?);
        }
        while level.len() > 1 {
            let groups = descriptor_groups(&level, self.profile);
            let mut next = Vec::with_capacity(groups.len());
            for group in groups {
                next.push(self.persist_object(
                    domain,
                    ContentObject::Branch(level[group].to_vec()),
                    store,
                )?);
            }
            level = next;
        }
        Ok(level.pop().expect("tree always has one root"))
    }

    /// Persist a byte stream without retaining the complete logical value.
    ///
    /// Chunks are only transport buffers: boundaries are calculated over the
    /// continuous byte stream, so changing their size does not change the
    /// resulting root.  The builder retains one leaf and one incomplete branch
    /// per tree level, bounded by this tree's profile.
    pub fn build_streaming<S, I, B>(
        &self,
        domain: &ContentDomain,
        chunks: I,
        store: &mut S,
    ) -> Result<(ContentId, u64), ContentError>
    where
        S: ImmutableContentStore,
        I: IntoIterator<Item = B>,
        B: AsRef<[u8]>,
    {
        let mut builder = StreamingContentBuilder::new(*self, domain, store);
        for chunk in chunks {
            builder.push(chunk.as_ref())?;
        }
        builder.finish()
    }

    /// Materialize the complete bytes below one root.
    pub fn materialize<S: ImmutableContentStore>(
        &self,
        domain: &ContentDomain,
        root: ContentId,
        expected_len: u64,
        store: &S,
    ) -> Result<Vec<u8>, ContentError> {
        let expected_usize =
            usize::try_from(expected_len).map_err(|_| ContentError::LengthOverflow)?;
        let mut out = Vec::new();
        self.read_object_range(
            domain,
            root,
            0,
            expected_len,
            Some(expected_len),
            store,
            &mut out,
        )?;
        if out.len() != expected_usize {
            return Err(ContentError::MalformedObject(
                "root aggregate length does not match descriptor".to_owned(),
            ));
        }
        Ok(out)
    }

    /// Read one immutable byte range without loading unrelated leaves.
    pub fn read_range<S: ImmutableContentStore>(
        &self,
        domain: &ContentDomain,
        root: ContentId,
        root_len: u64,
        offset: u64,
        len: u64,
        store: &S,
    ) -> Result<Vec<u8>, ContentError> {
        let end = offset
            .checked_add(len)
            .ok_or(ContentError::LengthOverflow)?;
        if end > root_len {
            return Err(ContentError::PatchOutOfBounds {
                offset,
                end,
                value_len: root_len,
            });
        }
        usize::try_from(len).map_err(|_| ContentError::LengthOverflow)?;
        let mut out = Vec::new();
        self.read_object_range(domain, root, offset, end, Some(root_len), store, &mut out)?;
        Ok(out)
    }

    fn utf16_prefix<S: ImmutableContentStore>(
        &self,
        domain: &ContentDomain,
        root: ContentId,
        root_byte_len: u64,
        root_utf16_len: u64,
        byte_offset: u64,
        store: &S,
    ) -> Result<u64, ContentError> {
        self.utf16_prefix_object(
            domain,
            root,
            root_byte_len,
            root_utf16_len,
            byte_offset,
            store,
        )
    }

    fn utf16_prefix_object<S: ImmutableContentStore>(
        &self,
        domain: &ContentDomain,
        id: ContentId,
        expected_bytes: u64,
        expected_utf16: u64,
        byte_offset: u64,
        store: &S,
    ) -> Result<u64, ContentError> {
        if byte_offset > expected_bytes {
            return Err(ContentError::PatchOutOfBounds {
                offset: byte_offset,
                end: byte_offset,
                value_len: expected_bytes,
            });
        }
        let object = self.load_object(domain, id, store)?;
        let metrics = object_metrics(&object)?;
        if metrics != (expected_bytes, Some(expected_utf16)) {
            return Err(ContentError::MalformedObject(
                "text child aggregate metrics do not match descriptor".to_owned(),
            ));
        }
        match object {
            ContentObject::Leaf {
                bytes,
                utf16_len: Some(_),
            } => {
                let text = std::str::from_utf8(&bytes).map_err(|_| ContentError::InvalidUtf8)?;
                let boundary = utf8_boundary(text, byte_offset)?;
                count_utf16(&text[..boundary])
            }
            ContentObject::Branch(children) => {
                let mut bytes_seen = 0_u64;
                let mut utf16_seen = 0_u64;
                for child in children {
                    let child_end = bytes_seen
                        .checked_add(child.byte_len)
                        .ok_or(ContentError::LengthOverflow)?;
                    let child_utf16 = child.utf16_len.ok_or_else(|| {
                        ContentError::MalformedObject("text branch lacks UTF-16 metric".to_owned())
                    })?;
                    if byte_offset <= child_end {
                        return utf16_seen
                            .checked_add(self.utf16_prefix_object(
                                domain,
                                child.id,
                                child.byte_len,
                                child_utf16,
                                byte_offset - bytes_seen,
                                store,
                            )?)
                            .ok_or(ContentError::LengthOverflow);
                    }
                    bytes_seen = child_end;
                    utf16_seen = utf16_seen
                        .checked_add(child_utf16)
                        .ok_or(ContentError::LengthOverflow)?;
                }
                Ok(utf16_seen)
            }
            ContentObject::Leaf {
                utf16_len: None, ..
            } => Err(ContentError::MalformedObject(
                "text root references a byte leaf".to_owned(),
            )),
        }
    }

    fn byte_offset_for_utf16<S: ImmutableContentStore>(
        &self,
        domain: &ContentDomain,
        root: ContentId,
        root_byte_len: u64,
        root_utf16_len: u64,
        utf16_offset: u64,
        store: &S,
    ) -> Result<u64, ContentError> {
        if utf16_offset > root_utf16_len {
            return Err(ContentError::TextOffsetOutOfBounds {
                encoding: "UTF-16",
                offset: utf16_offset,
                text_len: root_utf16_len,
            });
        }
        let object = self.load_object(domain, root, store)?;
        if object_metrics(&object)? != (root_byte_len, Some(root_utf16_len)) {
            return Err(ContentError::MalformedObject(
                "text child aggregate metrics do not match descriptor".to_owned(),
            ));
        }
        match object {
            ContentObject::Leaf {
                bytes,
                utf16_len: Some(_),
            } => {
                let text = std::str::from_utf8(&bytes).map_err(|_| ContentError::InvalidUtf8)?;
                u64::try_from(utf16_to_byte_offset(text, utf16_offset)?)
                    .map_err(|_| ContentError::LengthOverflow)
            }
            ContentObject::Branch(children) => {
                let mut byte_base = 0_u64;
                let mut utf16_base = 0_u64;
                for child in children {
                    let child_utf16 = child.utf16_len.ok_or_else(|| {
                        ContentError::MalformedObject("text branch lacks UTF-16 metric".to_owned())
                    })?;
                    let child_end = utf16_base
                        .checked_add(child_utf16)
                        .ok_or(ContentError::LengthOverflow)?;
                    if utf16_offset <= child_end {
                        return byte_base
                            .checked_add(self.byte_offset_for_utf16(
                                domain,
                                child.id,
                                child.byte_len,
                                child_utf16,
                                utf16_offset - utf16_base,
                                store,
                            )?)
                            .ok_or(ContentError::LengthOverflow);
                    }
                    byte_base = byte_base
                        .checked_add(child.byte_len)
                        .ok_or(ContentError::LengthOverflow)?;
                    utf16_base = child_end;
                }
                Ok(byte_base)
            }
            ContentObject::Leaf {
                utf16_len: None, ..
            } => Err(ContentError::MalformedObject(
                "text root references a byte leaf".to_owned(),
            )),
        }
    }

    fn persist_object<S: ImmutableContentStore>(
        &self,
        domain: &ContentDomain,
        object: ContentObject,
        store: &mut S,
    ) -> Result<ChildDescriptor, ContentError> {
        let (byte_len, utf16_len) = object_metrics(&object)?;
        let canonical = encode_object(&object)?;
        let id = object_id(domain, &canonical);
        store.put_if_absent_or_identical(id, &canonical)?;
        Ok(ChildDescriptor {
            id,
            byte_len,
            utf16_len,
        })
    }

    fn load_object<S: ImmutableContentStore>(
        &self,
        domain: &ContentDomain,
        id: ContentId,
        store: &S,
    ) -> Result<ContentObject, ContentError> {
        let canonical = store.get(id)?.ok_or(ContentError::MissingObject(id))?;
        if object_id(domain, &canonical) != id {
            return Err(ContentError::ObjectIdMismatch(id));
        }
        decode_object(&canonical, self.profile)
    }

    fn read_object_range<S: ImmutableContentStore>(
        &self,
        domain: &ContentDomain,
        id: ContentId,
        start: u64,
        end: u64,
        expected_len: Option<u64>,
        store: &S,
        out: &mut Vec<u8>,
    ) -> Result<(), ContentError> {
        let object = self.load_object(domain, id, store)?;
        let (actual_len, _) = object_metrics(&object)?;
        if expected_len.is_some_and(|expected| expected != actual_len) {
            return Err(ContentError::MalformedObject(
                "child aggregate length does not match descriptor".to_owned(),
            ));
        }
        match object {
            ContentObject::Leaf { bytes, .. } => {
                let leaf_len =
                    u64::try_from(bytes.len()).map_err(|_| ContentError::LengthOverflow)?;
                if end > leaf_len || start > end {
                    return Err(ContentError::MalformedObject(
                        "leaf range exceeds canonical bytes".to_owned(),
                    ));
                }
                let start = usize::try_from(start).map_err(|_| ContentError::LengthOverflow)?;
                let end = usize::try_from(end).map_err(|_| ContentError::LengthOverflow)?;
                out.extend_from_slice(&bytes[start..end]);
            }
            ContentObject::Branch(children) => {
                let total = children.iter().try_fold(0_u64, |sum, child| {
                    sum.checked_add(child.byte_len)
                        .ok_or(ContentError::LengthOverflow)
                })?;
                if end > total || start > end {
                    return Err(ContentError::MalformedObject(
                        "branch range exceeds aggregate length".to_owned(),
                    ));
                }
                let mut child_start = 0_u64;
                for child in children {
                    let child_end = child_start
                        .checked_add(child.byte_len)
                        .ok_or(ContentError::LengthOverflow)?;
                    let overlap_start = start.max(child_start);
                    let overlap_end = end.min(child_end);
                    if overlap_start < overlap_end {
                        self.read_object_range(
                            domain,
                            child.id,
                            overlap_start - child_start,
                            overlap_end - child_start,
                            Some(child.byte_len),
                            store,
                            out,
                        )?;
                    }
                    child_start = child_end;
                    if child_start >= end {
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}

/// Online counterpart to [`ContentTree::build`].
///
/// This is intentionally internal: callers use `build_streaming`, while this
/// type keeps the memory invariant explicit and testable near the tree format.
struct StreamingContentBuilder<'a, S> {
    tree: ContentTree,
    domain: &'a ContentDomain,
    store: &'a mut S,
    leaf: Vec<u8>,
    rolling: u64,
    window: [u8; LEAF_HASH_WINDOW],
    window_len: usize,
    window_cursor: usize,
    // Each entry is the unfinished child list for one branch level.  Complete
    // branches immediately become one descriptor in the next level.
    levels: Vec<Vec<ChildDescriptor>>,
    saw_bytes: bool,
}

const LEAF_HASH_WINDOW: usize = 63;

impl<'a, S: ImmutableContentStore> StreamingContentBuilder<'a, S> {
    fn new(tree: ContentTree, domain: &'a ContentDomain, store: &'a mut S) -> Self {
        Self {
            tree,
            domain,
            store,
            leaf: Vec::with_capacity(tree.profile.max_leaf_bytes),
            rolling: 0,
            window: [0; LEAF_HASH_WINDOW],
            window_len: 0,
            window_cursor: 0,
            levels: Vec::new(),
            saw_bytes: false,
        }
    }

    fn push(&mut self, bytes: &[u8]) -> Result<(), ContentError> {
        for byte in bytes.iter().copied() {
            self.saw_bytes = true;
            self.push_byte(byte)?;
        }
        Ok(())
    }

    fn push_byte(&mut self, byte: u8) -> Result<(), ContentError> {
        if self.window_len < LEAF_HASH_WINDOW {
            self.rolling = self.rolling.rotate_left(1) ^ gear(byte);
            self.window_len += 1;
        } else {
            let outgoing = self.window[self.window_cursor];
            self.rolling = self.rolling.rotate_left(1)
                ^ gear(byte)
                ^ gear(outgoing).rotate_left(LEAF_HASH_WINDOW as u32);
        }
        self.window[self.window_cursor] = byte;
        self.window_cursor = (self.window_cursor + 1) % LEAF_HASH_WINDOW;
        self.leaf.push(byte);

        let profile = self.tree.profile;
        let mask = u64::try_from(profile.target_leaf_bytes - 1).expect("target fits u64");
        if (self.leaf.len() >= profile.min_leaf_bytes && self.rolling & mask == 0)
            || self.leaf.len() >= profile.max_leaf_bytes
        {
            self.finish_leaf()?;
        }
        Ok(())
    }

    fn finish_leaf(&mut self) -> Result<(), ContentError> {
        let bytes = std::mem::take(&mut self.leaf);
        self.leaf = Vec::with_capacity(self.tree.profile.max_leaf_bytes);
        self.rolling = 0;
        self.window_len = 0;
        self.window_cursor = 0;
        let descriptor = self.tree.persist_object(
            self.domain,
            ContentObject::Leaf {
                bytes,
                utf16_len: None,
            },
            self.store,
        )?;
        self.push_descriptor(0, descriptor)
    }

    fn push_descriptor(
        &mut self,
        level: usize,
        descriptor: ChildDescriptor,
    ) -> Result<(), ContentError> {
        if self.levels.len() <= level {
            self.levels.resize_with(level + 1, Vec::new);
        }
        self.levels[level].push(descriptor);
        let children = &self.levels[level];
        let profile = self.tree.profile;
        if (children.len() >= profile.min_children
            && descriptor_boundary(children.last().unwrap(), profile))
            || children.len() >= profile.max_children
        {
            let children = std::mem::take(&mut self.levels[level]);
            let parent = self.tree.persist_object(
                self.domain,
                ContentObject::Branch(children),
                self.store,
            )?;
            self.push_descriptor(level + 1, parent)?;
        }
        Ok(())
    }

    fn finish(mut self) -> Result<(ContentId, u64), ContentError> {
        if self.saw_bytes {
            if !self.leaf.is_empty() {
                self.finish_leaf()?;
            }
        } else {
            self.finish_leaf()?;
        }

        // This is the online equivalent of the batch `while level.len() > 1`:
        // an incomplete lower group becomes a branch only when it has siblings
        // or an already-emitted group at any higher level. A completed group
        // can promote through an otherwise empty immediate parent level.
        let mut level = 0;
        loop {
            let Some(last_nonempty) = self.levels.iter().rposition(|items| !items.is_empty())
            else {
                unreachable!("stream always persists an empty or non-empty leaf");
            };
            if level > last_nonempty {
                unreachable!("finalization advances through all non-empty levels");
            }
            if self.levels[level].is_empty() {
                level += 1;
                continue;
            }
            let has_higher_level = self
                .levels
                .iter()
                .skip(level + 1)
                .any(|items| !items.is_empty());
            if self.levels[level].len() == 1 && !has_higher_level {
                let root = self.levels[level].pop().expect("non-empty level");
                return Ok((root.id, root.byte_len));
            }
            let children = std::mem::take(&mut self.levels[level]);
            let parent = self.tree.persist_object(
                self.domain,
                ContentObject::Branch(children),
                self.store,
            )?;
            self.push_descriptor(level + 1, parent)?;
            level += 1;
        }
    }
}

impl LargeValue {
    /// Return the assembled logical byte length without loading tree payloads.
    pub fn byte_len(&self) -> Result<u64, ContentError> {
        match self {
            Self::Inline(bytes) => {
                u64::try_from(bytes.len()).map_err(|_| ContentError::LengthOverflow)
            }
            Self::Chunked(large) => patched_length(large),
        }
    }

    /// Return the assembled UTF-16 code-unit length recorded for text.
    pub fn utf16_len(&self) -> Result<u64, ContentError> {
        match self {
            Self::Inline(bytes) => {
                count_utf16(std::str::from_utf8(bytes).map_err(|_| ContentError::InvalidUtf8)?)
            }
            Self::Chunked(large) => {
                let mut len = large.root_utf16_len.ok_or(ContentError::InvalidSelection)?;
                for patch in &large.edit_tail {
                    let metrics = patch.text_metrics.ok_or_else(|| {
                        ContentError::MalformedCell(
                            "text tail patch lacks UTF-16 metrics".to_owned(),
                        )
                    })?;
                    len = len
                        .checked_sub(metrics.delete_len)
                        .and_then(|value| value.checked_add(metrics.insert_len))
                        .ok_or(ContentError::LengthOverflow)?;
                }
                Ok(len)
            }
        }
    }

    /// Encode the complete atomic physical cell for ordinary Jazz storage and
    /// wire transport.
    pub fn encode_cell(&self) -> Result<Vec<u8>, ContentError> {
        let mut encoded = Vec::new();
        encoded.extend_from_slice(CELL_ENVELOPE);
        match self {
            Self::Inline(bytes) => {
                encoded.push(0);
                encoded.extend_from_slice(
                    &u64::try_from(bytes.len())
                        .map_err(|_| ContentError::LengthOverflow)?
                        .to_le_bytes(),
                );
                encoded.extend_from_slice(bytes);
            }
            Self::Chunked(large) => {
                encoded.push(1);
                encoded.extend_from_slice(large.root.as_bytes());
                encoded.extend_from_slice(&large.root_byte_len.to_le_bytes());
                encoded.push(u8::from(large.root_utf16_len.is_some()));
                encoded.extend_from_slice(&large.root_utf16_len.unwrap_or(0).to_le_bytes());
                encoded.extend_from_slice(
                    &u64::try_from(large.edit_tail.len())
                        .map_err(|_| ContentError::LengthOverflow)?
                        .to_le_bytes(),
                );
                for patch in &large.edit_tail {
                    encoded.extend_from_slice(&patch.offset.to_le_bytes());
                    encoded.extend_from_slice(&patch.delete_len.to_le_bytes());
                    encoded.extend_from_slice(
                        &u64::try_from(patch.insert.len())
                            .map_err(|_| ContentError::LengthOverflow)?
                            .to_le_bytes(),
                    );
                    encoded.push(u8::from(patch.text_metrics.is_some()));
                    let metrics = patch.text_metrics.unwrap_or(TextPatchMetrics {
                        delete_len: 0,
                        insert_len: 0,
                    });
                    encoded.extend_from_slice(&metrics.delete_len.to_le_bytes());
                    encoded.extend_from_slice(&metrics.insert_len.to_le_bytes());
                    encoded.extend_from_slice(&patch.insert);
                }
            }
        }
        Ok(encoded)
    }

    /// Decode one exact alpha physical cell. There is deliberately no legacy
    /// or compatibility fallback.
    pub fn decode_cell(schema: &LargeValueSchema, encoded: &[u8]) -> Result<Self, ContentError> {
        schema.validate()?;
        let payload = encoded
            .strip_prefix(CELL_ENVELOPE)
            .ok_or(ContentError::UnknownCellFormat)?;
        let (&tag, payload) = payload
            .split_first()
            .ok_or_else(|| ContentError::MalformedCell("missing cell tag".to_owned()))?;
        let mut cursor = 0;
        let cell = match tag {
            0 => {
                let len = read_u64(payload, &mut cursor)?;
                if len > u64::from(schema.inline_up_to) {
                    return Err(ContentError::MalformedCell(
                        "inline value exceeds schema threshold".to_owned(),
                    ));
                }
                let len = usize::try_from(len).map_err(|_| ContentError::LengthOverflow)?;
                let bytes = take_cell_bytes(payload, &mut cursor, len)?.to_vec();
                schema.kind.validate(&bytes)?;
                Self::Inline(bytes)
            }
            1 => {
                let mut root = [0; 32];
                root.copy_from_slice(take_cell_bytes(payload, &mut cursor, 32)?);
                let root_byte_len = read_u64(payload, &mut cursor)?;
                let root_metric_tag = take_cell_bytes(payload, &mut cursor, 1)?[0];
                if root_metric_tag > 1 {
                    return Err(ContentError::MalformedCell(
                        "invalid root UTF-16 metric tag".to_owned(),
                    ));
                }
                let root_metric = read_u64(payload, &mut cursor)?;
                let root_utf16_len = (root_metric_tag == 1).then_some(root_metric);
                let count = read_u64(payload, &mut cursor)?;
                if count > u64::from(schema.max_tail_entries) {
                    return Err(ContentError::TailTooLarge);
                }
                let count = usize::try_from(count).map_err(|_| ContentError::LengthOverflow)?;
                let mut edit_tail = Vec::with_capacity(count);
                let mut tail_frame_bytes = std::mem::size_of::<u64>();
                for _ in 0..count {
                    let offset = read_u64(payload, &mut cursor)?;
                    let delete_len = read_u64(payload, &mut cursor)?;
                    let insert_len = read_u64(payload, &mut cursor)?;
                    let insert_len =
                        usize::try_from(insert_len).map_err(|_| ContentError::LengthOverflow)?;
                    if insert_len
                        > usize::try_from(schema.max_tail_bytes)
                            .map_err(|_| ContentError::LengthOverflow)?
                    {
                        return Err(ContentError::TailTooLarge);
                    }
                    tail_frame_bytes = tail_frame_bytes
                        .checked_add(PATCH_FRAME_HEADER_BYTES)
                        .and_then(|bytes| bytes.checked_add(insert_len))
                        .ok_or(ContentError::LengthOverflow)?;
                    if tail_frame_bytes
                        > usize::try_from(schema.max_tail_bytes)
                            .map_err(|_| ContentError::LengthOverflow)?
                    {
                        return Err(ContentError::TailTooLarge);
                    }
                    let metric_tag = take_cell_bytes(payload, &mut cursor, 1)?[0];
                    if metric_tag > 1 {
                        return Err(ContentError::MalformedCell(
                            "invalid patch UTF-16 metric tag".to_owned(),
                        ));
                    }
                    let delete_utf16 = read_u64(payload, &mut cursor)?;
                    let insert_utf16 = read_u64(payload, &mut cursor)?;
                    let insert = take_cell_bytes(payload, &mut cursor, insert_len)?.to_vec();
                    edit_tail.push(BytePatch {
                        offset,
                        delete_len,
                        insert,
                        text_metrics: (metric_tag == 1).then_some(TextPatchMetrics {
                            delete_len: delete_utf16,
                            insert_len: insert_utf16,
                        }),
                    });
                }
                let large = ChunkedValue {
                    root: ContentId(root),
                    root_byte_len,
                    root_utf16_len,
                    edit_tail,
                };
                let text_metrics = schema.kind == ValueKind::String;
                if large.root_utf16_len.is_some() != text_metrics
                    || large
                        .edit_tail
                        .iter()
                        .any(|patch| patch.text_metrics.is_some() != text_metrics)
                {
                    return Err(ContentError::MalformedCell(
                        "UTF-16 metrics do not match the logical value kind".to_owned(),
                    ));
                }
                if !tail_within_bounds(&large.edit_tail, schema.tail_bounds())? {
                    return Err(ContentError::TailTooLarge);
                }
                Self::Chunked(large)
            }
            _ => return Err(ContentError::UnknownCellFormat),
        };
        if cursor != payload.len() {
            return Err(ContentError::MalformedCell(
                "trailing large value bytes".to_owned(),
            ));
        }
        Ok(cell)
    }

    /// Create an inline value after logical validation.
    pub fn inline(kind: ValueKind, bytes: impl Into<Vec<u8>>) -> Result<Self, ContentError> {
        let bytes = bytes.into();
        kind.validate(&bytes)?;
        Ok(Self::Inline(bytes))
    }

    /// Create the representation selected by one promotion threshold.
    pub fn create<S: ImmutableContentStore>(
        kind: ValueKind,
        domain: &ContentDomain,
        bytes: impl Into<Vec<u8>>,
        inline_up_to: usize,
        tree: ContentTree,
        store: &mut S,
    ) -> Result<Self, ContentError> {
        let bytes = bytes.into();
        kind.validate(&bytes)?;
        if bytes.len() <= inline_up_to {
            return Ok(Self::Inline(bytes));
        }
        let (root, root_byte_len, root_utf16_len) = if kind == ValueKind::String {
            let text = std::str::from_utf8(&bytes).map_err(|_| ContentError::InvalidUtf8)?;
            let (root, byte_len, utf16_len) = tree.build_text(domain, text, store)?;
            (root, byte_len, Some(utf16_len))
        } else {
            let (root, byte_len) = tree.build(domain, &bytes, store)?;
            (root, byte_len, None)
        };
        Ok(Self::Chunked(ChunkedValue {
            root,
            root_byte_len,
            root_utf16_len,
            edit_tail: Vec::new(),
        }))
    }

    /// Create a chunked bytes value from bounded transport buffers.
    ///
    /// Unlike [`Self::create`], this never retains the complete logical value
    /// and deliberately always returns the chunked representation. Text and
    /// JSON require streaming logical validation and are not accepted by this
    /// bytes-specific entry point.
    pub fn create_streaming_bytes<S, I, B>(
        domain: &ContentDomain,
        chunks: I,
        tree: ContentTree,
        store: &mut S,
    ) -> Result<Self, ContentError>
    where
        S: ImmutableContentStore,
        I: IntoIterator<Item = B>,
        B: AsRef<[u8]>,
    {
        let (root, root_byte_len) = tree.build_streaming(domain, chunks, store)?;
        Ok(Self::Chunked(ChunkedValue {
            root,
            root_byte_len,
            root_utf16_len: None,
            edit_tail: Vec::new(),
        }))
    }

    /// Materialize and validate the complete logical value.
    pub fn materialize<S: ImmutableContentStore>(
        &self,
        kind: ValueKind,
        domain: &ContentDomain,
        tree: ContentTree,
        store: &S,
    ) -> Result<Vec<u8>, ContentError> {
        let bytes = match self {
            Self::Inline(bytes) => bytes.clone(),
            Self::Chunked(large) => {
                materialize_large_range(large, domain, tree, store, 0, patched_length(large)?)?
            }
        };
        kind.validate(&bytes)?;
        Ok(bytes)
    }

    /// Evaluate one immutable query projection.
    pub fn select<S: ImmutableContentStore>(
        &self,
        kind: ValueKind,
        selection: &ValueSelection,
        domain: &ContentDomain,
        tree: ContentTree,
        store: &S,
    ) -> Result<ValueSelectionResult, ContentError> {
        match (kind, selection) {
            (ValueKind::Bytes, ValueSelection::ByteRange { offset, len }) => {
                let selected = match self {
                    Self::Inline(bytes) => checked_slice(bytes, *offset, *len)?.to_vec(),
                    Self::Chunked(large) => {
                        materialize_large_range(large, domain, tree, store, *offset, *len)?
                    }
                };
                return Ok(ValueSelectionResult::Bytes(selected));
            }
            (ValueKind::String, ValueSelection::TextUtf8Range { offset, len }) => {
                let end = offset
                    .checked_add(*len)
                    .ok_or(ContentError::LengthOverflow)?;
                let selected = match self {
                    Self::Inline(bytes) => {
                        let text =
                            std::str::from_utf8(bytes).map_err(|_| ContentError::InvalidUtf8)?;
                        let start = utf8_boundary(text, *offset)?;
                        let end = utf8_boundary(text, end)?;
                        text.as_bytes()[start..end].to_vec()
                    }
                    Self::Chunked(large) => {
                        validate_large_utf8_boundary(large, domain, tree, store, *offset)?;
                        validate_large_utf8_boundary(large, domain, tree, store, end)?;
                        materialize_large_range(large, domain, tree, store, *offset, *len)?
                    }
                };
                return String::from_utf8(selected)
                    .map(ValueSelectionResult::String)
                    .map_err(|_| ContentError::InvalidUtf8);
            }
            (ValueKind::String, ValueSelection::TextUtf16Range { offset, len }) => {
                let end = offset
                    .checked_add(*len)
                    .ok_or(ContentError::LengthOverflow)?;
                let selected = match self {
                    Self::Inline(bytes) => {
                        let text =
                            std::str::from_utf8(bytes).map_err(|_| ContentError::InvalidUtf8)?;
                        let start = utf16_to_byte_offset(text, *offset)?;
                        let end = utf16_to_byte_offset(text, end)?;
                        text.as_bytes()[start..end].to_vec()
                    }
                    Self::Chunked(large) => {
                        let start =
                            large_byte_offset_for_utf16(large, domain, tree, store, *offset)?;
                        let end = large_byte_offset_for_utf16(large, domain, tree, store, end)?;
                        materialize_large_range(large, domain, tree, store, start, end - start)?
                    }
                };
                return String::from_utf8(selected)
                    .map(ValueSelectionResult::String)
                    .map_err(|_| ContentError::InvalidUtf8);
            }
            _ => {}
        }
        let bytes = self.materialize(kind, domain, tree, store)?;
        match (kind, selection) {
            (ValueKind::Bytes, ValueSelection::Value) => Ok(ValueSelectionResult::Bytes(bytes)),
            (ValueKind::Bytes, ValueSelection::ByteRange { .. }) => unreachable!(),
            (ValueKind::String, ValueSelection::Value) => Ok(ValueSelectionResult::String(
                String::from_utf8(bytes).map_err(|_| ContentError::InvalidUtf8)?,
            )),
            (ValueKind::String, ValueSelection::TextUtf8Range { .. })
            | (ValueKind::String, ValueSelection::TextUtf16Range { .. }) => unreachable!(),
            (ValueKind::Json, ValueSelection::Value) => Ok(ValueSelectionResult::Json(
                serde_json::from_slice(&bytes)
                    .map_err(|error| ContentError::InvalidJson(error.to_string()))?,
            )),
            (ValueKind::Json, ValueSelection::JsonPointer(pointer)) => {
                let value: serde_json::Value = serde_json::from_slice(&bytes)
                    .map_err(|error| ContentError::InvalidJson(error.to_string()))?;
                Ok(ValueSelectionResult::Json(
                    value.pointer(pointer).cloned().ok_or_else(|| {
                        ContentError::InvalidJson(format!(
                            "JSON pointer {pointer:?} does not exist"
                        ))
                    })?,
                ))
            }
            _ => Err(ContentError::InvalidSelection),
        }
    }

    /// Lower one declarative operation against this exact value snapshot.
    pub fn lower_edit<S: ImmutableContentStore>(
        &self,
        kind: ValueKind,
        edit: ValueEdit,
        domain: &ContentDomain,
        tree: ContentTree,
        store: &S,
    ) -> Result<BytePatch, ContentError> {
        if let (
            ValueKind::String,
            ValueEdit::TextUtf8 {
                slice_offset,
                slice_len,
                offset,
                delete_len,
                insert,
            },
        ) = (kind, &edit)
        {
            validate_relative_range(*slice_len, *offset, *delete_len)?;
            let absolute = slice_offset
                .checked_add(*offset)
                .ok_or(ContentError::LengthOverflow)?;
            let end = absolute
                .checked_add(*delete_len)
                .ok_or(ContentError::LengthOverflow)?;
            let slice_end = slice_offset
                .checked_add(*slice_len)
                .ok_or(ContentError::LengthOverflow)?;
            let deleted = match self {
                Self::Inline(bytes) => {
                    let text = std::str::from_utf8(bytes).map_err(|_| ContentError::InvalidUtf8)?;
                    utf8_boundary(text, *slice_offset)?;
                    utf8_boundary(text, slice_end)?;
                    let start = utf8_boundary(text, absolute)?;
                    let end = utf8_boundary(text, end)?;
                    text.as_bytes()[start..end].to_vec()
                }
                Self::Chunked(large) => {
                    validate_large_utf8_boundary(large, domain, tree, store, *slice_offset)?;
                    validate_large_utf8_boundary(large, domain, tree, store, slice_end)?;
                    validate_large_utf8_boundary(large, domain, tree, store, absolute)?;
                    validate_large_utf8_boundary(large, domain, tree, store, end)?;
                    materialize_large_range(large, domain, tree, store, absolute, *delete_len)?
                }
            };
            let deleted = std::str::from_utf8(&deleted).map_err(|_| ContentError::InvalidUtf8)?;
            return Ok(BytePatch::replace(absolute, *delete_len, insert.as_bytes())
                .with_text_metrics(count_utf16(deleted)?, count_utf16(insert)?));
        }
        if let (
            ValueKind::String,
            ValueEdit::TextUtf16 {
                slice_offset,
                slice_len,
                offset,
                delete_len,
                insert,
            },
        ) = (kind, &edit)
        {
            validate_relative_range(*slice_len, *offset, *delete_len)?;
            let absolute = slice_offset
                .checked_add(*offset)
                .ok_or(ContentError::LengthOverflow)?;
            let end = absolute
                .checked_add(*delete_len)
                .ok_or(ContentError::LengthOverflow)?;
            let slice_end = slice_offset
                .checked_add(*slice_len)
                .ok_or(ContentError::LengthOverflow)?;
            let (start_byte, end_byte) = match self {
                Self::Inline(bytes) => {
                    let text = std::str::from_utf8(bytes).map_err(|_| ContentError::InvalidUtf8)?;
                    utf16_to_byte_offset(text, *slice_offset)?;
                    utf16_to_byte_offset(text, slice_end)?;
                    (
                        utf16_to_byte_offset(text, absolute)?,
                        utf16_to_byte_offset(text, end)?,
                    )
                }
                Self::Chunked(large) => {
                    large_byte_offset_for_utf16(large, domain, tree, store, *slice_offset)?;
                    large_byte_offset_for_utf16(large, domain, tree, store, slice_end)?;
                    (
                        usize::try_from(large_byte_offset_for_utf16(
                            large, domain, tree, store, absolute,
                        )?)
                        .map_err(|_| ContentError::LengthOverflow)?,
                        usize::try_from(large_byte_offset_for_utf16(
                            large, domain, tree, store, end,
                        )?)
                        .map_err(|_| ContentError::LengthOverflow)?,
                    )
                }
            };
            return Ok(BytePatch::replace(
                u64::try_from(start_byte).map_err(|_| ContentError::LengthOverflow)?,
                u64::try_from(end_byte - start_byte).map_err(|_| ContentError::LengthOverflow)?,
                insert.as_bytes(),
            )
            .with_text_metrics(*delete_len, count_utf16(insert)?));
        }
        let bytes = self.materialize(kind, domain, tree, store)?;
        match (kind, edit) {
            (ValueKind::Bytes, ValueEdit::Bytes(patch)) => {
                apply_patches(&bytes, std::slice::from_ref(&patch))?;
                Ok(patch)
            }
            (ValueKind::Bytes, ValueEdit::Append(insert)) => Ok(BytePatch::insert(
                u64::try_from(bytes.len()).map_err(|_| ContentError::LengthOverflow)?,
                insert,
            )),
            (ValueKind::String, ValueEdit::TextUtf8 { .. })
            | (ValueKind::String, ValueEdit::TextUtf16 { .. }) => unreachable!(),
            (ValueKind::Json, ValueEdit::Json(value)) => {
                let next = serde_json::to_vec(&value)
                    .map_err(|error| ContentError::InvalidJson(error.to_string()))?;
                Ok(single_replace_diff(&bytes, &next))
            }
            _ => Err(ContentError::InvalidEdit),
        }
    }

    /// Apply one declarative patch, consolidating only when tail bounds require it.
    pub fn apply_edit<S: ImmutableContentStore>(
        &self,
        kind: ValueKind,
        domain: &ContentDomain,
        mut patch: BytePatch,
        inline_up_to: usize,
        tail_bounds: TailBounds,
        tree: ContentTree,
        store: &mut S,
    ) -> Result<Self, ContentError> {
        let current = self.materialize(kind, domain, tree, store)?;
        if kind == ValueKind::String && patch.text_metrics.is_none() {
            patch.text_metrics = Some(text_patch_metrics(&current, &patch)?);
        }
        let next = apply_patches(&current, std::slice::from_ref(&patch))?;
        kind.validate(&next)?;

        match self {
            Self::Inline(_) if next.len() <= inline_up_to => Ok(Self::Inline(next)),
            Self::Inline(_) => Self::create(kind, domain, next, inline_up_to, tree, store),
            Self::Chunked(large) => {
                let mut tail = large.edit_tail.clone();
                tail.push(patch);
                if tail_within_bounds(&tail, tail_bounds)? {
                    Ok(Self::Chunked(ChunkedValue {
                        root: large.root,
                        root_byte_len: large.root_byte_len,
                        root_utf16_len: large.root_utf16_len,
                        edit_tail: tail,
                    }))
                } else {
                    let (root, root_byte_len, root_utf16_len) = if kind == ValueKind::String {
                        let text =
                            std::str::from_utf8(&next).map_err(|_| ContentError::InvalidUtf8)?;
                        let (root, byte_len, utf16_len) = tree.build_text(domain, text, store)?;
                        (root, byte_len, Some(utf16_len))
                    } else {
                        let (root, byte_len) = tree.build(domain, &next, store)?;
                        (root, byte_len, None)
                    };
                    Ok(Self::Chunked(ChunkedValue {
                        root,
                        root_byte_len,
                        root_utf16_len,
                        edit_tail: Vec::new(),
                    }))
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
enum MaterialPiece {
    Root { offset: u64, len: u64 },
    Insert(Vec<u8>),
}

impl MaterialPiece {
    fn len(&self) -> Result<u64, ContentError> {
        match self {
            Self::Root { len, .. } => Ok(*len),
            Self::Insert(bytes) => {
                u64::try_from(bytes.len()).map_err(|_| ContentError::LengthOverflow)
            }
        }
    }

    fn split(self, at: u64) -> Result<(Self, Self), ContentError> {
        match self {
            Self::Root { offset, len } => Ok((
                Self::Root { offset, len: at },
                Self::Root {
                    offset: offset.checked_add(at).ok_or(ContentError::LengthOverflow)?,
                    len: len.checked_sub(at).ok_or(ContentError::LengthOverflow)?,
                },
            )),
            Self::Insert(bytes) => {
                let at = usize::try_from(at).map_err(|_| ContentError::LengthOverflow)?;
                Ok((
                    Self::Insert(bytes[..at].to_vec()),
                    Self::Insert(bytes[at..].to_vec()),
                ))
            }
        }
    }
}

fn patched_length(large: &ChunkedValue) -> Result<u64, ContentError> {
    large
        .edit_tail
        .iter()
        .try_fold(large.root_byte_len, |len, patch| {
            let end = patch
                .offset
                .checked_add(patch.delete_len)
                .ok_or(ContentError::LengthOverflow)?;
            if end > len {
                return Err(ContentError::PatchOutOfBounds {
                    offset: patch.offset,
                    end,
                    value_len: len,
                });
            }
            len.checked_sub(patch.delete_len)
                .and_then(|len| len.checked_add(u64::try_from(patch.insert.len()).ok()?))
                .ok_or(ContentError::LengthOverflow)
        })
}

fn material_pieces(large: &ChunkedValue) -> Result<Vec<MaterialPiece>, ContentError> {
    let mut pieces = vec![MaterialPiece::Root {
        offset: 0,
        len: large.root_byte_len,
    }];
    for patch in &large.edit_tail {
        let current_len = pieces.iter().try_fold(0_u64, |sum, piece| {
            sum.checked_add(piece.len()?)
                .ok_or(ContentError::LengthOverflow)
        })?;
        let end = patch
            .offset
            .checked_add(patch.delete_len)
            .ok_or(ContentError::LengthOverflow)?;
        if end > current_len {
            return Err(ContentError::PatchOutOfBounds {
                offset: patch.offset,
                end,
                value_len: current_len,
            });
        }
        let start_index = split_pieces_at(&mut pieces, patch.offset)?;
        let end_index = split_pieces_at(&mut pieces, end)?;
        let replacement = (!patch.insert.is_empty())
            .then(|| MaterialPiece::Insert(patch.insert.clone()))
            .into_iter();
        pieces.splice(start_index..end_index, replacement);
    }
    Ok(pieces)
}

fn split_pieces_at(pieces: &mut Vec<MaterialPiece>, at: u64) -> Result<usize, ContentError> {
    let mut cursor = 0_u64;
    for index in 0..pieces.len() {
        let len = pieces[index].len()?;
        let end = cursor
            .checked_add(len)
            .ok_or(ContentError::LengthOverflow)?;
        if at == cursor {
            return Ok(index);
        }
        if at < end {
            let piece = pieces.remove(index);
            let (left, right) = piece.split(at - cursor)?;
            pieces.insert(index, right);
            pieces.insert(index, left);
            return Ok(index + 1);
        }
        cursor = end;
    }
    if at == cursor {
        Ok(pieces.len())
    } else {
        Err(ContentError::PatchOutOfBounds {
            offset: at,
            end: at,
            value_len: cursor,
        })
    }
}

fn materialize_large_range<S: ImmutableContentStore>(
    large: &ChunkedValue,
    domain: &ContentDomain,
    tree: ContentTree,
    store: &S,
    offset: u64,
    len: u64,
) -> Result<Vec<u8>, ContentError> {
    let logical_len = patched_length(large)?;
    let end = offset
        .checked_add(len)
        .ok_or(ContentError::LengthOverflow)?;
    if end > logical_len {
        return Err(ContentError::PatchOutOfBounds {
            offset,
            end,
            value_len: logical_len,
        });
    }
    usize::try_from(len).map_err(|_| ContentError::LengthOverflow)?;
    let mut out = Vec::new();
    let mut cursor = 0_u64;
    for piece in material_pieces(large)? {
        let piece_len = piece.len()?;
        let piece_end = cursor
            .checked_add(piece_len)
            .ok_or(ContentError::LengthOverflow)?;
        let overlap_start = offset.max(cursor);
        let overlap_end = end.min(piece_end);
        if overlap_start < overlap_end {
            let within = overlap_start - cursor;
            let overlap_len = overlap_end - overlap_start;
            match piece {
                MaterialPiece::Root {
                    offset: root_offset,
                    ..
                } => out.extend(
                    tree.read_range(
                        domain,
                        large.root,
                        large.root_byte_len,
                        root_offset
                            .checked_add(within)
                            .ok_or(ContentError::LengthOverflow)?,
                        overlap_len,
                        store,
                    )?,
                ),
                MaterialPiece::Insert(bytes) => {
                    out.extend_from_slice(checked_slice(&bytes, within, overlap_len)?)
                }
            }
        }
        cursor = piece_end;
        if cursor >= end {
            break;
        }
    }
    Ok(out)
}

fn large_utf16_len<S: ImmutableContentStore>(
    large: &ChunkedValue,
    domain: &ContentDomain,
    tree: ContentTree,
    store: &S,
) -> Result<u64, ContentError> {
    let root_utf16 = large.root_utf16_len.ok_or(ContentError::InvalidSelection)?;
    let mut total = root_utf16;
    for patch in &large.edit_tail {
        let metrics = patch.text_metrics.ok_or_else(|| {
            ContentError::MalformedCell("text tail patch lacks UTF-16 metrics".to_owned())
        })?;
        total = total
            .checked_sub(metrics.delete_len)
            .and_then(|value| value.checked_add(metrics.insert_len))
            .ok_or(ContentError::LengthOverflow)?;
    }
    // Authenticate the root metric even when the requested position falls in
    // tail-inserted text and no root payload would otherwise be visited.
    tree.utf16_prefix(
        domain,
        large.root,
        large.root_byte_len,
        root_utf16,
        0,
        store,
    )?;
    Ok(total)
}

fn large_byte_offset_for_utf16<S: ImmutableContentStore>(
    large: &ChunkedValue,
    domain: &ContentDomain,
    tree: ContentTree,
    store: &S,
    target: u64,
) -> Result<u64, ContentError> {
    let total = large_utf16_len(large, domain, tree, store)?;
    if target > total {
        return Err(ContentError::TextOffsetOutOfBounds {
            encoding: "UTF-16",
            offset: target,
            text_len: total,
        });
    }
    let root_utf16 = large.root_utf16_len.ok_or(ContentError::InvalidSelection)?;
    let mut logical_bytes = 0_u64;
    let mut logical_utf16 = 0_u64;
    for piece in material_pieces(large)? {
        let (piece_bytes, piece_utf16) = match &piece {
            MaterialPiece::Root { offset, len } => {
                let start = tree.utf16_prefix(
                    domain,
                    large.root,
                    large.root_byte_len,
                    root_utf16,
                    *offset,
                    store,
                )?;
                let end_offset = offset
                    .checked_add(*len)
                    .ok_or(ContentError::LengthOverflow)?;
                let end = tree.utf16_prefix(
                    domain,
                    large.root,
                    large.root_byte_len,
                    root_utf16,
                    end_offset,
                    store,
                )?;
                (
                    *len,
                    end.checked_sub(start).ok_or(ContentError::LengthOverflow)?,
                )
            }
            MaterialPiece::Insert(bytes) => {
                let text = std::str::from_utf8(bytes).map_err(|_| ContentError::InvalidUtf8)?;
                (
                    u64::try_from(bytes.len()).map_err(|_| ContentError::LengthOverflow)?,
                    count_utf16(text)?,
                )
            }
        };
        let piece_end = logical_utf16
            .checked_add(piece_utf16)
            .ok_or(ContentError::LengthOverflow)?;
        if target <= piece_end {
            let within_utf16 = target - logical_utf16;
            let within_bytes = match piece {
                MaterialPiece::Root { offset, .. } => {
                    let prefix = tree.utf16_prefix(
                        domain,
                        large.root,
                        large.root_byte_len,
                        root_utf16,
                        offset,
                        store,
                    )?;
                    let root_target = prefix
                        .checked_add(within_utf16)
                        .ok_or(ContentError::LengthOverflow)?;
                    tree.byte_offset_for_utf16(
                        domain,
                        large.root,
                        large.root_byte_len,
                        root_utf16,
                        root_target,
                        store,
                    )?
                    .checked_sub(offset)
                    .ok_or(ContentError::LengthOverflow)?
                }
                MaterialPiece::Insert(bytes) => {
                    let text =
                        std::str::from_utf8(&bytes).map_err(|_| ContentError::InvalidUtf8)?;
                    u64::try_from(utf16_to_byte_offset(text, within_utf16)?)
                        .map_err(|_| ContentError::LengthOverflow)?
                }
            };
            return logical_bytes
                .checked_add(within_bytes)
                .ok_or(ContentError::LengthOverflow);
        }
        logical_bytes = logical_bytes
            .checked_add(piece_bytes)
            .ok_or(ContentError::LengthOverflow)?;
        logical_utf16 = piece_end;
    }
    Ok(logical_bytes)
}

fn validate_large_utf8_boundary<S: ImmutableContentStore>(
    large: &ChunkedValue,
    domain: &ContentDomain,
    tree: ContentTree,
    store: &S,
    target: u64,
) -> Result<(), ContentError> {
    let total = patched_length(large)?;
    if target > total {
        return Err(ContentError::TextOffsetOutOfBounds {
            encoding: "UTF-8",
            offset: target,
            text_len: total,
        });
    }
    let root_utf16 = large.root_utf16_len.ok_or(ContentError::InvalidSelection)?;
    let mut cursor = 0_u64;
    for piece in material_pieces(large)? {
        let len = piece.len()?;
        let end = cursor
            .checked_add(len)
            .ok_or(ContentError::LengthOverflow)?;
        if target <= end {
            let within = target - cursor;
            match piece {
                MaterialPiece::Root { offset, .. } => {
                    tree.utf16_prefix(
                        domain,
                        large.root,
                        large.root_byte_len,
                        root_utf16,
                        offset
                            .checked_add(within)
                            .ok_or(ContentError::LengthOverflow)?,
                        store,
                    )?;
                }
                MaterialPiece::Insert(bytes) => {
                    let text =
                        std::str::from_utf8(&bytes).map_err(|_| ContentError::InvalidUtf8)?;
                    utf8_boundary(text, within)?;
                }
            }
            return Ok(());
        }
        cursor = end;
    }
    Ok(())
}

fn checked_slice(bytes: &[u8], offset: u64, len: u64) -> Result<&[u8], ContentError> {
    let end = offset
        .checked_add(len)
        .ok_or(ContentError::LengthOverflow)?;
    let start = usize::try_from(offset).map_err(|_| ContentError::LengthOverflow)?;
    let end_usize = usize::try_from(end).map_err(|_| ContentError::LengthOverflow)?;
    bytes
        .get(start..end_usize)
        .ok_or(ContentError::PatchOutOfBounds {
            offset,
            end,
            value_len: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        })
}

fn single_replace_diff(before: &[u8], after: &[u8]) -> BytePatch {
    let prefix = before
        .iter()
        .zip(after)
        .take_while(|(left, right)| left == right)
        .count();
    let max_suffix = before.len().min(after.len()).saturating_sub(prefix);
    let suffix = before[before.len() - max_suffix..]
        .iter()
        .rev()
        .zip(after[after.len() - max_suffix..].iter().rev())
        .take_while(|(left, right)| left == right)
        .count();
    BytePatch::replace(
        u64::try_from(prefix).expect("slice length fits u64"),
        u64::try_from(before.len() - prefix - suffix).expect("slice length fits u64"),
        after[prefix..after.len() - suffix].to_vec(),
    )
}

/// Lower one UTF-8 byte-coordinate text replacement to the universal byte patch.
pub fn text_replace_patch_utf8(
    text: &str,
    offset: u64,
    delete_len: u64,
    insert: &str,
) -> Result<BytePatch, ContentError> {
    let end = offset
        .checked_add(delete_len)
        .ok_or(ContentError::LengthOverflow)?;
    let start = utf8_boundary(text, offset)?;
    let end = utf8_boundary(text, end)?;
    Ok(BytePatch::replace(offset, delete_len, insert.as_bytes())
        .with_text_metrics(count_utf16(&text[start..end])?, count_utf16(insert)?))
}

/// Lower one UTF-16 code-unit text replacement to the universal byte patch.
pub fn text_replace_patch_utf16(
    text: &str,
    offset: u64,
    delete_len: u64,
    insert: &str,
) -> Result<BytePatch, ContentError> {
    let start = utf16_to_byte_offset(text, offset)?;
    let end_offset = offset
        .checked_add(delete_len)
        .ok_or(ContentError::LengthOverflow)?;
    let end = utf16_to_byte_offset(text, end_offset)?;
    Ok(BytePatch::replace(
        u64::try_from(start).map_err(|_| ContentError::LengthOverflow)?,
        u64::try_from(end - start).map_err(|_| ContentError::LengthOverflow)?,
        insert.as_bytes(),
    )
    .with_text_metrics(delete_len, count_utf16(insert)?))
}

fn utf8_boundary(text: &str, offset: u64) -> Result<usize, ContentError> {
    let offset_usize = usize::try_from(offset).map_err(|_| ContentError::LengthOverflow)?;
    if offset_usize <= text.len() && text.is_char_boundary(offset_usize) {
        Ok(offset_usize)
    } else {
        Err(ContentError::TextOffsetOutOfBounds {
            encoding: "UTF-8",
            offset,
            text_len: u64::try_from(text.len()).map_err(|_| ContentError::LengthOverflow)?,
        })
    }
}

fn utf16_to_byte_offset(text: &str, offset: u64) -> Result<usize, ContentError> {
    let mut utf16 = 0_u64;
    for (byte, character) in text.char_indices() {
        if utf16 == offset {
            return Ok(byte);
        }
        utf16 = utf16
            .checked_add(u64::from(character.len_utf16() as u8))
            .ok_or(ContentError::LengthOverflow)?;
        if utf16 > offset {
            return Err(ContentError::TextOffsetOutOfBounds {
                encoding: "UTF-16",
                offset,
                text_len: count_utf16(text)?,
            });
        }
    }
    if utf16 == offset {
        Ok(text.len())
    } else {
        Err(ContentError::TextOffsetOutOfBounds {
            encoding: "UTF-16",
            offset,
            text_len: utf16,
        })
    }
}

fn validate_relative_range(
    slice_len: u64,
    offset: u64,
    delete_len: u64,
) -> Result<(), ContentError> {
    let end = offset
        .checked_add(delete_len)
        .ok_or(ContentError::LengthOverflow)?;
    if end <= slice_len {
        Ok(())
    } else {
        Err(ContentError::PatchOutOfBounds {
            offset,
            end,
            value_len: slice_len,
        })
    }
}

fn text_patch_metrics(current: &[u8], patch: &BytePatch) -> Result<TextPatchMetrics, ContentError> {
    let text = std::str::from_utf8(current).map_err(|_| ContentError::InvalidUtf8)?;
    let start = utf8_boundary(text, patch.offset)?;
    let end_offset = patch
        .offset
        .checked_add(patch.delete_len)
        .ok_or(ContentError::LengthOverflow)?;
    let end = utf8_boundary(text, end_offset)?;
    let insert = std::str::from_utf8(&patch.insert).map_err(|_| ContentError::InvalidUtf8)?;
    Ok(TextPatchMetrics {
        delete_len: count_utf16(&text[start..end])?,
        insert_len: count_utf16(insert)?,
    })
}

fn tail_within_bounds(tail: &[BytePatch], bounds: TailBounds) -> Result<bool, ContentError> {
    if bounds.max_entries == 0 || bounds.max_encoded_bytes == 0 {
        return Err(ContentError::TailTooLarge);
    }
    if tail.len() > bounds.max_entries {
        return Ok(false);
    }
    let encoded_len = tail
        .iter()
        .try_fold(std::mem::size_of::<u64>(), |bytes, patch| {
            bytes
                .checked_add(PATCH_FRAME_HEADER_BYTES)
                .and_then(|bytes| bytes.checked_add(patch.insert.len()))
                .ok_or(ContentError::LengthOverflow)
        })?;
    Ok(encoded_len <= bounds.max_encoded_bytes)
}

fn object_id(domain: &ContentDomain, canonical: &[u8]) -> ContentId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(CONTENT_ID_DOMAIN);
    hasher.update(&(domain.bytes().len() as u64).to_le_bytes());
    hasher.update(domain.bytes());
    hasher.update(&(canonical.len() as u64).to_le_bytes());
    hasher.update(canonical);
    ContentId(*hasher.finalize().as_bytes())
}

fn object_metrics(object: &ContentObject) -> Result<(u64, Option<u64>), ContentError> {
    match object {
        ContentObject::Leaf { bytes, utf16_len } => Ok((
            u64::try_from(bytes.len()).map_err(|_| ContentError::LengthOverflow)?,
            *utf16_len,
        )),
        ContentObject::Branch(children) => {
            let byte_len = children.iter().try_fold(0_u64, |sum, child| {
                sum.checked_add(child.byte_len)
                    .ok_or(ContentError::LengthOverflow)
            })?;
            let text = children.first().and_then(|child| child.utf16_len).is_some();
            if children
                .iter()
                .any(|child| child.utf16_len.is_some() != text)
            {
                return Err(ContentError::MalformedObject(
                    "branch mixes byte and text metrics".to_owned(),
                ));
            }
            let utf16_len = text
                .then(|| {
                    children.iter().try_fold(0_u64, |sum, child| {
                        sum.checked_add(child.utf16_len.expect("validated text child"))
                            .ok_or(ContentError::LengthOverflow)
                    })
                })
                .transpose()?;
            Ok((byte_len, utf16_len))
        }
    }
}

fn encode_object(object: &ContentObject) -> Result<Vec<u8>, ContentError> {
    let mut bytes = vec![OBJECT_FORMAT_VERSION];
    match object {
        ContentObject::Leaf {
            bytes: payload,
            utf16_len,
        } => {
            bytes.push(if utf16_len.is_some() { 2 } else { 0 });
            write_u64(&mut bytes, payload.len())?;
            if let Some(utf16_len) = utf16_len {
                bytes.extend_from_slice(&utf16_len.to_le_bytes());
            }
            bytes.extend_from_slice(payload);
        }
        ContentObject::Branch(children) => {
            bytes.push(1);
            write_u64(&mut bytes, children.len())?;
            for child in children {
                bytes.extend_from_slice(child.id.as_bytes());
                bytes.extend_from_slice(&child.byte_len.to_le_bytes());
                bytes.push(u8::from(child.utf16_len.is_some()));
                bytes.extend_from_slice(&child.utf16_len.unwrap_or(0).to_le_bytes());
            }
        }
    }
    Ok(bytes)
}

fn decode_object(bytes: &[u8], profile: ChunkingProfile) -> Result<ContentObject, ContentError> {
    if bytes.len() < 2 || bytes[0] != OBJECT_FORMAT_VERSION {
        return Err(ContentError::MalformedObject(
            "unknown object format version".to_owned(),
        ));
    }
    let mut cursor = 2;
    let count = read_u64(bytes, &mut cursor)?;
    let count = usize::try_from(count).map_err(|_| ContentError::LengthOverflow)?;
    match bytes[1] {
        tag @ (0 | 2) => {
            let utf16_len = if tag == 2 {
                Some(read_u64(bytes, &mut cursor)?)
            } else {
                None
            };
            let max_leaf = if tag == 2 {
                profile.max_leaf_bytes.saturating_add(3)
            } else {
                profile.max_leaf_bytes
            };
            if count > max_leaf || bytes.len().saturating_sub(cursor) != count {
                return Err(ContentError::MalformedObject(
                    "leaf length exceeds bounds or payload".to_owned(),
                ));
            }
            let payload = bytes[cursor..].to_vec();
            if let Some(expected) = utf16_len {
                let text = std::str::from_utf8(&payload).map_err(|_| {
                    ContentError::MalformedObject("text leaf is not UTF-8".to_owned())
                })?;
                if count_utf16(text)? != expected {
                    return Err(ContentError::MalformedObject(
                        "text leaf UTF-16 metric is incorrect".to_owned(),
                    ));
                }
            }
            Ok(ContentObject::Leaf {
                bytes: payload,
                utf16_len,
            })
        }
        1 => {
            if count == 0 || count > profile.max_children {
                return Err(ContentError::MalformedObject(
                    "branch child count exceeds bounds".to_owned(),
                ));
            }
            let expected = cursor
                .checked_add(count.checked_mul(49).ok_or(ContentError::LengthOverflow)?)
                .ok_or(ContentError::LengthOverflow)?;
            if expected != bytes.len() {
                return Err(ContentError::MalformedObject(
                    "branch descriptor bytes do not match child count".to_owned(),
                ));
            }
            let mut children = Vec::with_capacity(count);
            for _ in 0..count {
                let mut id = [0_u8; 32];
                id.copy_from_slice(&bytes[cursor..cursor + 32]);
                cursor += 32;
                let mut length = [0_u8; 8];
                length.copy_from_slice(&bytes[cursor..cursor + 8]);
                cursor += 8;
                let byte_len = u64::from_le_bytes(length);
                let has_utf16 = bytes[cursor];
                cursor += 1;
                if has_utf16 > 1 {
                    return Err(ContentError::MalformedObject(
                        "invalid UTF-16 metric tag".to_owned(),
                    ));
                }
                let mut utf16 = [0_u8; 8];
                utf16.copy_from_slice(&bytes[cursor..cursor + 8]);
                cursor += 8;
                let utf16_len = (has_utf16 == 1).then(|| u64::from_le_bytes(utf16));
                if byte_len == 0 {
                    return Err(ContentError::MalformedObject(
                        "branch child cannot have zero aggregate length".to_owned(),
                    ));
                }
                children.push(ChildDescriptor {
                    id: ContentId(id),
                    byte_len,
                    utf16_len,
                });
            }
            Ok(ContentObject::Branch(children))
        }
        _ => Err(ContentError::MalformedObject(
            "unknown object kind".to_owned(),
        )),
    }
}

fn write_u64(out: &mut Vec<u8>, value: usize) -> Result<(), ContentError> {
    let value = u64::try_from(value).map_err(|_| ContentError::LengthOverflow)?;
    out.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64, ContentError> {
    let end = cursor.checked_add(8).ok_or(ContentError::LengthOverflow)?;
    let slice = bytes.get(*cursor..end).ok_or_else(|| {
        ContentError::MalformedObject("truncated canonical object length".to_owned())
    })?;
    let mut encoded = [0_u8; 8];
    encoded.copy_from_slice(slice);
    *cursor = end;
    Ok(u64::from_le_bytes(encoded))
}

fn take_cell_bytes<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    len: usize,
) -> Result<&'a [u8], ContentError> {
    let end = cursor
        .checked_add(len)
        .ok_or(ContentError::LengthOverflow)?;
    let slice = bytes
        .get(*cursor..end)
        .ok_or_else(|| ContentError::MalformedCell("truncated cell payload".to_owned()))?;
    *cursor = end;
    Ok(slice)
}

fn leaf_ranges(bytes: &[u8], profile: ChunkingProfile) -> Vec<std::ops::Range<usize>> {
    if bytes.is_empty() {
        return std::iter::once(0..0).collect();
    }
    let mask = u64::try_from(profile.target_leaf_bytes - 1).expect("target fits u64");
    let mut ranges = Vec::new();
    let mut start = 0;
    // A finite window is what lets boundaries converge again after an insert:
    // once the inserted bytes leave the window, unchanged suffixes have the
    // same hash (and therefore the same boundaries) as before.
    const WINDOW: usize = LEAF_HASH_WINDOW;
    let mut rolling = 0_u64;
    let mut window = [0_u8; WINDOW];
    let mut window_len = 0_usize;
    let mut window_cursor = 0_usize;
    for (index, byte) in bytes.iter().copied().enumerate() {
        if window_len < WINDOW {
            rolling = rolling.rotate_left(1) ^ gear(byte);
            window_len += 1;
        } else {
            let outgoing = window[window_cursor];
            rolling =
                rolling.rotate_left(1) ^ gear(byte) ^ gear(outgoing).rotate_left(WINDOW as u32);
        }
        window[window_cursor] = byte;
        window_cursor = (window_cursor + 1) % WINDOW;
        let len = index + 1 - start;
        let boundary = len >= profile.min_leaf_bytes && rolling & mask == 0;
        if boundary || len >= profile.max_leaf_bytes {
            ranges.push(start..index + 1);
            start = index + 1;
            rolling = 0;
            window_len = 0;
            window_cursor = 0;
        }
    }
    if start < bytes.len() {
        ranges.push(start..bytes.len());
    }
    ranges
}

fn text_leaf_ranges(text: &str, profile: ChunkingProfile) -> Vec<std::ops::Range<usize>> {
    let bytes = text.as_bytes();
    let raw = leaf_ranges(bytes, profile);
    if raw.len() < 2 {
        return raw;
    }
    let mut ranges = Vec::with_capacity(raw.len());
    let mut start = 0;
    for range in raw.iter().take(raw.len() - 1) {
        let aligned = (start + 1..=range.end)
            .rev()
            .find(|candidate| text.is_char_boundary(*candidate))
            .unwrap_or_else(|| {
                (range.end..=bytes.len())
                    .find(|candidate| text.is_char_boundary(*candidate))
                    .expect("the end of valid UTF-8 is a boundary")
            });
        ranges.push(start..aligned);
        start = aligned;
    }
    ranges.push(start..bytes.len());
    ranges
}

fn count_utf16(text: &str) -> Result<u64, ContentError> {
    u64::try_from(text.encode_utf16().count()).map_err(|_| ContentError::LengthOverflow)
}

fn descriptor_groups(
    children: &[ChildDescriptor],
    profile: ChunkingProfile,
) -> Vec<std::ops::Range<usize>> {
    let mut groups = Vec::new();
    let mut start = 0;
    for (index, child) in children.iter().enumerate() {
        // A descriptor is the indivisible unit at this level. Its stable
        // identity provides a boundary predicate that survives insertions and
        // deletions of neighboring descriptors.
        let len = index + 1 - start;
        let boundary = len >= profile.min_children && descriptor_boundary(child, profile);
        if boundary || len >= profile.max_children {
            groups.push(start..index + 1);
            start = index + 1;
        }
    }
    if start < children.len() {
        groups.push(start..children.len());
    }
    groups
}

fn descriptor_boundary(child: &ChildDescriptor, profile: ChunkingProfile) -> bool {
    let mask = u64::try_from(profile.target_children - 1).expect("target fits u64");
    let mut rolling = 0_u64;
    for byte in child
        .id
        .as_bytes()
        .iter()
        .copied()
        .chain(child.byte_len.to_le_bytes())
        .chain([u8::from(child.utf16_len.is_some())])
        .chain(child.utf16_len.unwrap_or(0).to_le_bytes())
    {
        rolling = rolling.rotate_left(1) ^ gear(byte);
    }
    rolling & mask == 0
}

fn gear(byte: u8) -> u64 {
    let mut value = u64::from(byte).wrapping_add(0x9e37_79b9_7f4a_7c15);
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tiny_profile() -> ChunkingProfile {
        ChunkingProfile {
            min_leaf_bytes: 4,
            target_leaf_bytes: 8,
            max_leaf_bytes: 16,
            min_children: 2,
            target_children: 4,
            max_children: 8,
        }
    }

    fn domain() -> ContentDomain {
        ContentDomain::new(b"test-domain".to_vec()).unwrap()
    }

    fn collect_leaf_ids(
        tree: ContentTree,
        store: &MemoryContentStore,
        id: ContentId,
        out: &mut Vec<ContentId>,
    ) {
        match tree.load_object(&domain(), id, store).unwrap() {
            ContentObject::Leaf { .. } => out.push(id),
            ContentObject::Branch(children) => {
                for child in children {
                    collect_leaf_ids(tree, store, child.id, out);
                }
            }
        }
    }

    #[test]
    fn tree_is_history_independent_and_ranges_are_lazy_values() {
        let tree = ContentTree::new(tiny_profile()).unwrap();
        let bytes = (0..=255).cycle().take(4096).collect::<Vec<_>>();
        let mut first = MemoryContentStore::default();
        let mut second = MemoryContentStore::default();
        let (root_a, len_a) = tree.build(&domain(), &bytes, &mut first).unwrap();
        let (root_b, len_b) = tree.build(&domain(), &bytes, &mut second).unwrap();
        assert_eq!((root_a, len_a), (root_b, len_b));
        assert_eq!(
            tree.read_range(&domain(), root_a, len_a, 997, 211, &first)
                .unwrap(),
            bytes[997..1208]
        );
    }

    // Internal because the public behavior being protected is the exact
    // content-addressed format identity, not a user-visible database flow.
    #[test]
    fn streaming_build_matches_slice_build_across_transport_chunk_sizes() {
        let tree = ContentTree::new(tiny_profile()).unwrap();
        let bytes = (0..=255).cycle().take(16_777).collect::<Vec<_>>();
        let mut expected_store = MemoryContentStore::default();
        let expected = tree.build(&domain(), &bytes, &mut expected_store).unwrap();

        for chunk_size in [1, 3, 17, 64, 513, 4096] {
            let mut actual_store = MemoryContentStore::default();
            let value = LargeValue::create_streaming_bytes(
                &domain(),
                bytes.chunks(chunk_size),
                tree,
                &mut actual_store,
            )
            .unwrap();
            let LargeValue::Chunked(large) = value else {
                panic!("streaming create must be chunked");
            };
            let actual = (large.root, large.root_byte_len);
            assert_eq!(actual, expected, "chunk size {chunk_size}");
            assert_eq!(
                tree.materialize(&domain(), actual.0, actual.1, &actual_store)
                    .unwrap(),
                bytes,
            );
        }
    }

    // Internal because it guards exact content-addressed format identity. The
    // 68-byte case crosses a tiny-profile branch boundary and used to leave a
    // promoted group above an empty intermediate level during finalization.
    #[test]
    fn streaming_build_exhaustively_matches_batch_around_branch_promotions() {
        let tree = ContentTree::new(tiny_profile()).unwrap();
        for len in 0..400 {
            let bytes = (0..=255).cycle().take(len).collect::<Vec<_>>();
            let mut expected_store = MemoryContentStore::default();
            let expected = tree.build(&domain(), &bytes, &mut expected_store).unwrap();
            for chunk_size in [1, 2, 3, 5, 7, 8, 11, 16, 31, 64, 127, 512] {
                let mut actual_store = MemoryContentStore::default();
                let actual = tree
                    .build_streaming(&domain(), bytes.chunks(chunk_size), &mut actual_store)
                    .unwrap();
                assert_eq!(actual, expected, "length {len}, chunk size {chunk_size}");
            }
        }
    }

    #[test]
    fn declared_child_lengths_must_match_canonical_objects() {
        let tree = ContentTree::new(tiny_profile()).unwrap();
        let mut store = MemoryContentStore::default();
        let leaf = encode_object(&ContentObject::Leaf {
            bytes: b"abc".to_vec(),
            utf16_len: None,
        })
        .unwrap();
        let leaf_id = object_id(&domain(), &leaf);
        store.put_if_absent_or_identical(leaf_id, &leaf).unwrap();
        let branch = encode_object(&ContentObject::Branch(vec![ChildDescriptor {
            id: leaf_id,
            byte_len: 2,
            utf16_len: None,
        }]))
        .unwrap();
        let root = object_id(&domain(), &branch);
        store.put_if_absent_or_identical(root, &branch).unwrap();

        assert!(matches!(
            tree.materialize(&domain(), root, 2, &store),
            Err(ContentError::MalformedObject(message))
                if message.contains("child aggregate length")
        ));
    }

    #[test]
    fn patched_range_does_not_load_unrelated_leaf_payloads() {
        let tree = ContentTree::new(tiny_profile()).unwrap();
        let mut store = MemoryContentStore::default();
        let bytes = (0..=255).cycle().take(4096).collect::<Vec<_>>();
        let value = LargeValue::create(ValueKind::Bytes, &domain(), bytes, 4, tree, &mut store)
            .unwrap()
            .apply_edit(
                ValueKind::Bytes,
                &domain(),
                BytePatch::insert(0, b"prefix"),
                4,
                TailBounds::default(),
                tree,
                &mut store,
            )
            .unwrap();
        let LargeValue::Chunked(large) = &value else {
            panic!("value must be large")
        };
        let mut leaves = Vec::new();
        collect_leaf_ids(tree, &store, large.root, &mut leaves);
        assert!(leaves.len() > 2);
        store.objects.remove(leaves.last().unwrap());

        assert_eq!(
            value
                .select(
                    ValueKind::Bytes,
                    &ValueSelection::ByteRange { offset: 0, len: 6 },
                    &domain(),
                    tree,
                    &store,
                )
                .unwrap(),
            ValueSelectionResult::Bytes(b"prefix".to_vec())
        );
        assert!(matches!(
            value.materialize(ValueKind::Bytes, &domain(), tree, &store),
            Err(ContentError::MissingObject(_))
        ));
    }

    #[test]
    fn large_value_tail_is_ordered_and_consolidates_at_the_bound() {
        let tree = ContentTree::new(tiny_profile()).unwrap();
        let mut store = MemoryContentStore::default();
        let value = LargeValue::create(
            ValueKind::String,
            &domain(),
            "abcdefghijklmnop",
            4,
            tree,
            &mut store,
        )
        .unwrap();
        let value = value
            .apply_edit(
                ValueKind::String,
                &domain(),
                BytePatch::insert(2, b"XX"),
                4,
                TailBounds {
                    max_entries: 1,
                    max_encoded_bytes: 100,
                },
                tree,
                &mut store,
            )
            .unwrap();
        assert!(matches!(&value, LargeValue::Chunked(value) if value.edit_tail.len() == 1));
        let value = value
            .apply_edit(
                ValueKind::String,
                &domain(),
                BytePatch::delete(0, 1),
                4,
                TailBounds {
                    max_entries: 1,
                    max_encoded_bytes: 100,
                },
                tree,
                &mut store,
            )
            .unwrap();
        assert!(matches!(&value, LargeValue::Chunked(value) if value.edit_tail.is_empty()));
        assert_eq!(
            value
                .materialize(ValueKind::String, &domain(), tree, &store)
                .unwrap(),
            b"bXXcdefghijklmnop"
        );
    }

    #[test]
    fn text_edits_lower_to_utf8_byte_patches() {
        let patch = text_replace_patch_utf16("a🦀z", 1, 2, "é").unwrap();
        assert_eq!(patch.offset, 1);
        assert_eq!(patch.delete_len, 4);
        assert_eq!(
            apply_patches("a🦀z".as_bytes(), &[patch]).unwrap(),
            "aéz".as_bytes()
        );
    }

    #[test]
    fn text_ranges_expose_explicit_utf8_and_utf16_coordinates() {
        let tree = ContentTree::new(tiny_profile()).unwrap();
        let mut store = MemoryContentStore::default();
        let source = "a🦀e\u{301}Z—終";
        let value =
            LargeValue::create(ValueKind::String, &domain(), source, 1, tree, &mut store).unwrap();

        let boundaries = source
            .char_indices()
            .map(|(byte, _)| byte)
            .chain(std::iter::once(source.len()))
            .collect::<Vec<_>>();
        for start_index in 0..boundaries.len() {
            for end_index in start_index..boundaries.len() {
                let start = boundaries[start_index];
                let end = boundaries[end_index];
                let expected = &source[start..end];
                assert_eq!(
                    value
                        .select(
                            ValueKind::String,
                            &ValueSelection::TextUtf8Range {
                                offset: start as u64,
                                len: (end - start) as u64,
                            },
                            &domain(),
                            tree,
                            &store,
                        )
                        .unwrap(),
                    ValueSelectionResult::String(expected.to_owned())
                );

                let utf16_start = source[..start].encode_utf16().count() as u64;
                let utf16_len = expected.encode_utf16().count() as u64;
                assert_eq!(
                    value
                        .select(
                            ValueKind::String,
                            &ValueSelection::TextUtf16Range {
                                offset: utf16_start,
                                len: utf16_len,
                            },
                            &domain(),
                            tree,
                            &store,
                        )
                        .unwrap(),
                    ValueSelectionResult::String(expected.to_owned())
                );
            }
        }

        assert!(matches!(
            value.select(
                ValueKind::String,
                &ValueSelection::TextUtf8Range { offset: 2, len: 0 },
                &domain(),
                tree,
                &store,
            ),
            Err(ContentError::TextOffsetOutOfBounds {
                encoding: "UTF-8",
                ..
            })
        ));
        assert!(matches!(
            value.select(
                ValueKind::String,
                &ValueSelection::TextUtf16Range { offset: 2, len: 0 },
                &domain(),
                tree,
                &store,
            ),
            Err(ContentError::TextOffsetOutOfBounds {
                encoding: "UTF-16",
                ..
            })
        ));
    }

    #[test]
    fn utf16_slice_relative_edits_survive_tail_materialization_and_consolidation() {
        let tree = ContentTree::new(tiny_profile()).unwrap();
        let mut store = MemoryContentStore::default();
        let original = "zero 🦀 one 🐙 two — end".repeat(40);
        let mut expected = original.clone();
        let mut value = LargeValue::create(
            ValueKind::String,
            &domain(),
            original.as_bytes(),
            4,
            tree,
            &mut store,
        )
        .unwrap();
        let bounds = TailBounds {
            max_entries: 2,
            max_encoded_bytes: 1024,
        };

        for (offset, delete_len, insert) in [(5, 2, "é"), (11, 0, "🧑‍🔬"), (1, 3, "XYZ")] {
            let total = expected.encode_utf16().count() as u64;
            let edit = ValueEdit::TextUtf16 {
                slice_offset: 0,
                slice_len: total,
                offset,
                delete_len,
                insert: insert.to_owned(),
            };
            let patch = value
                .lower_edit(ValueKind::String, edit, &domain(), tree, &store)
                .unwrap();
            let start = utf16_to_byte_offset(&expected, offset).unwrap();
            let end = utf16_to_byte_offset(&expected, offset + delete_len).unwrap();
            expected.replace_range(start..end, insert);
            value = value
                .apply_edit(
                    ValueKind::String,
                    &domain(),
                    patch,
                    4,
                    bounds,
                    tree,
                    &mut store,
                )
                .unwrap();
            assert_eq!(
                value
                    .materialize(ValueKind::String, &domain(), tree, &store)
                    .unwrap(),
                expected.as_bytes()
            );
            let selected_len = expected.encode_utf16().count().min(40) as u64;
            assert_eq!(
                value
                    .select(
                        ValueKind::String,
                        &ValueSelection::TextUtf16Range {
                            offset: 0,
                            len: selected_len,
                        },
                        &domain(),
                        tree,
                        &store,
                    )
                    .unwrap(),
                ValueSelectionResult::String(
                    expected[..utf16_to_byte_offset(&expected, selected_len).unwrap()].to_owned()
                )
            );
        }

        let LargeValue::Chunked(consolidated) = &value else {
            panic!("large text remains chunked")
        };
        assert!(consolidated.edit_tail.is_empty());
        assert_eq!(
            consolidated.root_utf16_len,
            Some(expected.encode_utf16().count() as u64)
        );
    }

    #[test]
    fn utf16_tail_range_does_not_load_an_unrelated_leaf() {
        let tree = ContentTree::new(tiny_profile()).unwrap();
        let mut store = MemoryContentStore::default();
        let source = "abcdefghijklmnop".repeat(256);
        let value = LargeValue::create(ValueKind::String, &domain(), source, 4, tree, &mut store)
            .unwrap()
            .apply_edit(
                ValueKind::String,
                &domain(),
                BytePatch::insert(0, "🦀".as_bytes()),
                4,
                TailBounds::default(),
                tree,
                &mut store,
            )
            .unwrap();
        let LargeValue::Chunked(large) = &value else {
            panic!("value must be chunked")
        };
        let mut leaves = Vec::new();
        collect_leaf_ids(tree, &store, large.root, &mut leaves);
        store.objects.remove(leaves.last().unwrap());

        assert_eq!(
            value
                .select(
                    ValueKind::String,
                    &ValueSelection::TextUtf16Range { offset: 0, len: 2 },
                    &domain(),
                    tree,
                    &store,
                )
                .unwrap(),
            ValueSelectionResult::String("🦀".to_owned())
        );
        assert!(matches!(
            value.materialize(ValueKind::String, &domain(), tree, &store),
            Err(ContentError::MissingObject(_))
        ));
    }

    #[test]
    fn mixed_unicode_edit_tail_matches_plain_string_model_exhaustively() {
        let tree = ContentTree::new(tiny_profile()).unwrap();
        let mut store = MemoryContentStore::default();
        let mut expected = "A🦀e\u{301}—終z".repeat(80);
        let mut value = LargeValue::create(
            ValueKind::String,
            &domain(),
            expected.as_bytes(),
            4,
            tree,
            &mut store,
        )
        .unwrap();
        let bounds = TailBounds {
            max_entries: 7,
            max_encoded_bytes: 512,
        };
        let inserts = ["", "x", "é", "🧑‍🔬", "終🙂"];

        for step in 0..100_usize {
            let mut boundaries = vec![(0_u64, 0_usize)];
            let mut utf16 = 0_u64;
            for (byte, character) in expected.char_indices() {
                if byte != 0 {
                    boundaries.push((utf16, byte));
                }
                utf16 += character.len_utf16() as u64;
            }
            boundaries.push((utf16, expected.len()));
            boundaries.sort_unstable();
            boundaries.dedup();
            let start_index = (step * 17) % boundaries.len();
            let end_index = (start_index + (step % 4)).min(boundaries.len() - 1);
            let (start_utf16, start_byte) = boundaries[start_index];
            let (end_utf16, end_byte) = boundaries[end_index];
            let insert = inserts[step % inserts.len()];
            let total_utf16 = boundaries.last().unwrap().0;
            let patch = value
                .lower_edit(
                    ValueKind::String,
                    ValueEdit::TextUtf16 {
                        slice_offset: 0,
                        slice_len: total_utf16,
                        offset: start_utf16,
                        delete_len: end_utf16 - start_utf16,
                        insert: insert.to_owned(),
                    },
                    &domain(),
                    tree,
                    &store,
                )
                .unwrap();
            expected.replace_range(start_byte..end_byte, insert);
            value = value
                .apply_edit(
                    ValueKind::String,
                    &domain(),
                    patch,
                    4,
                    bounds,
                    tree,
                    &mut store,
                )
                .unwrap();
            assert_eq!(
                value.utf16_len().unwrap(),
                expected.encode_utf16().count() as u64
            );
            assert_eq!(
                value
                    .materialize(ValueKind::String, &domain(), tree, &store)
                    .unwrap(),
                expected.as_bytes(),
                "step {step}"
            );
        }
    }

    #[test]
    fn json_validation_observes_the_complete_atomic_tail() {
        let tree = ContentTree::new(tiny_profile()).unwrap();
        let mut store = MemoryContentStore::default();
        let value = LargeValue::create(
            ValueKind::Json,
            &domain(),
            br#"{"a":1}"#.to_vec(),
            4,
            tree,
            &mut store,
        )
        .unwrap();
        let value = value
            .apply_edit(
                ValueKind::Json,
                &domain(),
                BytePatch::replace(5, 1, b"2"),
                4,
                TailBounds::default(),
                tree,
                &mut store,
            )
            .unwrap();
        assert_eq!(
            value
                .materialize(ValueKind::Json, &domain(), tree, &store)
                .unwrap(),
            br#"{"a":2}"#
        );
    }

    #[test]
    fn local_insert_reuses_most_content_defined_objects() {
        // Internal evidence is appropriate here: object identity reuse is the
        // physical invariant under test and has no public row API observation.
        let tree = ContentTree::new(tiny_profile()).unwrap();
        let original = (0..=255).cycle().take(16 * 1024).collect::<Vec<_>>();
        let mut edited = original.clone();
        edited.splice(257..257, [99, 98, 97]);
        let mut store = MemoryContentStore::default();
        tree.build(&domain(), &original, &mut store).unwrap();
        let before = store
            .objects
            .keys()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        tree.build(&domain(), &edited, &mut store).unwrap();
        let after = store
            .objects
            .keys()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        let reused = before.intersection(&after).count();
        assert!(
            reused * 4 > before.len() * 3,
            "expected at least 75% object reuse, reused {reused} of {}",
            before.len()
        );
    }

    #[test]
    fn projections_and_declarative_edits_use_native_values() {
        let tree = ContentTree::new(tiny_profile()).unwrap();
        let mut store = MemoryContentStore::default();
        let text = LargeValue::create(
            ValueKind::String,
            &domain(),
            "zero 🦀 two",
            4,
            tree,
            &mut store,
        )
        .unwrap();
        assert_eq!(
            text.select(
                ValueKind::String,
                &ValueSelection::TextUtf16Range { offset: 5, len: 2 },
                &domain(),
                tree,
                &store,
            )
            .unwrap(),
            ValueSelectionResult::String("🦀".to_owned())
        );

        let json = LargeValue::create(
            ValueKind::Json,
            &domain(),
            br#"{"profile":{"name":"old"},"stable":1}"#.to_vec(),
            4,
            tree,
            &mut store,
        )
        .unwrap();
        assert_eq!(
            json.select(
                ValueKind::Json,
                &ValueSelection::JsonPointer("/profile/name".to_owned()),
                &domain(),
                tree,
                &store,
            )
            .unwrap(),
            ValueSelectionResult::Json(serde_json::json!("old"))
        );
        let patch = json
            .lower_edit(
                ValueKind::Json,
                ValueEdit::Json(serde_json::json!({
                    "profile": { "name": "new" },
                    "stable": 1
                })),
                &domain(),
                tree,
                &store,
            )
            .unwrap();
        let current = json
            .materialize(ValueKind::Json, &domain(), tree, &store)
            .unwrap();
        let next = apply_patches(&current, &[patch]).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&next).unwrap(),
            serde_json::json!({ "profile": { "name": "new" }, "stable": 1 })
        );
    }

    #[test]
    fn json_merge_analysis_attributes_independent_changes_and_conflicts() {
        let independent = analyze_json_merge(
            br#"{"name":"old","profile":{"city":"A"}}"#,
            br#"{"name":"alice","profile":{"city":"A"}}"#,
            br#"{"name":"old","profile":{"city":"B"}}"#,
        )
        .unwrap();
        assert!(independent.conflicts.is_empty());
        assert_eq!(independent.independent.len(), 2);
        assert!(
            independent
                .independent
                .iter()
                .any(|change| { change.side == JsonSide::A && change.pointer == "/name" })
        );
        assert!(
            independent
                .independent
                .iter()
                .any(|change| { change.side == JsonSide::B && change.pointer == "/profile/city" })
        );

        let conflict = analyze_json_merge(
            br#"{"name":"old"}"#,
            br#"{"name":"alice"}"#,
            br#"{"name":"bob"}"#,
        )
        .unwrap();
        assert_eq!(conflict.conflicts.len(), 1);
        assert!(conflict.independent.is_empty());
    }

    #[test]
    fn atomic_cell_round_trips_without_a_legacy_fallback() {
        let schema = LargeValueSchema::built_in(ValueKind::Bytes);
        let cell = LargeValue::Chunked(ChunkedValue {
            root: ContentId([7; 32]),
            root_byte_len: 42,
            root_utf16_len: None,
            edit_tail: vec![BytePatch::insert(4, b"next")],
        });
        let encoded = cell.encode_cell().unwrap();
        assert_eq!(LargeValue::decode_cell(&schema, &encoded).unwrap(), cell);
        assert_eq!(
            LargeValue::decode_cell(&schema, &encoded[CELL_ENVELOPE.len()..]),
            Err(ContentError::UnknownCellFormat)
        );
    }

    #[test]
    fn decoded_cells_obey_the_schema_format_and_tail_bounds() {
        let mut schema = LargeValueSchema::built_in(ValueKind::Bytes);
        let cell = LargeValue::Chunked(ChunkedValue {
            root: ContentId([7; 32]),
            root_byte_len: 42,
            root_utf16_len: None,
            edit_tail: (0..=schema.max_tail_entries)
                .map(|_| BytePatch::insert(0, b"x"))
                .collect(),
        });
        let encoded = cell.encode_cell().unwrap();
        assert_eq!(
            LargeValue::decode_cell(&schema, &encoded),
            Err(ContentError::TailTooLarge)
        );

        schema.tree_format += 1;
        assert_eq!(
            LargeValue::decode_cell(&schema, &encoded),
            Err(ContentError::UnsupportedTreeFormat(schema.tree_format))
        );

        let schema = LargeValueSchema::built_in(ValueKind::Bytes);
        let mut hostile = CELL_ENVELOPE.to_vec();
        hostile.push(1);
        hostile.extend_from_slice(&[0; 32]);
        hostile.extend_from_slice(&0_u64.to_le_bytes());
        hostile.push(0);
        hostile.extend_from_slice(&0_u64.to_le_bytes());
        hostile.extend_from_slice(&u64::MAX.to_le_bytes());
        assert_eq!(
            LargeValue::decode_cell(&schema, &hostile),
            Err(ContentError::TailTooLarge)
        );

        let oversized_bytes = LargeValue::Chunked(ChunkedValue {
            root: ContentId([7; 32]),
            root_byte_len: 42,
            root_utf16_len: None,
            edit_tail: (0..schema.max_tail_entries)
                .map(|_| BytePatch::insert(0, vec![b'x'; 250]))
                .collect(),
        })
        .encode_cell()
        .unwrap();
        assert_eq!(
            LargeValue::decode_cell(&schema, &oversized_bytes),
            Err(ContentError::TailTooLarge)
        );
    }

    #[test]
    fn untrusted_descriptor_length_does_not_drive_prevalidation_allocation() {
        let value = LargeValue::Chunked(ChunkedValue {
            root: ContentId([9; 32]),
            root_byte_len: u64::MAX,
            root_utf16_len: None,
            edit_tail: Vec::new(),
        });
        assert!(matches!(
            value.materialize(
                ValueKind::Bytes,
                &domain(),
                ContentTree::new(tiny_profile()).unwrap(),
                &MemoryContentStore::default(),
            ),
            Err(ContentError::MissingObject(_))
        ));
    }

    #[test]
    fn ordered_kv_adapter_enforces_absent_or_identical_objects() {
        use groove::storage::MemoryStorage;

        let storage = MemoryStorage::new(&[CONTENT_OBJECTS_CF]);
        let mut store = KvContentStore::new(&storage);
        let id = ContentId([3; 32]);
        store.put_if_absent_or_identical(id, b"same").unwrap();
        store.put_if_absent_or_identical(id, b"same").unwrap();
        assert_eq!(store.get(id).unwrap(), Some(b"same".to_vec()));
        assert_eq!(
            store.put_if_absent_or_identical(id, b"different"),
            Err(ContentError::ImmutableCollision(id))
        );
    }
}
