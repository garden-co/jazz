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
const OBJECT_FORMAT_VERSION: u8 = 1;
const CELL_ENVELOPE: &[u8] = b"JAZZ-ADAPTIVE-SCALAR-V2\0";
const PATCH_FRAME_HEADER_BYTES: usize = 3 * std::mem::size_of::<u64>();
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
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ContentObject {
    Leaf(Vec<u8>),
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
}

impl BytePatch {
    /// Insert bytes without deleting existing content.
    pub fn insert(offset: u64, bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            offset,
            delete_len: 0,
            insert: bytes.into(),
        }
    }

    /// Delete one byte range.
    pub fn delete(offset: u64, delete_len: u64) -> Self {
        Self {
            offset,
            delete_len,
            insert: Vec::new(),
        }
    }

    /// Replace one byte range.
    pub fn replace(offset: u64, delete_len: u64, bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            offset,
            delete_len,
            insert: bytes.into(),
        }
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
            tree_format: 1,
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
    /// Return one Unicode-scalar text range.
    TextRange {
        /// Inclusive Unicode-scalar start.
        offset: u64,
        /// Requested Unicode-scalar count.
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
    /// Replace a Unicode-scalar text range.
    Text {
        /// Inclusive Unicode-scalar start.
        offset: u64,
        /// Unicode-scalar count to remove.
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
    /// Side-attributed changes whose paths do not overlap incompatibly.
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
    /// A text scalar position exceeds the string.
    #[error("text scalar offset {offset} exceeds scalar length {scalar_len}")]
    ScalarOffsetOutOfBounds {
        /// Requested scalar offset.
        offset: u64,
        /// Current Unicode-scalar length.
        scalar_len: u64,
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
pub struct ProllyTree {
    profile: ChunkingProfile,
}

impl ProllyTree {
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
        let chunks = leaf_ranges(bytes, self.profile);
        let mut level = Vec::with_capacity(chunks.len());
        for range in chunks {
            let object = ContentObject::Leaf(bytes[range].to_vec());
            level.push(self.persist_object(domain, object, store)?);
        }
        if level.is_empty() {
            level.push(self.persist_object(domain, ContentObject::Leaf(Vec::new()), store)?);
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
        let root = level.pop().expect("tree always has one root");
        Ok((root.id, root.byte_len))
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
                "root aggregate length does not match manifest".to_owned(),
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

    fn persist_object<S: ImmutableContentStore>(
        &self,
        domain: &ContentDomain,
        object: ContentObject,
        store: &mut S,
    ) -> Result<ChildDescriptor, ContentError> {
        let byte_len = object_len(&object)?;
        let canonical = encode_object(&object)?;
        let id = object_id(domain, &canonical);
        store.put_if_absent_or_identical(id, &canonical)?;
        Ok(ChildDescriptor { id, byte_len })
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
        let actual_len = object_len(&object)?;
        if expected_len.is_some_and(|expected| expected != actual_len) {
            return Err(ContentError::MalformedObject(
                "child aggregate length does not match descriptor".to_owned(),
            ));
        }
        match object {
            ContentObject::Leaf(bytes) => {
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

impl LargeValue {
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
                    let insert = take_cell_bytes(payload, &mut cursor, insert_len)?.to_vec();
                    edit_tail.push(BytePatch {
                        offset,
                        delete_len,
                        insert,
                    });
                }
                let large = ChunkedValue {
                    root: ContentId(root),
                    root_byte_len,
                    edit_tail,
                };
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
        tree: ProllyTree,
        store: &mut S,
    ) -> Result<Self, ContentError> {
        let bytes = bytes.into();
        kind.validate(&bytes)?;
        if bytes.len() <= inline_up_to {
            return Ok(Self::Inline(bytes));
        }
        let (root, root_byte_len) = tree.build(domain, &bytes, store)?;
        Ok(Self::Chunked(ChunkedValue {
            root,
            root_byte_len,
            edit_tail: Vec::new(),
        }))
    }

    /// Materialize and validate the complete logical value.
    pub fn materialize<S: ImmutableContentStore>(
        &self,
        kind: ValueKind,
        domain: &ContentDomain,
        tree: ProllyTree,
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
        tree: ProllyTree,
        store: &S,
    ) -> Result<ValueSelectionResult, ContentError> {
        if let (ValueKind::Bytes, ValueSelection::ByteRange { offset, len }) = (kind, selection) {
            let selected = match self {
                Self::Inline(bytes) => checked_slice(bytes, *offset, *len)?.to_vec(),
                Self::Chunked(large) => {
                    materialize_large_range(large, domain, tree, store, *offset, *len)?
                }
            };
            return Ok(ValueSelectionResult::Bytes(selected));
        }
        let bytes = self.materialize(kind, domain, tree, store)?;
        match (kind, selection) {
            (ValueKind::Bytes, ValueSelection::Value) => Ok(ValueSelectionResult::Bytes(bytes)),
            (ValueKind::Bytes, ValueSelection::ByteRange { .. }) => unreachable!(),
            (ValueKind::String, ValueSelection::Value) => Ok(ValueSelectionResult::String(
                String::from_utf8(bytes).map_err(|_| ContentError::InvalidUtf8)?,
            )),
            (ValueKind::String, ValueSelection::TextRange { offset, len }) => {
                let text = std::str::from_utf8(&bytes).map_err(|_| ContentError::InvalidUtf8)?;
                let start = scalar_to_byte_offset(text, *offset)?;
                let end_scalar = offset
                    .checked_add(*len)
                    .ok_or(ContentError::LengthOverflow)?;
                let end = scalar_to_byte_offset(text, end_scalar)?;
                Ok(ValueSelectionResult::String(text[start..end].to_owned()))
            }
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
        tree: ProllyTree,
        store: &S,
    ) -> Result<BytePatch, ContentError> {
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
            (
                ValueKind::String,
                ValueEdit::Text {
                    offset,
                    delete_len,
                    insert,
                },
            ) => text_replace_patch(
                std::str::from_utf8(&bytes).map_err(|_| ContentError::InvalidUtf8)?,
                offset,
                delete_len,
                &insert,
            ),
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
        patch: BytePatch,
        inline_up_to: usize,
        tail_bounds: TailBounds,
        tree: ProllyTree,
        store: &mut S,
    ) -> Result<Self, ContentError> {
        let current = self.materialize(kind, domain, tree, store)?;
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
                        edit_tail: tail,
                    }))
                } else {
                    let (root, root_byte_len) = tree.build(domain, &next, store)?;
                    Ok(Self::Chunked(ChunkedValue {
                        root,
                        root_byte_len,
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
    tree: ProllyTree,
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

/// Lower one Unicode-scalar text replacement to the universal byte patch.
pub fn text_replace_patch(
    text: &str,
    scalar_offset: u64,
    delete_scalars: u64,
    insert: &str,
) -> Result<BytePatch, ContentError> {
    let start = scalar_to_byte_offset(text, scalar_offset)?;
    let end_scalar = scalar_offset
        .checked_add(delete_scalars)
        .ok_or(ContentError::LengthOverflow)?;
    let end = scalar_to_byte_offset(text, end_scalar)?;
    Ok(BytePatch::replace(
        u64::try_from(start).map_err(|_| ContentError::LengthOverflow)?,
        u64::try_from(end - start).map_err(|_| ContentError::LengthOverflow)?,
        insert.as_bytes(),
    ))
}

fn scalar_to_byte_offset(text: &str, offset: u64) -> Result<usize, ContentError> {
    if offset == 0 {
        return Ok(0);
    }
    let scalar_len =
        u64::try_from(text.chars().count()).map_err(|_| ContentError::LengthOverflow)?;
    if offset == scalar_len {
        return Ok(text.len());
    }
    text.char_indices()
        .nth(usize::try_from(offset).map_err(|_| ContentError::LengthOverflow)?)
        .map(|(byte, _)| byte)
        .ok_or(ContentError::ScalarOffsetOutOfBounds { offset, scalar_len })
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

fn object_len(object: &ContentObject) -> Result<u64, ContentError> {
    match object {
        ContentObject::Leaf(bytes) => {
            u64::try_from(bytes.len()).map_err(|_| ContentError::LengthOverflow)
        }
        ContentObject::Branch(children) => children.iter().try_fold(0_u64, |sum, child| {
            sum.checked_add(child.byte_len)
                .ok_or(ContentError::LengthOverflow)
        }),
    }
}

fn encode_object(object: &ContentObject) -> Result<Vec<u8>, ContentError> {
    let mut bytes = vec![OBJECT_FORMAT_VERSION];
    match object {
        ContentObject::Leaf(payload) => {
            bytes.push(0);
            write_u64(&mut bytes, payload.len())?;
            bytes.extend_from_slice(payload);
        }
        ContentObject::Branch(children) => {
            bytes.push(1);
            write_u64(&mut bytes, children.len())?;
            for child in children {
                bytes.extend_from_slice(child.id.as_bytes());
                bytes.extend_from_slice(&child.byte_len.to_le_bytes());
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
        0 => {
            if count > profile.max_leaf_bytes || bytes.len().saturating_sub(cursor) != count {
                return Err(ContentError::MalformedObject(
                    "leaf length exceeds bounds or payload".to_owned(),
                ));
            }
            Ok(ContentObject::Leaf(bytes[cursor..].to_vec()))
        }
        1 => {
            if count == 0 || count > profile.max_children {
                return Err(ContentError::MalformedObject(
                    "branch child count exceeds bounds".to_owned(),
                ));
            }
            let expected = cursor
                .checked_add(count.checked_mul(40).ok_or(ContentError::LengthOverflow)?)
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
                if byte_len == 0 {
                    return Err(ContentError::MalformedObject(
                        "branch child cannot have zero aggregate length".to_owned(),
                    ));
                }
                children.push(ChildDescriptor {
                    id: ContentId(id),
                    byte_len,
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
    const WINDOW: usize = 63;
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

fn descriptor_groups(
    children: &[ChildDescriptor],
    profile: ChunkingProfile,
) -> Vec<std::ops::Range<usize>> {
    let mask = u64::try_from(profile.target_children - 1).expect("target fits u64");
    let mut groups = Vec::new();
    let mut start = 0;
    for (index, child) in children.iter().enumerate() {
        // A descriptor is the indivisible unit at this level. Its stable
        // identity provides a boundary predicate that survives insertions and
        // deletions of neighboring descriptors.
        let mut rolling = 0_u64;
        for byte in child
            .id
            .as_bytes()
            .iter()
            .copied()
            .chain(child.byte_len.to_le_bytes())
        {
            rolling = rolling.rotate_left(1) ^ gear(byte);
        }
        let len = index + 1 - start;
        let boundary = len >= profile.min_children && rolling & mask == 0;
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
        tree: ProllyTree,
        store: &MemoryContentStore,
        id: ContentId,
        out: &mut Vec<ContentId>,
    ) {
        match tree.load_object(&domain(), id, store).unwrap() {
            ContentObject::Leaf(_) => out.push(id),
            ContentObject::Branch(children) => {
                for child in children {
                    collect_leaf_ids(tree, store, child.id, out);
                }
            }
        }
    }

    #[test]
    fn tree_is_history_independent_and_ranges_are_lazy_values() {
        let tree = ProllyTree::new(tiny_profile()).unwrap();
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

    #[test]
    fn declared_child_lengths_must_match_canonical_objects() {
        let tree = ProllyTree::new(tiny_profile()).unwrap();
        let mut store = MemoryContentStore::default();
        let leaf = encode_object(&ContentObject::Leaf(b"abc".to_vec())).unwrap();
        let leaf_id = object_id(&domain(), &leaf);
        store.put_if_absent_or_identical(leaf_id, &leaf).unwrap();
        let branch = encode_object(&ContentObject::Branch(vec![ChildDescriptor {
            id: leaf_id,
            byte_len: 2,
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
        let tree = ProllyTree::new(tiny_profile()).unwrap();
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
        let tree = ProllyTree::new(tiny_profile()).unwrap();
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
        let patch = text_replace_patch("a🦀z", 1, 1, "é").unwrap();
        assert_eq!(patch.offset, 1);
        assert_eq!(patch.delete_len, 4);
        assert_eq!(
            apply_patches("a🦀z".as_bytes(), &[patch]).unwrap(),
            "aéz".as_bytes()
        );
    }

    #[test]
    fn json_validation_observes_the_complete_atomic_tail() {
        let tree = ProllyTree::new(tiny_profile()).unwrap();
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
        let tree = ProllyTree::new(tiny_profile()).unwrap();
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
        let tree = ProllyTree::new(tiny_profile()).unwrap();
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
                &ValueSelection::TextRange { offset: 5, len: 1 },
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
        hostile.extend_from_slice(&u64::MAX.to_le_bytes());
        assert_eq!(
            LargeValue::decode_cell(&schema, &hostile),
            Err(ContentError::TailTooLarge)
        );

        let oversized_bytes = LargeValue::Chunked(ChunkedValue {
            root: ContentId([7; 32]),
            root_byte_len: 42,
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
    fn untrusted_manifest_length_does_not_drive_prevalidation_allocation() {
        let value = LargeValue::Chunked(ChunkedValue {
            root: ContentId([9; 32]),
            root_byte_len: u64::MAX,
            edit_tail: Vec::new(),
        });
        assert!(matches!(
            value.materialize(
                ValueKind::Bytes,
                &domain(),
                ProllyTree::new(tiny_profile()).unwrap(),
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
