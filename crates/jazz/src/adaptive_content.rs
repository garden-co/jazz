//! Generic adaptive binary content used by large built-in scalar values.
//!
//! The module deliberately owns no row identity, history, policy, or sync
//! state. An [`AdaptiveScalar`] is one ordinary atomic cell whose large arm
//! references immutable, domain-scoped objects.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

const CONTENT_ID_DOMAIN: &[u8] = b"jazz-adaptive-content-v1";
const OBJECT_FORMAT_VERSION: u8 = 1;

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

/// Large physical arm of an adaptive scalar cell.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LargeScalar {
    /// Root of the immutable recursive byte tree.
    pub root: ContentId,
    /// Materialized root length before applying the tail.
    pub root_byte_len: u64,
    /// Ordered byte replacements.
    pub edit_tail: Vec<BytePatch>,
}

/// One ordinary scalar cell with transparent inline/large representation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdaptiveScalar {
    /// Direct bytes for small values.
    Inline(#[serde(with = "serde_bytes")] Vec<u8>),
    /// Immutable byte tree plus a bounded ordered patch tail.
    Large(LargeScalar),
}

/// Built-in semantic interpretation of adaptive bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScalarKind {
    /// Uninterpreted bytes.
    Bytes,
    /// UTF-8 text.
    String,
    /// UTF-8 JSON source.
    Json,
}

/// Immutable query projection over one adaptive scalar.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ScalarSelection {
    /// Materialize the complete idiomatic value.
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

/// Idiomatic immutable result of a scalar query projection.
#[derive(Clone, Debug, PartialEq)]
pub enum ScalarSelectionValue {
    /// Complete or ranged bytes.
    Bytes(Vec<u8>),
    /// Complete or ranged UTF-8 text.
    String(String),
    /// Complete or projected detached JSON.
    Json(serde_json::Value),
}

/// Declarative update authored against an ordinary immutable row snapshot.
#[derive(Clone, Debug, PartialEq)]
pub enum ScalarEdit {
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
    /// Replace JSON with one arbitrary idiomatic value.
    Json(serde_json::Value),
}

impl ScalarKind {
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
    #[error("adaptive string is not valid UTF-8")]
    InvalidUtf8,
    /// JSON source is invalid.
    #[error("adaptive JSON is invalid: {0}")]
    InvalidJson(String),
    /// A query projection does not apply to the logical scalar kind.
    #[error("query selection does not apply to this scalar kind")]
    InvalidSelection,
    /// An update operation does not apply to the logical scalar kind.
    #[error("edit operation does not apply to this scalar kind")]
    InvalidEdit,
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
        let capacity = usize::try_from(expected_len).map_err(|_| ContentError::LengthOverflow)?;
        let mut out = Vec::with_capacity(capacity);
        self.read_object_range(domain, root, 0, expected_len, store, &mut out)?;
        if out.len() != capacity {
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
        let capacity = usize::try_from(len).map_err(|_| ContentError::LengthOverflow)?;
        let mut out = Vec::with_capacity(capacity);
        self.read_object_range(domain, root, offset, end, store, &mut out)?;
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
        store: &S,
        out: &mut Vec<u8>,
    ) -> Result<(), ContentError> {
        match self.load_object(domain, id, store)? {
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

impl AdaptiveScalar {
    /// Create an inline scalar after logical validation.
    pub fn inline(kind: ScalarKind, bytes: impl Into<Vec<u8>>) -> Result<Self, ContentError> {
        let bytes = bytes.into();
        kind.validate(&bytes)?;
        Ok(Self::Inline(bytes))
    }

    /// Create the representation selected by one promotion threshold.
    pub fn create<S: ImmutableContentStore>(
        kind: ScalarKind,
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
        Ok(Self::Large(LargeScalar {
            root,
            root_byte_len,
            edit_tail: Vec::new(),
        }))
    }

    /// Materialize and validate the complete logical value.
    pub fn materialize<S: ImmutableContentStore>(
        &self,
        kind: ScalarKind,
        domain: &ContentDomain,
        tree: ProllyTree,
        store: &S,
    ) -> Result<Vec<u8>, ContentError> {
        let bytes = match self {
            Self::Inline(bytes) => bytes.clone(),
            Self::Large(large) => {
                let base = tree.materialize(domain, large.root, large.root_byte_len, store)?;
                apply_patches(&base, &large.edit_tail)?
            }
        };
        kind.validate(&bytes)?;
        Ok(bytes)
    }

    /// Evaluate one immutable query projection.
    pub fn select<S: ImmutableContentStore>(
        &self,
        kind: ScalarKind,
        selection: &ScalarSelection,
        domain: &ContentDomain,
        tree: ProllyTree,
        store: &S,
    ) -> Result<ScalarSelectionValue, ContentError> {
        let bytes = self.materialize(kind, domain, tree, store)?;
        match (kind, selection) {
            (ScalarKind::Bytes, ScalarSelection::Value) => Ok(ScalarSelectionValue::Bytes(bytes)),
            (ScalarKind::Bytes, ScalarSelection::ByteRange { offset, len }) => Ok(
                ScalarSelectionValue::Bytes(checked_slice(&bytes, *offset, *len)?.to_vec()),
            ),
            (ScalarKind::String, ScalarSelection::Value) => Ok(ScalarSelectionValue::String(
                String::from_utf8(bytes).map_err(|_| ContentError::InvalidUtf8)?,
            )),
            (ScalarKind::String, ScalarSelection::TextRange { offset, len }) => {
                let text = std::str::from_utf8(&bytes).map_err(|_| ContentError::InvalidUtf8)?;
                let start = scalar_to_byte_offset(text, *offset)?;
                let end_scalar = offset
                    .checked_add(*len)
                    .ok_or(ContentError::LengthOverflow)?;
                let end = scalar_to_byte_offset(text, end_scalar)?;
                Ok(ScalarSelectionValue::String(text[start..end].to_owned()))
            }
            (ScalarKind::Json, ScalarSelection::Value) => Ok(ScalarSelectionValue::Json(
                serde_json::from_slice(&bytes)
                    .map_err(|error| ContentError::InvalidJson(error.to_string()))?,
            )),
            (ScalarKind::Json, ScalarSelection::JsonPointer(pointer)) => {
                let value: serde_json::Value = serde_json::from_slice(&bytes)
                    .map_err(|error| ContentError::InvalidJson(error.to_string()))?;
                Ok(ScalarSelectionValue::Json(
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
        kind: ScalarKind,
        edit: ScalarEdit,
        domain: &ContentDomain,
        tree: ProllyTree,
        store: &S,
    ) -> Result<BytePatch, ContentError> {
        let bytes = self.materialize(kind, domain, tree, store)?;
        match (kind, edit) {
            (ScalarKind::Bytes, ScalarEdit::Bytes(patch)) => {
                apply_patches(&bytes, std::slice::from_ref(&patch))?;
                Ok(patch)
            }
            (ScalarKind::Bytes, ScalarEdit::Append(insert)) => Ok(BytePatch::insert(
                u64::try_from(bytes.len()).map_err(|_| ContentError::LengthOverflow)?,
                insert,
            )),
            (
                ScalarKind::String,
                ScalarEdit::Text {
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
            (ScalarKind::Json, ScalarEdit::Json(value)) => {
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
        kind: ScalarKind,
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
            Self::Large(large) => {
                let mut tail = large.edit_tail.clone();
                tail.push(patch);
                if tail_within_bounds(&tail, tail_bounds)? {
                    Ok(Self::Large(LargeScalar {
                        root: large.root,
                        root_byte_len: large.root_byte_len,
                        edit_tail: tail,
                    }))
                } else {
                    let (root, root_byte_len) = tree.build(domain, &next, store)?;
                    Ok(Self::Large(LargeScalar {
                        root,
                        root_byte_len,
                        edit_tail: Vec::new(),
                    }))
                }
            }
        }
    }
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
    let encoded = postcard::to_allocvec(tail)
        .map_err(|error| ContentError::MalformedObject(error.to_string()))?;
    Ok(encoded.len() <= bounds.max_encoded_bytes)
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

fn leaf_ranges(bytes: &[u8], profile: ChunkingProfile) -> Vec<std::ops::Range<usize>> {
    if bytes.is_empty() {
        return std::iter::once(0..0).collect();
    }
    let mask = u64::try_from(profile.target_leaf_bytes - 1).expect("target fits u64");
    let mut ranges = Vec::new();
    let mut start = 0;
    let mut rolling = 0_u64;
    for (index, byte) in bytes.iter().copied().enumerate() {
        rolling = rolling.rotate_left(1).wrapping_add(gear(byte));
        let len = index + 1 - start;
        let boundary = len >= profile.min_leaf_bytes && rolling & mask == 0;
        if boundary || len >= profile.max_leaf_bytes {
            ranges.push(start..index + 1);
            start = index + 1;
            rolling = 0;
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
    let mut rolling = 0_u64;
    for (index, child) in children.iter().enumerate() {
        for byte in child.id.as_bytes() {
            rolling = rolling.rotate_left(1).wrapping_add(gear(*byte));
        }
        rolling = rolling.rotate_left(1) ^ child.byte_len;
        let len = index + 1 - start;
        let boundary = len >= profile.min_children && rolling & mask == 0;
        if boundary || len >= profile.max_children {
            groups.push(start..index + 1);
            start = index + 1;
            rolling = 0;
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
    fn adaptive_tail_is_ordered_and_consolidates_at_the_bound() {
        let tree = ProllyTree::new(tiny_profile()).unwrap();
        let mut store = MemoryContentStore::default();
        let value = AdaptiveScalar::create(
            ScalarKind::String,
            &domain(),
            "abcdefghijklmnop",
            4,
            tree,
            &mut store,
        )
        .unwrap();
        let value = value
            .apply_edit(
                ScalarKind::String,
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
        assert!(matches!(&value, AdaptiveScalar::Large(value) if value.edit_tail.len() == 1));
        let value = value
            .apply_edit(
                ScalarKind::String,
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
        assert!(matches!(&value, AdaptiveScalar::Large(value) if value.edit_tail.is_empty()));
        assert_eq!(
            value
                .materialize(ScalarKind::String, &domain(), tree, &store)
                .unwrap(),
            b"bXXcdefghijklmnop"
        );
    }

    #[test]
    fn scalar_text_edits_lower_to_utf8_byte_patches() {
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
        let value = AdaptiveScalar::create(
            ScalarKind::Json,
            &domain(),
            br#"{"a":1}"#.to_vec(),
            4,
            tree,
            &mut store,
        )
        .unwrap();
        let value = value
            .apply_edit(
                ScalarKind::Json,
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
                .materialize(ScalarKind::Json, &domain(), tree, &store)
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
        assert!(reused * 2 > before.len(), "expected majority object reuse");
    }

    #[test]
    fn projections_and_declarative_edits_use_idiomatic_values() {
        let tree = ProllyTree::new(tiny_profile()).unwrap();
        let mut store = MemoryContentStore::default();
        let text = AdaptiveScalar::create(
            ScalarKind::String,
            &domain(),
            "zero 🦀 two",
            4,
            tree,
            &mut store,
        )
        .unwrap();
        assert_eq!(
            text.select(
                ScalarKind::String,
                &ScalarSelection::TextRange { offset: 5, len: 1 },
                &domain(),
                tree,
                &store,
            )
            .unwrap(),
            ScalarSelectionValue::String("🦀".to_owned())
        );

        let json = AdaptiveScalar::create(
            ScalarKind::Json,
            &domain(),
            br#"{"profile":{"name":"old"},"stable":1}"#.to_vec(),
            4,
            tree,
            &mut store,
        )
        .unwrap();
        assert_eq!(
            json.select(
                ScalarKind::Json,
                &ScalarSelection::JsonPointer("/profile/name".to_owned()),
                &domain(),
                tree,
                &store,
            )
            .unwrap(),
            ScalarSelectionValue::Json(serde_json::json!("old"))
        );
        let patch = json
            .lower_edit(
                ScalarKind::Json,
                ScalarEdit::Json(serde_json::json!({
                    "profile": { "name": "new" },
                    "stable": 1
                })),
                &domain(),
                tree,
                &store,
            )
            .unwrap();
        let current = json
            .materialize(ScalarKind::Json, &domain(), tree, &store)
            .unwrap();
        let next = apply_patches(&current, &[patch]).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&next).unwrap(),
            serde_json::json!({ "profile": { "name": "new" }, "stable": 1 })
        );
    }
}
