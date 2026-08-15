//! The `text-v1` embedded-content adapter.
//!
//! The public seam is [`TextContentAdapter`].  It is intentionally expressed
//! through `content_manifest` rather than through a second mutable document or
//! version table: the application cell containing a [`ContentManifest`] is the
//! mutable identity and every historical row version already retains a complete
//! text snapshot.

use std::collections::BTreeMap;

use groove::records::{Value, ValueType};

use crate::content_manifest::{
    ContentAddress, ContentDomainId, ContentId, ContentManifest, ContentManifestAdapter,
    ContentManifestSchema, ContentReadContext, ImmutableContentKind, ImmutableContentStore,
    ManifestError, MaterializationRequest, content_id,
};

/// Stable adapter metadata used by `ColumnSchema::content_manifest`.
pub const TEXT_ADAPTER_KIND: &str = "text-v1";
/// Format, rather than tuning, limit: every reader applies this bound.
pub const TEXT_MAX_TAIL_ENTRIES: u32 = 64;
/// Format, rather than tuning, limit: every reader applies this bound.
pub const TEXT_MAX_TAIL_BYTES: u32 = 16 * 1024;
/// Maximum canonical bytes in a remotely supplied immutable leaf.
pub const TEXT_MAX_LEAF_BYTES: usize = 4096;
/// Maximum accepted root-to-leaf depth for a remotely supplied rope.
pub const TEXT_MAX_ROPE_DEPTH: u32 = 64;

const LEAF_TAG: &[u8; 5] = b"TXT1L";
const NODE_TAG: &[u8; 5] = b"TXT1N";
const ROOT_TAG: &[u8; 5] = b"TXT1R";
const EDIT_TAG: &[u8; 5] = b"TXT1E";

/// A UTF-8 insertion intent, addressed in Unicode scalar positions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TextEdit {
    /// Insertion position measured in Unicode scalar values, not UTF-16 units.
    pub at_code_point: u64,
    /// Well-formed UTF-8 text inserted at that position.
    pub text: String,
}

/// Strongly typed view of a root stored in a text manifest.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TextRootId(pub ContentId);

/// The text adapter and its writer consolidation thresholds.
#[derive(Clone, Debug)]
pub struct TextContentAdapter {
    schema: ContentManifestSchema,
}

#[derive(Clone, Debug)]
enum Rope {
    Leaf {
        id: ContentId,
        text: String,
        length: u64,
    },
    Branch {
        id: ContentId,
        left: Box<Rope>,
        right: Box<Rope>,
        length: u64,
        height: u32,
    },
}

impl Rope {
    fn id(&self) -> ContentId {
        match self {
            Self::Leaf { id, .. } | Self::Branch { id, .. } => *id,
        }
    }
    fn length(&self) -> u64 {
        match self {
            Self::Leaf { length, .. } | Self::Branch { length, .. } => *length,
        }
    }
    fn height(&self) -> u32 {
        match self {
            Self::Leaf { .. } => 1,
            Self::Branch { height, .. } => *height,
        }
    }
    fn text(&self, output: &mut String) {
        match self {
            Self::Leaf { text, .. } => output.push_str(text),
            Self::Branch { left, right, .. } => {
                left.text(output);
                right.text(output);
            }
        }
    }
}

impl TextContentAdapter {
    /// Creates an adapter whose writer bounds are also valid persisted bounds.
    pub fn new(max_tail_entries: u32, max_tail_bytes: u32) -> Result<Self, ManifestError> {
        if max_tail_entries == 0
            || max_tail_entries > TEXT_MAX_TAIL_ENTRIES
            || max_tail_bytes == 0
            || max_tail_bytes > TEXT_MAX_TAIL_BYTES
        {
            return Err(ManifestError::InvalidSchema);
        }
        Ok(Self {
            schema: ContentManifestSchema::with_tail_entry_type(
                TEXT_ADAPTER_KIND,
                ValueType::Bytes,
                max_tail_entries,
                max_tail_bytes,
            )?,
        })
    }

    /// Default text format limits. Callers can select lower writer thresholds.
    pub fn schema(&self) -> &ContentManifestSchema {
        &self.schema
    }

    /// Returns the manifest root at the typed text-adapter boundary.
    pub fn root_id(&self, manifest: &ContentManifest) -> TextRootId {
        TextRootId(manifest.root)
    }

    /// Stores a complete initial immutable rope and returns its empty-tail manifest.
    pub fn create(
        &self,
        text: &str,
        context: ContentReadContext,
        store: &mut dyn ImmutableContentStore,
    ) -> Result<ContentManifest, ManifestError> {
        let root = self.build_root(text, context.domain, store)?;
        Ok(ContentManifest {
            root,
            edit_tail: Vec::new(),
        })
    }

    /// Adds an insertion to the tail or synchronously promotes it into a new root.
    ///
    /// This is deliberately a foreground operation: it never performs a
    /// background root rewrite that could race an ordinary owning-row update.
    pub fn insert(
        &self,
        manifest: &ContentManifest,
        at_code_point: u64,
        text: &str,
        context: ContentReadContext,
        store: &mut dyn ImmutableContentStore,
    ) -> Result<ContentManifest, ManifestError> {
        if text.is_empty() {
            return Ok(manifest.clone());
        }
        manifest.validate(&self.schema)?;
        let root = self.load_root(manifest.root, context, store)?;
        let mut logical_length = root.length();
        for operation in &manifest.edit_tail {
            let edit = decode_edit(value_bytes(operation)?)?;
            validate_edit_offset(logical_length, &edit)?;
            logical_length = logical_length
                .checked_add(scalar_len(&edit.text))
                .ok_or(ManifestError::Malformed)?;
        }
        if at_code_point > logical_length {
            return Err(ManifestError::Malformed);
        }
        let encoded = encode_edit(&TextEdit {
            at_code_point,
            text: text.to_owned(),
        });
        let mut tail = manifest.edit_tail.clone();
        tail.push(Value::Bytes(encoded));
        let candidate = ContentManifest {
            root: manifest.root,
            edit_tail: tail,
        };
        if candidate.validate(&self.schema).is_ok() {
            return Ok(candidate);
        }
        let mut promoted = root;
        for operation in &candidate.edit_tail {
            let edit = decode_edit(value_bytes(operation)?)?;
            promoted = self.insert_into_rope(
                promoted,
                edit.at_code_point,
                &edit.text,
                context.domain,
                store,
            )?;
        }
        let payload = root_payload(promoted.id(), promoted.length(), promoted.height());
        let root = store.put_if_absent_or_identical(
            ContentAddress {
                domain: context.domain,
                adapter_kind: TEXT_ADAPTER_KIND,
                kind: ImmutableContentKind::Root,
            },
            payload,
        )?;
        Ok(ContentManifest {
            root,
            edit_tail: Vec::new(),
        })
    }

    /// Decodes a text operation for callers that need to inspect a tail.
    pub fn decode_edit(operation: &Value) -> Result<TextEdit, ManifestError> {
        decode_edit(value_bytes(operation)?)
    }

    fn build_root(
        &self,
        text: &str,
        domain: ContentDomainId,
        store: &mut dyn ImmutableContentStore,
    ) -> Result<ContentId, ManifestError> {
        let leaves = split_utf8(text, TEXT_MAX_LEAF_BYTES)?
            .into_iter()
            .map(|part| self.store_leaf(&part, domain, store))
            .collect::<Result<Vec<_>, _>>()?;
        let tree = self.build_balanced(leaves, domain, store)?;
        let payload = root_payload(tree.id(), tree.length(), tree.height());
        store.put_if_absent_or_identical(
            ContentAddress {
                domain,
                adapter_kind: TEXT_ADAPTER_KIND,
                kind: ImmutableContentKind::Root,
            },
            payload,
        )
    }

    fn build_balanced(
        &self,
        mut nodes: Vec<Rope>,
        domain: ContentDomainId,
        store: &mut dyn ImmutableContentStore,
    ) -> Result<Rope, ManifestError> {
        if nodes.is_empty() {
            return Err(ManifestError::Malformed);
        }
        if nodes.len() == 1 {
            return Ok(nodes.pop().expect("one text rope node"));
        }
        // Splitting by count, recursively, guarantees the two child heights
        // differ by at most one for every leaf count. Pairing successive levels
        // leaves a five-leaf tree as [height 3, height 1], which its own reader
        // correctly rejects.
        let right = nodes.split_off(nodes.len() / 2);
        let left = self.build_balanced(nodes, domain, store)?;
        let right = self.build_balanced(right, domain, store)?;
        self.store_branch(left, right, domain, store)
    }

    fn store_leaf(
        &self,
        text: &str,
        domain: ContentDomainId,
        store: &mut dyn ImmutableContentStore,
    ) -> Result<Rope, ManifestError> {
        let length = scalar_len(text);
        let id = store.put_if_absent_or_identical(
            ContentAddress {
                domain,
                adapter_kind: TEXT_ADAPTER_KIND,
                kind: ImmutableContentKind::Leaf,
            },
            leaf_payload(text, length),
        )?;
        Ok(Rope::Leaf {
            id,
            text: text.to_owned(),
            length,
        })
    }

    fn store_branch(
        &self,
        left: Rope,
        right: Rope,
        domain: ContentDomainId,
        store: &mut dyn ImmutableContentStore,
    ) -> Result<Rope, ManifestError> {
        let length = left
            .length()
            .checked_add(right.length())
            .ok_or(ManifestError::Malformed)?;
        let height = left
            .height()
            .max(right.height())
            .checked_add(1)
            .ok_or(ManifestError::Malformed)?;
        if height > TEXT_MAX_ROPE_DEPTH {
            return Err(ManifestError::Malformed);
        }
        let id = store.put_if_absent_or_identical(
            ContentAddress {
                domain,
                adapter_kind: TEXT_ADAPTER_KIND,
                kind: ImmutableContentKind::Node,
            },
            branch_payload(left.id(), right.id(), length, height),
        )?;
        Ok(Rope::Branch {
            id,
            left: Box::new(left),
            right: Box::new(right),
            length,
            height,
        })
    }

    fn insert_into_rope(
        &self,
        rope: Rope,
        at: u64,
        inserted: &str,
        domain: ContentDomainId,
        store: &mut dyn ImmutableContentStore,
    ) -> Result<Rope, ManifestError> {
        if at > rope.length() {
            return Err(ManifestError::Malformed);
        }
        match rope {
            Rope::Leaf { text, .. } => {
                let updated = insert_at_code_point(&text, at, inserted)?;
                let leaves = split_utf8(&updated, TEXT_MAX_LEAF_BYTES)?
                    .into_iter()
                    .map(|part| self.store_leaf(part, domain, store))
                    .collect::<Result<Vec<_>, _>>()?;
                self.build_balanced(leaves, domain, store)
            }
            Rope::Branch { left, right, .. } if at <= left.length() => {
                let left = self.insert_into_rope(*left, at, inserted, domain, store)?;
                self.balance(left, *right, domain, store)
            }
            Rope::Branch { left, right, .. } => {
                let offset = at
                    .checked_sub(left.length())
                    .ok_or(ManifestError::Malformed)?;
                let right = self.insert_into_rope(*right, offset, inserted, domain, store)?;
                self.balance(*left, right, domain, store)
            }
        }
    }

    fn balance(
        &self,
        left: Rope,
        right: Rope,
        domain: ContentDomainId,
        store: &mut dyn ImmutableContentStore,
    ) -> Result<Rope, ManifestError> {
        if left.height() > right.height().saturating_add(1) {
            let Rope::Branch {
                left: ll,
                right: lr,
                ..
            } = left
            else {
                return Err(ManifestError::Malformed);
            };
            if ll.height() >= lr.height() {
                let new_right = self.store_branch(*lr, right, domain, store)?;
                return self.store_branch(*ll, new_right, domain, store);
            }
            let Rope::Branch {
                left: lrl,
                right: lrr,
                ..
            } = *lr
            else {
                return Err(ManifestError::Malformed);
            };
            let new_left = self.store_branch(*ll, *lrl, domain, store)?;
            let new_right = self.store_branch(*lrr, right, domain, store)?;
            return self.store_branch(new_left, new_right, domain, store);
        }
        if right.height() > left.height().saturating_add(1) {
            let Rope::Branch {
                left: rl,
                right: rr,
                ..
            } = right
            else {
                return Err(ManifestError::Malformed);
            };
            if rr.height() >= rl.height() {
                let new_left = self.store_branch(left, *rl, domain, store)?;
                return self.store_branch(new_left, *rr, domain, store);
            }
            let Rope::Branch {
                left: rll,
                right: rlr,
                ..
            } = *rl
            else {
                return Err(ManifestError::Malformed);
            };
            let new_left = self.store_branch(left, *rll, domain, store)?;
            let new_right = self.store_branch(*rlr, *rr, domain, store)?;
            return self.store_branch(new_left, new_right, domain, store);
        }
        self.store_branch(left, right, domain, store)
    }

    fn load_root(
        &self,
        root: ContentId,
        context: ContentReadContext,
        store: &dyn ImmutableContentStore,
    ) -> Result<Rope, ManifestError> {
        let payload = checked_get(store, context, root, ImmutableContentKind::Root)?;
        let (child, length, height) = parse_root(payload)?;
        if height == 0 || height > TEXT_MAX_ROPE_DEPTH {
            return Err(ManifestError::Malformed);
        }
        let tree = self.load_tree(child, 1, context, store)?;
        if tree.length() != length || tree.height() != height {
            return Err(ManifestError::Malformed);
        }
        Ok(tree)
    }

    fn load_tree(
        &self,
        id: ContentId,
        depth: u32,
        context: ContentReadContext,
        store: &dyn ImmutableContentStore,
    ) -> Result<Rope, ManifestError> {
        if depth == 0 || depth > TEXT_MAX_ROPE_DEPTH {
            return Err(ManifestError::Malformed);
        }
        let bytes = store.get(context, id).ok_or(ManifestError::Malformed)?;
        if bytes.starts_with(LEAF_TAG) {
            let payload = checked_get(store, context, id, ImmutableContentKind::Leaf)?;
            let (text, length) = parse_leaf(payload)?;
            return Ok(Rope::Leaf { id, text, length });
        }
        let payload = checked_get(store, context, id, ImmutableContentKind::Node)?;
        let (left_id, right_id, length, height) = parse_branch(payload)?;
        if height <= 1 || height > TEXT_MAX_ROPE_DEPTH {
            return Err(ManifestError::Malformed);
        }
        let next_depth = depth.checked_add(1).ok_or(ManifestError::Malformed)?;
        let left = self.load_tree(left_id, next_depth, context, store)?;
        let right = self.load_tree(right_id, next_depth, context, store)?;
        if length
            != left
                .length()
                .checked_add(right.length())
                .ok_or(ManifestError::Malformed)?
            || left.height().abs_diff(right.height()) > 1
            || height
                != left
                    .height()
                    .max(right.height())
                    .checked_add(1)
                    .ok_or(ManifestError::Malformed)?
        {
            return Err(ManifestError::Malformed);
        }
        Ok(Rope::Branch {
            id,
            left: Box::new(left),
            right: Box::new(right),
            length,
            height,
        })
    }

    fn apply_tail(&self, root: Rope, tail: &[Value]) -> Result<String, ManifestError> {
        let mut text = String::new();
        root.text(&mut text);
        for operation in tail {
            let edit = decode_edit(value_bytes(operation)?)?;
            text = insert_at_code_point(&text, edit.at_code_point, &edit.text)?;
        }
        Ok(text)
    }
}

impl Default for TextContentAdapter {
    fn default() -> Self {
        Self::new(TEXT_MAX_TAIL_ENTRIES, TEXT_MAX_TAIL_BYTES).expect("valid text defaults")
    }
}

impl ContentManifestAdapter for TextContentAdapter {
    fn adapter_kind(&self) -> &str {
        TEXT_ADAPTER_KIND
    }

    fn validate_schema(&self, schema: &ContentManifestSchema) -> Result<(), ManifestError> {
        if schema.adapter_kind != TEXT_ADAPTER_KIND
            || schema.tail_entry_type != ValueType::Bytes
            || schema.max_tail_entries > TEXT_MAX_TAIL_ENTRIES
            || schema.max_tail_bytes > TEXT_MAX_TAIL_BYTES
        {
            return Err(ManifestError::InvalidSchema);
        }
        Ok(())
    }

    fn validate_operation(&self, operation: &Value) -> Result<(), ManifestError> {
        Self::decode_edit(operation).map(|_| ())
    }

    fn materialize(
        &self,
        manifest: &ContentManifest,
        request: &MaterializationRequest,
        context: ContentReadContext,
        store: &dyn ImmutableContentStore,
    ) -> Result<Vec<u8>, ManifestError> {
        manifest.validate(&self.schema)?;
        for operation in &manifest.edit_tail {
            self.validate_operation(operation)?;
        }
        let full = self.apply_tail(
            self.load_root(manifest.root, context, store)?,
            &manifest.edit_tail,
        )?;
        match request {
            MaterializationRequest::Full | MaterializationRequest::Projection(_) => {
                Ok(full.into_bytes())
            }
            MaterializationRequest::Range { offset, length } => {
                range_at_code_points(&full, *offset, *length).map(String::into_bytes)
            }
        }
    }

    fn merge(
        &self,
        manifests: &[ContentManifest],
        context: ContentReadContext,
        store: &dyn ImmutableContentStore,
    ) -> Result<ContentManifest, ManifestError> {
        let Some(first) = manifests.first() else {
            return Err(ManifestError::Conflict("no text candidates"));
        };
        let first_value = self.materialize(first, &MaterializationRequest::Full, context, store)?;
        for candidate in &manifests[1..] {
            if self.materialize(candidate, &MaterializationRequest::Full, context, store)?
                != first_value
            {
                return Err(ManifestError::Conflict(
                    "different text values require normal atomic LWW",
                ));
            }
        }
        Ok(first.clone())
    }

    fn index_values(
        &self,
        manifest: &ContentManifest,
        requested: &[String],
        context: ContentReadContext,
        store: &dyn ImmutableContentStore,
    ) -> Result<BTreeMap<String, Vec<u8>>, ManifestError> {
        let text = self.materialize(manifest, &MaterializationRequest::Full, context, store)?;
        let text = std::str::from_utf8(&text).map_err(|_| ManifestError::Malformed)?;
        let mut values = BTreeMap::new();
        for key in requested {
            match key.as_str() {
                "text" => {
                    values.insert(key.clone(), text.as_bytes().to_vec());
                }
                "length" => {
                    values.insert(key.clone(), scalar_len(text).to_le_bytes().to_vec());
                }
                _ => return Err(ManifestError::Conflict("unknown text index")),
            }
        }
        Ok(values)
    }
}

fn value_bytes(value: &Value) -> Result<&[u8], ManifestError> {
    match value {
        Value::Bytes(bytes) => Ok(bytes),
        _ => Err(ManifestError::Malformed),
    }
}

fn checked_get(
    store: &dyn ImmutableContentStore,
    context: ContentReadContext,
    id: ContentId,
    kind: ImmutableContentKind,
) -> Result<&[u8], ManifestError> {
    let bytes = store.get(context, id).ok_or(ManifestError::Malformed)?;
    if content_id(context.domain, TEXT_ADAPTER_KIND, kind, bytes) != id {
        return Err(ManifestError::Malformed);
    }
    Ok(bytes)
}

fn scalar_len(text: &str) -> u64 {
    text.chars().count() as u64
}

fn split_utf8(text: &str, max_bytes: usize) -> Result<Vec<&str>, ManifestError> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut bytes = 0;
    for (index, ch) in text.char_indices() {
        let width = ch.len_utf8();
        if width > max_bytes {
            return Err(ManifestError::Malformed);
        }
        if bytes != 0 && bytes + width > max_bytes {
            out.push(&text[start..index]);
            start = index;
            bytes = 0;
        }
        bytes += width;
    }
    if start < text.len() || text.is_empty() {
        out.push(&text[start..]);
    }
    Ok(out)
}

fn insert_at_code_point(base: &str, at: u64, inserted: &str) -> Result<String, ManifestError> {
    let length = scalar_len(base);
    if at > length {
        return Err(ManifestError::Malformed);
    }
    let byte = if at == length {
        base.len()
    } else {
        base.char_indices()
            .nth(at as usize)
            .ok_or(ManifestError::Malformed)?
            .0
    };
    let mut output = String::with_capacity(base.len() + inserted.len());
    output.push_str(&base[..byte]);
    output.push_str(inserted);
    output.push_str(&base[byte..]);
    Ok(output)
}

fn validate_edit_offset(current_length: u64, edit: &TextEdit) -> Result<(), ManifestError> {
    if edit.at_code_point > current_length {
        return Err(ManifestError::Malformed);
    }
    Ok(())
}

fn range_at_code_points(text: &str, offset: u64, length: u64) -> Result<String, ManifestError> {
    let total = scalar_len(text);
    let end = offset.checked_add(length).ok_or(ManifestError::Malformed)?;
    if offset > total || end > total {
        return Err(ManifestError::Malformed);
    }
    let start_byte = if offset == total {
        text.len()
    } else {
        text.char_indices()
            .nth(offset as usize)
            .ok_or(ManifestError::Malformed)?
            .0
    };
    let end_byte = if end == total {
        text.len()
    } else {
        text.char_indices()
            .nth(end as usize)
            .ok_or(ManifestError::Malformed)?
            .0
    };
    Ok(text[start_byte..end_byte].to_owned())
}

fn leaf_payload(text: &str, length: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(17 + text.len());
    out.extend_from_slice(LEAF_TAG);
    out.extend_from_slice(&length.to_le_bytes());
    out.extend_from_slice(&(text.len() as u32).to_le_bytes());
    out.extend_from_slice(text.as_bytes());
    out
}
fn branch_payload(left: ContentId, right: ContentId, length: u64, height: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(81);
    out.extend_from_slice(NODE_TAG);
    out.extend_from_slice(&left.0);
    out.extend_from_slice(&right.0);
    out.extend_from_slice(&length.to_le_bytes());
    out.extend_from_slice(&height.to_le_bytes());
    out
}
fn root_payload(child: ContentId, length: u64, height: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(49);
    out.extend_from_slice(ROOT_TAG);
    out.extend_from_slice(&child.0);
    out.extend_from_slice(&length.to_le_bytes());
    out.extend_from_slice(&height.to_le_bytes());
    out
}
fn encode_edit(edit: &TextEdit) -> Vec<u8> {
    let mut out = Vec::with_capacity(17 + edit.text.len());
    out.extend_from_slice(EDIT_TAG);
    out.extend_from_slice(&edit.at_code_point.to_le_bytes());
    out.extend_from_slice(&(edit.text.len() as u32).to_le_bytes());
    out.extend_from_slice(edit.text.as_bytes());
    out
}
fn parse_leaf(bytes: &[u8]) -> Result<(String, u64), ManifestError> {
    if bytes.len() < 17 || !bytes.starts_with(LEAF_TAG) {
        return Err(ManifestError::Malformed);
    }
    let length = u64::from_le_bytes(
        bytes[5..13]
            .try_into()
            .map_err(|_| ManifestError::Malformed)?,
    );
    let byte_len = u32::from_le_bytes(
        bytes[13..17]
            .try_into()
            .map_err(|_| ManifestError::Malformed)?,
    ) as usize;
    if byte_len > TEXT_MAX_LEAF_BYTES || bytes.len() != 17 + byte_len {
        return Err(ManifestError::Malformed);
    }
    let text = std::str::from_utf8(&bytes[17..])
        .map_err(|_| ManifestError::Malformed)?
        .to_owned();
    if scalar_len(&text) != length {
        return Err(ManifestError::Malformed);
    }
    Ok((text, length))
}
fn parse_branch(bytes: &[u8]) -> Result<(ContentId, ContentId, u64, u32), ManifestError> {
    if bytes.len() != 81 || !bytes.starts_with(NODE_TAG) {
        return Err(ManifestError::Malformed);
    }
    let mut left = [0; 32];
    left.copy_from_slice(&bytes[5..37]);
    let mut right = [0; 32];
    right.copy_from_slice(&bytes[37..69]);
    let length = u64::from_le_bytes(
        bytes[69..77]
            .try_into()
            .map_err(|_| ManifestError::Malformed)?,
    );
    let height = u32::from_le_bytes(
        bytes[77..81]
            .try_into()
            .map_err(|_| ManifestError::Malformed)?,
    );
    Ok((ContentId(left), ContentId(right), length, height))
}
fn parse_root(bytes: &[u8]) -> Result<(ContentId, u64, u32), ManifestError> {
    if bytes.len() != 49 || !bytes.starts_with(ROOT_TAG) {
        return Err(ManifestError::Malformed);
    }
    let mut child = [0; 32];
    child.copy_from_slice(&bytes[5..37]);
    let length = u64::from_le_bytes(
        bytes[37..45]
            .try_into()
            .map_err(|_| ManifestError::Malformed)?,
    );
    let height = u32::from_le_bytes(
        bytes[45..49]
            .try_into()
            .map_err(|_| ManifestError::Malformed)?,
    );
    Ok((ContentId(child), length, height))
}
fn decode_edit(bytes: &[u8]) -> Result<TextEdit, ManifestError> {
    if bytes.len() < 17 || !bytes.starts_with(EDIT_TAG) {
        return Err(ManifestError::Malformed);
    }
    let at_code_point = u64::from_le_bytes(
        bytes[5..13]
            .try_into()
            .map_err(|_| ManifestError::Malformed)?,
    );
    let byte_len = u32::from_le_bytes(
        bytes[13..17]
            .try_into()
            .map_err(|_| ManifestError::Malformed)?,
    ) as usize;
    if bytes.len() != 17 + byte_len {
        return Err(ManifestError::Malformed);
    }
    let text = std::str::from_utf8(&bytes[17..])
        .map_err(|_| ManifestError::Malformed)?
        .to_owned();
    Ok(TextEdit {
        at_code_point,
        text,
    })
}

#[cfg(test)]
mod tests {
    // These are internal only because the foundation currently exposes an
    // adapter/store seam, not a public client-level adapter registry. They are
    // black-box tests of that public seam and use no rope internals.
    use super::*;
    use crate::content_manifest::{
        ContentManifestRuntime, ContentManifestRuntimeProvider, MemoryImmutableContentStore,
        global_content_manifest_adapters,
    };
    use crate::{
        ids::NodeUuid,
        node::NodeState,
        schema::{ColumnSchema, JazzSchema, TableSchema},
    };
    use groove::{records::Value, schema::ColumnType, storage::MemoryStorage};
    use std::{collections::BTreeSet, sync::Arc};

    #[derive(Default)]
    struct CountingStore {
        inner: MemoryImmutableContentStore,
        puts: usize,
        ids: BTreeSet<ContentId>,
    }
    impl ImmutableContentStore for CountingStore {
        fn get(&self, context: ContentReadContext, id: ContentId) -> Option<&[u8]> {
            self.inner.get(context, id)
        }
        fn put_if_absent_or_identical(
            &mut self,
            address: ContentAddress<'_>,
            bytes: Vec<u8>,
        ) -> Result<ContentId, ManifestError> {
            self.puts += 1;
            let id = self.inner.put_if_absent_or_identical(address, bytes)?;
            self.ids.insert(id);
            Ok(id)
        }
    }

    fn context() -> ContentReadContext {
        ContentReadContext {
            domain: ContentDomainId(uuid::Uuid::from_bytes([7; 16])),
        }
    }
    fn text(
        adapter: &TextContentAdapter,
        manifest: &ContentManifest,
        store: &MemoryImmutableContentStore,
    ) -> String {
        String::from_utf8(
            adapter
                .materialize(manifest, &MaterializationRequest::Full, context(), store)
                .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn manifests_materialize_unicode_root_plus_tail_and_range() {
        let adapter = TextContentAdapter::new(4, 1024).unwrap();
        let mut store = MemoryImmutableContentStore::default();
        let base = adapter.create("A😀C", context(), &mut store).unwrap();
        let edited = adapter
            .insert(&base, 2, "é", context(), &mut store)
            .unwrap();
        assert_eq!(edited.root, base.root, "tail-only edit reuses root");
        assert_eq!(text(&adapter, &edited, &store), "A😀éC");
        assert_eq!(
            adapter
                .materialize(
                    &edited,
                    &MaterializationRequest::Range {
                        offset: 1,
                        length: 2
                    },
                    context(),
                    &store
                )
                .unwrap(),
            "😀é".as_bytes()
        );
        assert!(
            adapter
                .insert(&edited, 5, "x", context(), &mut store)
                .is_err()
        );
    }

    #[test]
    fn promotion_retains_direct_historical_manifest() {
        let adapter = TextContentAdapter::new(1, 1024).unwrap();
        let mut store = MemoryImmutableContentStore::default();
        let base = adapter.create("base", context(), &mut store).unwrap();
        let tail = adapter
            .insert(&base, 4, "!", context(), &mut store)
            .unwrap();
        let promoted = adapter
            .insert(&tail, 5, "?", context(), &mut store)
            .unwrap();
        assert_ne!(promoted.root, base.root);
        assert!(promoted.edit_tail.is_empty());
        assert_eq!(text(&adapter, &tail, &store), "base!");
        assert_eq!(text(&adapter, &promoted, &store), "base!?");
    }

    #[test]
    fn materializers_and_indices_observe_unpromoted_tail() {
        let adapter = TextContentAdapter::default();
        let mut store = MemoryImmutableContentStore::default();
        let base = adapter.create("old", context(), &mut store).unwrap();
        let edited = adapter
            .insert(&base, 3, " tail", context(), &mut store)
            .unwrap();
        assert_eq!(
            adapter
                .index_values(
                    &edited,
                    &["text".into(), "length".into()],
                    context(),
                    &store
                )
                .unwrap()
                .get("text"),
            Some(&b"old tail".to_vec())
        );
        assert_eq!(
            adapter.merge(&[base, edited], context(), &store),
            Err(ManifestError::Conflict(
                "different text values require normal atomic LWW"
            ))
        );
    }

    #[test]
    fn malformed_or_cross_domain_content_fails_closed() {
        let adapter = TextContentAdapter::default();
        let mut store = MemoryImmutableContentStore::default();
        let base = adapter.create("safe", context(), &mut store).unwrap();
        let malformed = ContentManifest {
            root: base.root,
            edit_tail: vec![Value::Bytes(b"not an edit".to_vec())],
        };
        assert!(
            adapter
                .materialize(&malformed, &MaterializationRequest::Full, context(), &store)
                .is_err()
        );
        let other = ContentReadContext {
            domain: ContentDomainId(uuid::Uuid::from_bytes([8; 16])),
        };
        assert!(
            adapter
                .materialize(&base, &MaterializationRequest::Full, other, &store)
                .is_err()
        );
    }

    #[test]
    fn equal_text_is_domain_scoped_content_addressed_and_stale_promotion_is_not_merged() {
        let adapter = TextContentAdapter::new(1, 1024).unwrap();
        let mut store = MemoryImmutableContentStore::default();
        let base = adapter.create("same", context(), &mut store).unwrap();
        let repeated = adapter.create("same", context(), &mut store).unwrap();
        assert_eq!(adapter.root_id(&base), adapter.root_id(&repeated));

        let first = adapter
            .insert(&base, 4, "!", context(), &mut store)
            .unwrap();
        let promoted = adapter
            .insert(&first, 5, "?", context(), &mut store)
            .unwrap();
        let stale = adapter
            .insert(&base, 4, ".", context(), &mut store)
            .unwrap();
        assert!(promoted.edit_tail.is_empty());
        assert_eq!(text(&adapter, &promoted, &store), "same!?");
        assert_eq!(text(&adapter, &stale, &store), "same.");
        assert!(
            adapter
                .merge(&[promoted, stale], context(), &store)
                .is_err()
        );
    }

    #[test]
    fn promotion_path_copies_only_the_edited_path_and_reuses_untouched_subtrees() {
        let adapter = TextContentAdapter::new(64, 1).unwrap();
        let mut store = CountingStore::default();
        let initial = format!("{}{}tail", "a".repeat(4096), "b".repeat(4096));
        let base = adapter.create(&initial, context(), &mut store).unwrap();
        let old = adapter.load_root(base.root, context(), &store).unwrap();
        let old_left = match old {
            Rope::Branch { left, .. } => left.id(),
            Rope::Leaf { .. } => panic!("fixture must have multiple leaves"),
        };
        store.puts = 0;
        store.ids.clear();

        let promoted = adapter
            .insert(&base, scalar_len(&initial), "!", context(), &mut store)
            .unwrap();
        assert!(promoted.edit_tail.is_empty());
        assert_eq!(
            text(&adapter, &promoted, &store.inner),
            format!("{initial}!")
        );
        let new = adapter.load_root(promoted.root, context(), &store).unwrap();
        let new_left = match new {
            Rope::Branch { left, .. } => left.id(),
            Rope::Leaf { .. } => panic!("fixture must remain multi-leaf"),
        };
        assert_eq!(new_left, old_left, "untouched left subtree must be reused");
        assert_eq!(
            store.puts, 4,
            "one leaf, two path nodes, and one root wrapper"
        );
        assert_eq!(store.ids.len(), 4);
    }

    #[test]
    fn balanced_builder_round_trips_adversarial_leaf_counts() {
        let adapter = TextContentAdapter::default();
        let mut store = MemoryImmutableContentStore::default();
        for leaf_count in [1usize, 2, 3, 4, 5, 6, 7, 9, 10, 17, 31, 33, 65] {
            let byte_length = if leaf_count == 1 {
                1
            } else {
                (leaf_count - 1) * TEXT_MAX_LEAF_BYTES + 1
            };
            let value = "x".repeat(byte_length);
            let manifest = adapter.create(&value, context(), &mut store).unwrap();
            assert_eq!(
                text(&adapter, &manifest, &store),
                value,
                "leaf count {leaf_count} must create a reader-valid AVL rope"
            );
        }
    }

    fn store_raw(
        store: &mut MemoryImmutableContentStore,
        kind: ImmutableContentKind,
        payload: Vec<u8>,
    ) -> ContentId {
        store
            .put_if_absent_or_identical(
                ContentAddress {
                    domain: context().domain,
                    adapter_kind: TEXT_ADAPTER_KIND,
                    kind,
                },
                payload,
            )
            .unwrap()
    }

    #[test]
    fn content_valid_oversize_skewed_and_deep_remote_ropes_fail_closed() {
        let adapter = TextContentAdapter::default();

        let mut oversized_store = MemoryImmutableContentStore::default();
        let oversized_text = "x".repeat(TEXT_MAX_LEAF_BYTES + 1);
        let leaf = store_raw(
            &mut oversized_store,
            ImmutableContentKind::Leaf,
            leaf_payload(&oversized_text, scalar_len(&oversized_text)),
        );
        let root = store_raw(
            &mut oversized_store,
            ImmutableContentKind::Root,
            root_payload(leaf, scalar_len(&oversized_text), 1),
        );
        assert!(
            adapter
                .materialize(
                    &ContentManifest {
                        root,
                        edit_tail: vec![]
                    },
                    &MaterializationRequest::Full,
                    context(),
                    &oversized_store
                )
                .is_err()
        );

        let mut skewed_store = MemoryImmutableContentStore::default();
        let leaf = store_raw(
            &mut skewed_store,
            ImmutableContentKind::Leaf,
            leaf_payload("x", 1),
        );
        let pair = store_raw(
            &mut skewed_store,
            ImmutableContentKind::Node,
            branch_payload(leaf, leaf, 2, 2),
        );
        let balanced = store_raw(
            &mut skewed_store,
            ImmutableContentKind::Node,
            branch_payload(pair, leaf, 3, 3),
        );
        let skewed = store_raw(
            &mut skewed_store,
            ImmutableContentKind::Node,
            branch_payload(balanced, leaf, 4, 4),
        );
        let root = store_raw(
            &mut skewed_store,
            ImmutableContentKind::Root,
            root_payload(skewed, 4, 4),
        );
        assert!(
            adapter
                .materialize(
                    &ContentManifest {
                        root,
                        edit_tail: vec![]
                    },
                    &MaterializationRequest::Full,
                    context(),
                    &skewed_store
                )
                .is_err()
        );

        let mut deep_store = MemoryImmutableContentStore::default();
        let leaf = store_raw(
            &mut deep_store,
            ImmutableContentKind::Leaf,
            leaf_payload("x", 1),
        );
        let mut child = leaf;
        let mut length = 1;
        for height in 2..=TEXT_MAX_ROPE_DEPTH + 1 {
            length += 1;
            child = store_raw(
                &mut deep_store,
                ImmutableContentKind::Node,
                branch_payload(child, leaf, length, height),
            );
        }
        let root = store_raw(
            &mut deep_store,
            ImmutableContentKind::Root,
            root_payload(child, length, TEXT_MAX_ROPE_DEPTH + 1),
        );
        assert!(
            adapter
                .materialize(
                    &ContentManifest {
                        root,
                        edit_tail: vec![]
                    },
                    &MaterializationRequest::Full,
                    context(),
                    &deep_store
                )
                .is_err()
        );
    }

    struct Provider(MemoryImmutableContentStore);
    impl ContentManifestRuntimeProvider for Provider {
        fn read_context(&self, _: NodeUuid) -> ContentReadContext {
            context()
        }
        fn immutable_store(&self) -> &dyn ImmutableContentStore {
            &self.0
        }
    }

    #[test]
    fn registered_text_schema_runs_through_node_materialize_merge_and_index_seams() {
        let writer = TextContentAdapter::default();
        let manifest_schema = writer.schema().clone();
        // No adapter registration call precedes this lookup: `text-v1` is a
        // production built-in installed while the global registry is created.
        assert!(
            global_content_manifest_adapters()
                .get(TEXT_ADAPTER_KIND)
                .is_ok()
        );
        let column = ColumnSchema::content_manifest("body", manifest_schema.clone());
        let schema = JazzSchema::new([TableSchema::new(
            "documents",
            [
                ColumnSchema::new("title", ColumnType::String),
                column.clone(),
            ],
        )]);
        let mut store = MemoryImmutableContentStore::default();
        let base = writer.create("root", context(), &mut store).unwrap();
        let tailed = writer
            .insert(&base, 4, " tail", context(), &mut store)
            .unwrap();
        let equivalent = writer.create("root tail", context(), &mut store).unwrap();
        let tailed_value = tailed.into_value(&manifest_schema).unwrap();
        let equivalent_value = equivalent.into_value(&manifest_schema).unwrap();

        crate::node::codec::validate_cell_value(&column, &tailed_value).unwrap();
        let refs = schema.column_families();
        let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
        let provider = Arc::new(Provider(store));
        let node = NodeState::new_with_content_manifest_provider(
            NodeUuid(uuid::Uuid::from_bytes([99; 16])),
            schema,
            MemoryStorage::new(&refs),
            provider.clone(),
            false,
        )
        .unwrap();
        assert_eq!(
            node.materialize_content_manifest(
                "documents",
                "body",
                &tailed_value,
                &MaterializationRequest::Full
            )
            .unwrap(),
            b"root tail"
        );
        assert_eq!(
            node.content_manifest_index_values(
                "documents",
                "body",
                &tailed_value,
                &["text".into()]
            )
            .unwrap()["text"],
            b"root tail"
        );

        let runtime = ContentManifestRuntime::new(
            global_content_manifest_adapters(),
            context(),
            provider.immutable_store(),
        );
        let merged = runtime
            .merge_cells(&manifest_schema, &[tailed_value, equivalent_value])
            .unwrap();
        assert_eq!(
            runtime
                .materialize_cell(&manifest_schema, &merged, &MaterializationRequest::Full)
                .unwrap(),
            b"root tail"
        );
    }

    #[test]
    fn public_text_schema_rejects_bounds_above_intrinsic_format_before_cell_admission() {
        let schema_with = |tail_entry_type, entries, bytes| {
            JazzSchema::new([TableSchema::new(
                "documents",
                [ColumnSchema::content_manifest(
                    "body",
                    ContentManifestSchema::with_tail_entry_type(
                        TEXT_ADAPTER_KIND,
                        tail_entry_type,
                        entries,
                        bytes,
                    )
                    .unwrap(),
                )],
            )])
        };
        assert!(
            std::panic::catch_unwind(|| {
                schema_with(ValueType::Bytes, TEXT_MAX_TAIL_ENTRIES + 1, 1024)
            })
            .is_err(),
            "65 operations must be rejected during public schema construction"
        );
        assert!(
            std::panic::catch_unwind(|| schema_with(ValueType::Bytes, 8, 20_000)).is_err(),
            "20,000 tail bytes must be rejected during public schema construction"
        );
        assert!(
            std::panic::catch_unwind(|| schema_with(ValueType::String, 8, 1024)).is_err(),
            "text manifests must reject a non-byte typed tail during schema construction"
        );

        let schema = schema_with(ValueType::Bytes, TEXT_MAX_TAIL_ENTRIES, TEXT_MAX_TAIL_BYTES);
        let column = &schema.tables[0].columns[0];
        let manifest_schema = column.content_manifest.as_ref().unwrap();
        let value = ContentManifest {
            root: ContentId([0; 32]),
            edit_tail: vec![],
        }
        .into_value(manifest_schema)
        .unwrap();
        crate::node::codec::validate_cell_value(column, &value).unwrap();
    }
}
