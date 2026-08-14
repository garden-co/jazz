//! The `text-v1` embedded-content adapter.
//!
//! The public seam is [`TextContentAdapter`].  It is intentionally expressed
//! through `content_manifest` rather than through a second mutable document or
//! version table: the application cell containing a [`ContentManifest`] is the
//! mutable identity and every historical row version already retains a complete
//! text snapshot.

use std::collections::BTreeMap;

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
            schema: ContentManifestSchema::new(
                TEXT_ADAPTER_KIND,
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
        let current = self.materialize(manifest, &MaterializationRequest::Full, context, store)?;
        let current = std::str::from_utf8(&current).map_err(|_| ManifestError::Malformed)?;
        let updated = insert_at_code_point(current, at_code_point, text)?;
        let encoded = encode_edit(&TextEdit {
            at_code_point,
            text: text.to_owned(),
        });
        let mut tail = manifest.edit_tail.clone();
        tail.push(encoded);
        let candidate = ContentManifest {
            root: manifest.root,
            edit_tail: tail,
        };
        if candidate.validate(&self.schema).is_ok() {
            return Ok(candidate);
        }
        let root = self.build_root(&updated, context.domain, store)?;
        Ok(ContentManifest {
            root,
            edit_tail: Vec::new(),
        })
    }

    /// Decodes a text operation for callers that need to inspect a tail.
    pub fn decode_edit(operation: &[u8]) -> Result<TextEdit, ManifestError> {
        decode_edit(operation)
    }

    fn build_root(
        &self,
        text: &str,
        domain: ContentDomainId,
        store: &mut dyn ImmutableContentStore,
    ) -> Result<ContentId, ManifestError> {
        let leaves = split_utf8(text, 4096)?
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
        mut level: Vec<Rope>,
        domain: ContentDomainId,
        store: &mut dyn ImmutableContentStore,
    ) -> Result<Rope, ManifestError> {
        debug_assert!(!level.is_empty());
        while level.len() > 1 {
            let mut next = Vec::with_capacity(level.len().div_ceil(2));
            let mut entries = level.into_iter();
            while let Some(left) = entries.next() {
                match entries.next() {
                    Some(right) => next.push(self.store_branch(left, right, domain, store)?),
                    None => next.push(left),
                }
            }
            level = next;
        }
        Ok(level.pop().expect("nonempty text rope"))
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

    fn load_root(
        &self,
        root: ContentId,
        context: ContentReadContext,
        store: &dyn ImmutableContentStore,
    ) -> Result<Rope, ManifestError> {
        let payload = checked_get(store, context, root, ImmutableContentKind::Root)?;
        let (child, length, height) = parse_root(payload)?;
        let tree = self.load_tree(child, context, store)?;
        if tree.length() != length || tree.height() != height {
            return Err(ManifestError::Malformed);
        }
        Ok(tree)
    }

    fn load_tree(
        &self,
        id: ContentId,
        context: ContentReadContext,
        store: &dyn ImmutableContentStore,
    ) -> Result<Rope, ManifestError> {
        let bytes = store.get(context, id).ok_or(ManifestError::Malformed)?;
        if bytes.starts_with(LEAF_TAG) {
            let payload = checked_get(store, context, id, ImmutableContentKind::Leaf)?;
            let (text, length) = parse_leaf(payload)?;
            return Ok(Rope::Leaf { id, text, length });
        }
        let payload = checked_get(store, context, id, ImmutableContentKind::Node)?;
        let (left_id, right_id, length, height) = parse_branch(payload)?;
        let left = self.load_tree(left_id, context, store)?;
        let right = self.load_tree(right_id, context, store)?;
        if length
            != left
                .length()
                .checked_add(right.length())
                .ok_or(ManifestError::Malformed)?
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

    fn apply_tail(&self, root: Rope, tail: &[Vec<u8>]) -> Result<String, ManifestError> {
        let mut text = String::new();
        root.text(&mut text);
        for operation in tail {
            let edit = decode_edit(operation)?;
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

    fn validate_operation(&self, operation: &[u8]) -> Result<(), ManifestError> {
        decode_edit(operation).map(|_| ())
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
    if bytes.len() != 17 + byte_len {
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
    use crate::content_manifest::MemoryImmutableContentStore;

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
            edit_tail: vec![b"not an edit".to_vec()],
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
}
