//! `content.file-v1`: immutable, content-addressed byte extent trees plus a
//! bounded, manifest-local set of byte edits.
//!
//! This adapter intentionally has no network object-store implementation.  An
//! external descriptor/signing service remains a server boundary; see
//! [`FileUploadReceipt`].  Inline leaves are nevertheless sufficient to make
//! the tree, edit-tail, history, merge, index, and domain boundaries concrete.
#![allow(missing_docs)]

use std::collections::{BTreeMap, BTreeSet};

use crate::content_manifest::{
    ContentAddress, ContentDomainId, ContentId, ContentManifest, ContentManifestAdapter,
    ContentReadContext, ImmutableContentKind, ImmutableContentStore, ManifestError,
    MaterializationRequest,
};

pub const FILE_ADAPTER_KIND: &str = "file-v1";
const LEAF_BYTES: usize = 4096;
const FANOUT: usize = 32;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileUploadReceipt {
    pub domain: ContentDomainId,
    pub digest: [u8; 32],
    pub byte_length: u64,
    pub generation: String,
    pub key_version: u32,
}

/// A byte edit encoded in the `editTail`. Every offset is against the immutable
/// base root, never a preceding tail operation. Multiple entries can target
/// independent extents; this is deliberately not a single append frontier.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FileEdit {
    Overwrite {
        offset: u64,
        delete: u64,
        bytes: Vec<u8>,
    },
    Insert {
        offset: u64,
        bytes: Vec<u8>,
    },
    Delete {
        offset: u64,
        delete: u64,
    },
}

#[derive(Default)]
pub struct FileContentAdapter;

impl FileContentAdapter {
    pub fn encode_edit(edit: &FileEdit) -> Vec<u8> {
        let (tag, offset, delete, bytes) = match edit {
            FileEdit::Overwrite {
                offset,
                delete,
                bytes,
            } => (1, *offset, *delete, bytes.as_slice()),
            FileEdit::Insert { offset, bytes } => (2, *offset, 0, bytes.as_slice()),
            FileEdit::Delete { offset, delete } => (3, *offset, *delete, &[] as &[u8]),
        };
        let mut out = Vec::with_capacity(21 + bytes.len());
        out.push(tag);
        out.extend_from_slice(&offset.to_le_bytes());
        out.extend_from_slice(&delete.to_le_bytes());
        out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(bytes);
        out
    }

    pub fn decode_edit(bytes: &[u8]) -> Result<FileEdit, ManifestError> {
        if bytes.len() < 21 {
            return Err(ManifestError::Malformed);
        }
        let offset = u64::from_le_bytes(
            bytes[1..9]
                .try_into()
                .map_err(|_| ManifestError::Malformed)?,
        );
        let delete = u64::from_le_bytes(
            bytes[9..17]
                .try_into()
                .map_err(|_| ManifestError::Malformed)?,
        );
        let count = u32::from_le_bytes(
            bytes[17..21]
                .try_into()
                .map_err(|_| ManifestError::Malformed)?,
        ) as usize;
        if bytes.len() != 21usize.checked_add(count).ok_or(ManifestError::Malformed)? {
            return Err(ManifestError::Malformed);
        }
        let data = bytes[21..].to_vec();
        match bytes[0] {
            1 => Ok(FileEdit::Overwrite {
                offset,
                delete,
                bytes: data,
            }),
            2 if delete == 0 => Ok(FileEdit::Insert {
                offset,
                bytes: data,
            }),
            3 if data.is_empty() => Ok(FileEdit::Delete { offset, delete }),
            _ => Err(ManifestError::Malformed),
        }
    }

    /// Creates a persistent fanout-32 tree. Equal leaves/nodes naturally
    /// reuse their address through `put_if_absent_or_identical`; the first
    /// production path-copy writer can therefore retain untouched subtrees.
    pub fn store_bytes(
        &self,
        bytes: &[u8],
        context: ContentReadContext,
        store: &mut dyn ImmutableContentStore,
    ) -> Result<ContentId, ManifestError> {
        let mut level = Vec::new();
        for chunk in bytes.chunks(LEAF_BYTES).chain(if bytes.is_empty() {
            Some(&[][..])
        } else {
            None
        }) {
            level.push(store.put_if_absent_or_identical(
                ContentAddress {
                    domain: context.domain,
                    adapter_kind: FILE_ADAPTER_KIND,
                    kind: ImmutableContentKind::Leaf,
                },
                leaf_payload(chunk),
            )?);
        }
        while level.len() > 1 {
            let mut next = Vec::new();
            for children in level.chunks(FANOUT) {
                let payload = node_payload(children, context, store)?;
                next.push(store.put_if_absent_or_identical(
                    ContentAddress {
                        domain: context.domain,
                        adapter_kind: FILE_ADAPTER_KIND,
                        kind: ImmutableContentKind::Node,
                    },
                    payload,
                )?);
            }
            level = next;
        }
        let mut root = b"FRT1".to_vec();
        root.extend_from_slice(&level[0].0);
        root.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
        store.put_if_absent_or_identical(
            ContentAddress {
                domain: context.domain,
                adapter_kind: FILE_ADAPTER_KIND,
                kind: ImmutableContentKind::Root,
            },
            root,
        )
    }

    /// Consolidation is foreground-only: callers publish its returned empty
    /// tail with their own row update.  It does not attempt unsafe background
    /// CAS against another writer.
    pub fn consolidate(
        &self,
        manifest: &ContentManifest,
        context: ContentReadContext,
        store: &mut dyn ImmutableContentStore,
    ) -> Result<ContentManifest, ManifestError> {
        let total = self.root_length(manifest.root, context, store)?;
        let edits = canonical_edits(&manifest.edit_tail)?;
        if edits.iter().all(|edit| matches!(edit, FileEdit::Overwrite { delete, bytes, .. } if *delete == bytes.len() as u64)) {
            let mut root = manifest.root;
            for edit in edits {
                let FileEdit::Overwrite { offset, bytes, .. } = edit else { unreachable!() };
                root = overwrite_root(root, offset, &bytes, context, store)?;
            }
            return Ok(ContentManifest { root, edit_tail: Vec::new() });
        }
        let segments = planned_segments(total, &edits)?;
        let new_total = total_after_segments(&segments)?;
        let leaves = collect_leaves(manifest.root, context, store)?;
        let mut next = Vec::new();
        for segment in segments {
            match segment {
                Segment::Patch(bytes) => {
                    for chunk in bytes.chunks(LEAF_BYTES) {
                        next.push(self.put_leaf(chunk, context, store)?);
                    }
                }
                Segment::Base { start, length } => {
                    append_base_leaves(&leaves, start, length, context, store, &mut next)?
                }
            }
        }
        Ok(ContentManifest {
            root: self.store_leaf_ids(next, new_total, context, store)?,
            edit_tail: Vec::new(),
        })
    }

    fn put_leaf(
        &self,
        bytes: &[u8],
        context: ContentReadContext,
        store: &mut dyn ImmutableContentStore,
    ) -> Result<ContentId, ManifestError> {
        store.put_if_absent_or_identical(
            ContentAddress {
                domain: context.domain,
                adapter_kind: FILE_ADAPTER_KIND,
                kind: ImmutableContentKind::Leaf,
            },
            leaf_payload(bytes),
        )
    }
    fn store_leaf_ids(
        &self,
        mut level: Vec<ContentId>,
        length: u64,
        context: ContentReadContext,
        store: &mut dyn ImmutableContentStore,
    ) -> Result<ContentId, ManifestError> {
        if level.is_empty() {
            level.push(self.put_leaf(&[], context, store)?);
        }
        while level.len() > 1 {
            let mut next = Vec::new();
            for children in level.chunks(FANOUT) {
                next.push(store.put_if_absent_or_identical(
                    ContentAddress {
                        domain: context.domain,
                        adapter_kind: FILE_ADAPTER_KIND,
                        kind: ImmutableContentKind::Node,
                    },
                    node_payload(children, context, store)?,
                )?);
            }
            level = next;
        }
        let mut root = b"FRT1".to_vec();
        root.extend_from_slice(&level[0].0);
        root.extend_from_slice(&length.to_le_bytes());
        store.put_if_absent_or_identical(
            ContentAddress {
                domain: context.domain,
                adapter_kind: FILE_ADAPTER_KIND,
                kind: ImmutableContentKind::Root,
            },
            root,
        )
    }
    fn root_length(
        &self,
        root: ContentId,
        context: ContentReadContext,
        store: &dyn ImmutableContentStore,
    ) -> Result<u64, ManifestError> {
        let bytes = fetch(root, ImmutableContentKind::Root, context, store)?;
        if bytes.len() != 44 || &bytes[..4] != b"FRT1" {
            return Err(ManifestError::Malformed);
        }
        Ok(u64::from_le_bytes(
            bytes[36..44]
                .try_into()
                .map_err(|_| ManifestError::Malformed)?,
        ))
    }

    fn root_bytes(
        &self,
        root: ContentId,
        context: ContentReadContext,
        store: &dyn ImmutableContentStore,
    ) -> Result<Vec<u8>, ManifestError> {
        let root = fetch(root, ImmutableContentKind::Root, context, store)?;
        if root.len() != 44 || &root[..4] != b"FRT1" {
            return Err(ManifestError::Malformed);
        }
        let id = ContentId(
            root[4..36]
                .try_into()
                .map_err(|_| ManifestError::Malformed)?,
        );
        let length = u64::from_le_bytes(
            root[36..44]
                .try_into()
                .map_err(|_| ManifestError::Malformed)?,
        );
        let length = usize::try_from(length).map_err(|_| ManifestError::Malformed)?;
        let mut out = Vec::with_capacity(length);
        read_object(id, context, store, &mut out)?;
        if out.len() != length {
            return Err(ManifestError::Malformed);
        }
        Ok(out)
    }

    fn root_range(
        &self,
        root: ContentId,
        offset: u64,
        length: u64,
        context: ContentReadContext,
        store: &dyn ImmutableContentStore,
    ) -> Result<Vec<u8>, ManifestError> {
        let root_bytes = fetch(root, ImmutableContentKind::Root, context, store)?;
        if root_bytes.len() != 44 || &root_bytes[..4] != b"FRT1" {
            return Err(ManifestError::Malformed);
        }
        let child = ContentId(
            root_bytes[4..36]
                .try_into()
                .map_err(|_| ManifestError::Malformed)?,
        );
        let total = u64::from_le_bytes(
            root_bytes[36..44]
                .try_into()
                .map_err(|_| ManifestError::Malformed)?,
        );
        if declared_object_length(child, context, store)? != total {
            return Err(ManifestError::Malformed);
        }
        let end = offset.checked_add(length).ok_or(ManifestError::Malformed)?;
        if end > total {
            return Err(ManifestError::Malformed);
        }
        let cap = usize::try_from(length).map_err(|_| ManifestError::Malformed)?;
        let mut out = Vec::with_capacity(cap);
        read_range_object(child, offset, length, context, store, &mut out)?;
        if out.len() != cap {
            return Err(ManifestError::Malformed);
        }
        Ok(out)
    }
}

impl ContentManifestAdapter for FileContentAdapter {
    fn adapter_kind(&self) -> &str {
        FILE_ADAPTER_KIND
    }
    fn validate_operation(&self, operation: &[u8]) -> Result<(), ManifestError> {
        Self::decode_edit(operation).map(|_| ())
    }

    fn materialize(
        &self,
        manifest: &ContentManifest,
        request: &MaterializationRequest,
        context: ContentReadContext,
        store: &dyn ImmutableContentStore,
    ) -> Result<Vec<u8>, ManifestError> {
        if manifest.edit_tail.is_empty() {
            if let MaterializationRequest::Range { offset, length } = request {
                return self.root_range(manifest.root, *offset, *length, context, store);
            }
        }
        if let MaterializationRequest::Range { offset, length } = request {
            let total = self.root_length(manifest.root, context, store)?;
            return materialize_range(
                manifest.root,
                total,
                &canonical_edits(&manifest.edit_tail)?,
                *offset,
                *length,
                context,
                store,
            );
        }
        let mut value = self.root_bytes(manifest.root, context, store)?;
        for edit in canonical_edits(&manifest.edit_tail)? {
            apply(&mut value, &edit)?;
        }
        match request {
            MaterializationRequest::Full | MaterializationRequest::Projection(_) => Ok(value),
            MaterializationRequest::Range { offset, length } => {
                let end = offset
                    .checked_add(*length)
                    .ok_or(ManifestError::Malformed)?;
                let start = usize::try_from(*offset).map_err(|_| ManifestError::Malformed)?;
                let end = usize::try_from(end).map_err(|_| ManifestError::Malformed)?;
                value
                    .get(start..end)
                    .map(ToOwned::to_owned)
                    .ok_or(ManifestError::Malformed)
            }
        }
    }

    fn merge(
        &self,
        manifests: &[ContentManifest],
        _context: ContentReadContext,
        _store: &dyn ImmutableContentStore,
    ) -> Result<ContentManifest, ManifestError> {
        let Some(first) = manifests.first() else {
            return Err(ManifestError::Conflict("no candidates"));
        };
        if manifests.iter().any(|m| m.root != first.root) {
            return Err(ManifestError::Conflict("different roots require rebase"));
        }
        let mut edits = BTreeSet::new();
        for manifest in manifests {
            for raw in &manifest.edit_tail {
                self.validate_operation(raw)?;
                edits.insert(raw.clone());
            }
        }
        let mut decoded: Vec<_> = edits
            .into_iter()
            .map(|raw| Ok((Self::decode_edit(&raw)?, raw)))
            .collect::<Result<_, ManifestError>>()?;
        for (i, (left, _)) in decoded.iter().enumerate() {
            for (right, _) in &decoded[i + 1..] {
                if overlaps(left, right) {
                    return Err(ManifestError::Conflict("overlapping file edits"));
                }
            }
        }
        // Offsets are rooted in the immutable base. Apply right-to-left so
        // an insertion/deletion cannot shift an earlier edit's coordinate.
        decoded.sort_by(|(left, _), (right, _)| edit_order(right, left));
        Ok(ContentManifest {
            root: first.root,
            edit_tail: decoded.into_iter().map(|(_, raw)| raw).collect(),
        })
    }

    fn index_values(
        &self,
        manifest: &ContentManifest,
        requested: &[String],
        context: ContentReadContext,
        store: &dyn ImmutableContentStore,
    ) -> Result<BTreeMap<String, Vec<u8>>, ManifestError> {
        let bytes = self.materialize(manifest, &MaterializationRequest::Full, context, store)?;
        let mut result = BTreeMap::new();
        for name in requested {
            match name.as_str() {
                "byte_length" => {
                    result.insert(name.clone(), (bytes.len() as u64).to_le_bytes().to_vec());
                }
                "blake3" => {
                    result.insert(name.clone(), blake3::hash(&bytes).as_bytes().to_vec());
                }
                _ => return Err(ManifestError::Conflict("unknown file index")),
            };
        }
        Ok(result)
    }
}

fn leaf_payload(bytes: &[u8]) -> Vec<u8> {
    let mut out = b"FLF1".to_vec();
    out.extend_from_slice(bytes);
    out
}
fn node_payload(
    children: &[ContentId],
    context: ContentReadContext,
    store: &dyn ImmutableContentStore,
) -> Result<Vec<u8>, ManifestError> {
    let mut out = b"FND1".to_vec();
    out.push(children.len() as u8);
    for child in children {
        let bytes = store.get(context, *child).ok_or(ManifestError::Malformed)?;
        let len = object_length(bytes)?;
        out.extend_from_slice(&child.0);
        out.extend_from_slice(&(len as u64).to_le_bytes());
    }
    Ok(out)
}
fn object_length(bytes: &[u8]) -> Result<usize, ManifestError> {
    if bytes.starts_with(b"FLF1") {
        Ok(bytes.len() - 4)
    } else if bytes.starts_with(b"FND1") {
        let count = *bytes.get(4).ok_or(ManifestError::Malformed)? as usize;
        if count == 0 || count > FANOUT || bytes.len() != 5 + count * 40 {
            return Err(ManifestError::Malformed);
        }
        let mut sum = 0u64;
        for i in 0..count {
            let p = 5 + i * 40 + 32;
            sum = sum
                .checked_add(u64::from_le_bytes(
                    bytes[p..p + 8]
                        .try_into()
                        .map_err(|_| ManifestError::Malformed)?,
                ))
                .ok_or(ManifestError::Malformed)?;
        }
        usize::try_from(sum).map_err(|_| ManifestError::Malformed)
    } else {
        Err(ManifestError::Malformed)
    }
}
fn fetch(
    id: ContentId,
    kind: ImmutableContentKind,
    context: ContentReadContext,
    store: &dyn ImmutableContentStore,
) -> Result<&[u8], ManifestError> {
    let bytes = store.get(context, id).ok_or(ManifestError::Malformed)?;
    (crate::content_manifest::content_id(context.domain, FILE_ADAPTER_KIND, kind, bytes) == id)
        .then_some(bytes)
        .ok_or(ManifestError::Malformed)
}
fn read_object(
    id: ContentId,
    context: ContentReadContext,
    store: &dyn ImmutableContentStore,
    out: &mut Vec<u8>,
) -> Result<(), ManifestError> {
    let leaf = fetch(id, ImmutableContentKind::Leaf, context, store);
    if let Ok(bytes) = leaf {
        if !bytes.starts_with(b"FLF1") {
            return Err(ManifestError::Malformed);
        }
        out.extend_from_slice(&bytes[4..]);
        return Ok(());
    }
    let bytes = fetch(id, ImmutableContentKind::Node, context, store)?;
    if !bytes.starts_with(b"FND1") {
        return Err(ManifestError::Malformed);
    }
    let count = *bytes.get(4).ok_or(ManifestError::Malformed)? as usize;
    if count == 0 || count > FANOUT || bytes.len() != 5 + count * 40 {
        return Err(ManifestError::Malformed);
    }
    for i in 0..count {
        let at = 5 + i * 40;
        let child = ContentId(
            bytes[at..at + 32]
                .try_into()
                .map_err(|_| ManifestError::Malformed)?,
        );
        let declared = u64::from_le_bytes(
            bytes[at + 32..at + 40]
                .try_into()
                .map_err(|_| ManifestError::Malformed)?,
        );
        let before = out.len();
        read_object(child, context, store, out)?;
        if u64::try_from(out.len() - before).map_err(|_| ManifestError::Malformed)? != declared {
            return Err(ManifestError::Malformed);
        }
    }
    Ok(())
}
fn declared_object_length(
    id: ContentId,
    context: ContentReadContext,
    store: &dyn ImmutableContentStore,
) -> Result<u64, ManifestError> {
    let raw = store.get(context, id).ok_or(ManifestError::Malformed)?;
    if raw.starts_with(b"FLF1") {
        if crate::content_manifest::content_id(
            context.domain,
            FILE_ADAPTER_KIND,
            ImmutableContentKind::Leaf,
            raw,
        ) != id
        {
            return Err(ManifestError::Malformed);
        }
        return u64::try_from(raw.len().checked_sub(4).ok_or(ManifestError::Malformed)?)
            .map_err(|_| ManifestError::Malformed);
    }
    if !raw.starts_with(b"FND1")
        || crate::content_manifest::content_id(
            context.domain,
            FILE_ADAPTER_KIND,
            ImmutableContentKind::Node,
            raw,
        ) != id
    {
        return Err(ManifestError::Malformed);
    }
    let count = *raw.get(4).ok_or(ManifestError::Malformed)? as usize;
    if count == 0 || count > FANOUT || raw.len() != 5 + count * 40 {
        return Err(ManifestError::Malformed);
    }
    (0..count).try_fold(0u64, |sum, i| {
        let at = 5 + i * 40 + 32;
        sum.checked_add(u64::from_le_bytes(
            raw[at..at + 8]
                .try_into()
                .map_err(|_| ManifestError::Malformed)?,
        ))
        .ok_or(ManifestError::Malformed)
    })
}
fn read_range_object(
    id: ContentId,
    offset: u64,
    length: u64,
    context: ContentReadContext,
    store: &dyn ImmutableContentStore,
    out: &mut Vec<u8>,
) -> Result<(), ManifestError> {
    let leaf = fetch(id, ImmutableContentKind::Leaf, context, store);
    if let Ok(bytes) = leaf {
        if !bytes.starts_with(b"FLF1") {
            return Err(ManifestError::Malformed);
        }
        let start = usize::try_from(offset).map_err(|_| ManifestError::Malformed)?;
        let end = usize::try_from(offset.checked_add(length).ok_or(ManifestError::Malformed)?)
            .map_err(|_| ManifestError::Malformed)?;
        out.extend_from_slice(
            bytes
                .get(4 + start..4 + end)
                .ok_or(ManifestError::Malformed)?,
        );
        return Ok(());
    }
    let bytes = fetch(id, ImmutableContentKind::Node, context, store)?;
    if !bytes.starts_with(b"FND1") {
        return Err(ManifestError::Malformed);
    }
    let count = *bytes.get(4).ok_or(ManifestError::Malformed)? as usize;
    if count == 0 || count > FANOUT || bytes.len() != 5 + count * 40 {
        return Err(ManifestError::Malformed);
    }
    let end = offset.checked_add(length).ok_or(ManifestError::Malformed)?;
    let mut cursor = 0u64;
    for i in 0..count {
        let at = 5 + i * 40;
        let child = ContentId(
            bytes[at..at + 32]
                .try_into()
                .map_err(|_| ManifestError::Malformed)?,
        );
        let child_len = u64::from_le_bytes(
            bytes[at + 32..at + 40]
                .try_into()
                .map_err(|_| ManifestError::Malformed)?,
        );
        let next = cursor
            .checked_add(child_len)
            .ok_or(ManifestError::Malformed)?;
        let start = offset.max(cursor);
        let stop = end.min(next);
        if start < stop {
            if declared_object_length(child, context, store)? != child_len {
                return Err(ManifestError::Malformed);
            }
            read_range_object(child, start - cursor, stop - start, context, store, out)?;
        }
        cursor = next;
    }
    if end > cursor {
        return Err(ManifestError::Malformed);
    }
    Ok(())
}
fn overwrite_root(
    root: ContentId,
    offset: u64,
    bytes: &[u8],
    context: ContentReadContext,
    store: &mut dyn ImmutableContentStore,
) -> Result<ContentId, ManifestError> {
    let payload = fetch(root, ImmutableContentKind::Root, context, store)?.to_vec();
    if payload.len() != 44 || &payload[..4] != b"FRT1" {
        return Err(ManifestError::Malformed);
    }
    let child = ContentId(
        payload[4..36]
            .try_into()
            .map_err(|_| ManifestError::Malformed)?,
    );
    let total = u64::from_le_bytes(
        payload[36..44]
            .try_into()
            .map_err(|_| ManifestError::Malformed)?,
    );
    if offset
        .checked_add(bytes.len() as u64)
        .ok_or(ManifestError::Malformed)?
        > total
    {
        return Err(ManifestError::Malformed);
    }
    let child = overwrite_object(child, offset, bytes, context, store)?;
    let mut next = b"FRT1".to_vec();
    next.extend_from_slice(&child.0);
    next.extend_from_slice(&total.to_le_bytes());
    store.put_if_absent_or_identical(
        ContentAddress {
            domain: context.domain,
            adapter_kind: FILE_ADAPTER_KIND,
            kind: ImmutableContentKind::Root,
        },
        next,
    )
}
fn overwrite_object(
    id: ContentId,
    offset: u64,
    replacement: &[u8],
    context: ContentReadContext,
    store: &mut dyn ImmutableContentStore,
) -> Result<ContentId, ManifestError> {
    let raw = store
        .get(context, id)
        .ok_or(ManifestError::Malformed)?
        .to_vec();
    if raw.starts_with(b"FLF1") {
        if crate::content_manifest::content_id(
            context.domain,
            FILE_ADAPTER_KIND,
            ImmutableContentKind::Leaf,
            &raw,
        ) != id
        {
            return Err(ManifestError::Malformed);
        }
        let start = usize::try_from(offset).map_err(|_| ManifestError::Malformed)?;
        let end = start
            .checked_add(replacement.len())
            .ok_or(ManifestError::Malformed)?;
        if end > raw.len() - 4 {
            return Err(ManifestError::Malformed);
        }
        let mut payload = raw;
        payload[4 + start..4 + end].copy_from_slice(replacement);
        return store.put_if_absent_or_identical(
            ContentAddress {
                domain: context.domain,
                adapter_kind: FILE_ADAPTER_KIND,
                kind: ImmutableContentKind::Leaf,
            },
            payload,
        );
    }
    if crate::content_manifest::content_id(
        context.domain,
        FILE_ADAPTER_KIND,
        ImmutableContentKind::Node,
        &raw,
    ) != id
        || !raw.starts_with(b"FND1")
    {
        return Err(ManifestError::Malformed);
    }
    let count = *raw.get(4).ok_or(ManifestError::Malformed)? as usize;
    if count == 0 || count > FANOUT || raw.len() != 5 + count * 40 {
        return Err(ManifestError::Malformed);
    }
    let end = offset
        .checked_add(replacement.len() as u64)
        .ok_or(ManifestError::Malformed)?;
    let mut cursor = 0u64;
    let mut next = raw.clone();
    for i in 0..count {
        let at = 5 + i * 40;
        let len = u64::from_le_bytes(
            raw[at + 32..at + 40]
                .try_into()
                .map_err(|_| ManifestError::Malformed)?,
        );
        let stop = cursor.checked_add(len).ok_or(ManifestError::Malformed)?;
        let a = offset.max(cursor);
        let z = end.min(stop);
        if a < z {
            let child = ContentId(
                raw[at..at + 32]
                    .try_into()
                    .map_err(|_| ManifestError::Malformed)?,
            );
            if declared_object_length(child, context, store)? != len {
                return Err(ManifestError::Malformed);
            }
            let from = usize::try_from(a - offset).map_err(|_| ManifestError::Malformed)?;
            let to = usize::try_from(z - offset).map_err(|_| ManifestError::Malformed)?;
            let changed =
                overwrite_object(child, a - cursor, &replacement[from..to], context, store)?;
            next[at..at + 32].copy_from_slice(&changed.0);
        }
        cursor = stop;
    }
    if end > cursor {
        return Err(ManifestError::Malformed);
    }
    store.put_if_absent_or_identical(
        ContentAddress {
            domain: context.domain,
            adapter_kind: FILE_ADAPTER_KIND,
            kind: ImmutableContentKind::Node,
        },
        next,
    )
}
fn apply(value: &mut Vec<u8>, edit: &FileEdit) -> Result<(), ManifestError> {
    let (offset, delete, insert) = match edit {
        FileEdit::Overwrite {
            offset,
            delete,
            bytes,
        } => (*offset, *delete, bytes.as_slice()),
        FileEdit::Insert { offset, bytes } => (*offset, 0, bytes.as_slice()),
        FileEdit::Delete { offset, delete } => (*offset, *delete, &[] as &[u8]),
    };
    let end = offset.checked_add(delete).ok_or(ManifestError::Malformed)?;
    let start = usize::try_from(offset).map_err(|_| ManifestError::Malformed)?;
    let end = usize::try_from(end).map_err(|_| ManifestError::Malformed)?;
    if start > end || end > value.len() {
        return Err(ManifestError::Malformed);
    }
    value.splice(start..end, insert.iter().copied());
    Ok(())
}
fn range(edit: &FileEdit) -> (u64, u64) {
    match edit {
        FileEdit::Overwrite { offset, delete, .. } => (*offset, offset.saturating_add(*delete)),
        FileEdit::Insert { offset, .. } => (*offset, *offset),
        FileEdit::Delete { offset, delete } => (*offset, offset.saturating_add(*delete)),
    }
}
fn overlaps(a: &FileEdit, b: &FileEdit) -> bool {
    let (as_, ae) = range(a);
    let (bs, be) = range(b);
    as_ < be && bs < ae || (as_ == ae && bs == be && as_ == bs)
}
fn edit_order(a: &FileEdit, b: &FileEdit) -> std::cmp::Ordering {
    let ao = range(a).0;
    let bo = range(b).0;
    ao.cmp(&bo)
        .then_with(|| edit_tag(a).cmp(&edit_tag(b)))
        .then_with(|| FileContentAdapter::encode_edit(a).cmp(&FileContentAdapter::encode_edit(b)))
}
fn edit_tag(edit: &FileEdit) -> u8 {
    match edit {
        FileEdit::Delete { .. } => 0,
        FileEdit::Overwrite { .. } => 1,
        FileEdit::Insert { .. } => 2,
    }
}
fn canonical_edits(raw: &[Vec<u8>]) -> Result<Vec<FileEdit>, ManifestError> {
    let mut edits = raw
        .iter()
        .map(|r| FileContentAdapter::decode_edit(r))
        .collect::<Result<Vec<_>, _>>()?;
    for (i, a) in edits.iter().enumerate() {
        for b in &edits[i + 1..] {
            if overlaps(a, b) {
                return Err(ManifestError::Conflict("overlapping file edits"));
            }
        }
    }
    edits.sort_by(|a, b| {
        range(b)
            .0
            .cmp(&range(a).0)
            .then_with(|| edit_tag(a).cmp(&edit_tag(b)))
            .then_with(|| {
                FileContentAdapter::encode_edit(a).cmp(&FileContentAdapter::encode_edit(b))
            })
    });
    Ok(edits)
}
#[derive(Clone)]
enum Segment {
    Base { start: u64, length: u64 },
    Patch(Vec<u8>),
}
fn planned_segments(total: u64, edits: &[FileEdit]) -> Result<Vec<Segment>, ManifestError> {
    let mut ascending = edits.to_vec();
    ascending.sort_by(edit_order);
    let mut cursor = 0u64;
    let mut out = Vec::new();
    let mut index = 0;
    while index < ascending.len() {
        let offset = range(&ascending[index]).0;
        if offset < cursor || offset > total {
            return Err(ManifestError::Malformed);
        }
        if offset > cursor {
            out.push(Segment::Base {
                start: cursor,
                length: offset - cursor,
            });
        }
        let mut end = offset;
        // Inserts at a coordinate precede the replacement/deletion of the
        // base byte at that coordinate in the logical result.
        while index < ascending.len() && range(&ascending[index]).0 == offset {
            if let FileEdit::Insert { bytes, .. } = &ascending[index] {
                if !bytes.is_empty() {
                    out.push(Segment::Patch(bytes.clone()));
                }
            }
            index += 1;
        }
        let group_start = ascending[..index]
            .iter()
            .rposition(|edit| range(edit).0 != offset)
            .map_or(0, |i| i + 1);
        for edit in &ascending[group_start..index] {
            match edit {
                FileEdit::Overwrite { delete, bytes, .. } => {
                    end = offset
                        .checked_add(*delete)
                        .ok_or(ManifestError::Malformed)?;
                    if !bytes.is_empty() {
                        out.push(Segment::Patch(bytes.clone()));
                    }
                }
                FileEdit::Delete { delete, .. } => {
                    end = offset
                        .checked_add(*delete)
                        .ok_or(ManifestError::Malformed)?
                }
                FileEdit::Insert { .. } => {}
            }
        }
        if end > total {
            return Err(ManifestError::Malformed);
        }
        cursor = end;
    }
    if cursor < total {
        out.push(Segment::Base {
            start: cursor,
            length: total - cursor,
        });
    }
    Ok(out)
}
fn total_after_segments(segments: &[Segment]) -> Result<u64, ManifestError> {
    segments.iter().try_fold(0u64, |n, s| {
        n.checked_add(match s {
            Segment::Base { length, .. } => *length,
            Segment::Patch(b) => b.len() as u64,
        })
        .ok_or(ManifestError::Malformed)
    })
}
fn materialize_range(
    root: ContentId,
    total: u64,
    edits: &[FileEdit],
    offset: u64,
    length: u64,
    context: ContentReadContext,
    store: &dyn ImmutableContentStore,
) -> Result<Vec<u8>, ManifestError> {
    let segments = planned_segments(total, edits)?;
    let final_len = total_after_segments(&segments)?;
    let end = offset.checked_add(length).ok_or(ManifestError::Malformed)?;
    if end > final_len {
        return Err(ManifestError::Malformed);
    }
    let mut cursor: u64 = 0;
    let mut out =
        Vec::with_capacity(usize::try_from(length).map_err(|_| ManifestError::Malformed)?);
    for s in segments {
        let slen = match &s {
            Segment::Base { length, .. } => *length,
            Segment::Patch(b) => b.len() as u64,
        };
        let stop = cursor.checked_add(slen).ok_or(ManifestError::Malformed)?;
        let from = offset.max(cursor);
        let to = end.min(stop);
        if from < to {
            let local = from - cursor;
            let take = to - from;
            match s {
                Segment::Base { start, .. } => out.extend_from_slice(
                    &FileContentAdapter.root_range(root, start + local, take, context, store)?,
                ),
                Segment::Patch(b) => {
                    let a = usize::try_from(local).map_err(|_| ManifestError::Malformed)?;
                    let z = usize::try_from(local + take).map_err(|_| ManifestError::Malformed)?;
                    out.extend_from_slice(b.get(a..z).ok_or(ManifestError::Malformed)?);
                }
            }
        }
        cursor = stop;
    }
    Ok(out)
}
fn collect_leaves(
    root: ContentId,
    context: ContentReadContext,
    store: &dyn ImmutableContentStore,
) -> Result<Vec<(ContentId, u64)>, ManifestError> {
    let root = fetch(root, ImmutableContentKind::Root, context, store)?;
    let id = ContentId(
        root.get(4..36)
            .ok_or(ManifestError::Malformed)?
            .try_into()
            .map_err(|_| ManifestError::Malformed)?,
    );
    let mut out = Vec::new();
    collect_leaf_ids(id, context, store, &mut out)?;
    Ok(out)
}
fn collect_leaf_ids(
    id: ContentId,
    context: ContentReadContext,
    store: &dyn ImmutableContentStore,
    out: &mut Vec<(ContentId, u64)>,
) -> Result<(), ManifestError> {
    if let Ok(b) = fetch(id, ImmutableContentKind::Leaf, context, store) {
        out.push((
            id,
            u64::try_from(b.len() - 4).map_err(|_| ManifestError::Malformed)?,
        ));
        return Ok(());
    };
    let b = fetch(id, ImmutableContentKind::Node, context, store)?;
    let n = *b.get(4).ok_or(ManifestError::Malformed)? as usize;
    if n == 0 || n > FANOUT || b.len() != 5 + n * 40 {
        return Err(ManifestError::Malformed);
    };
    for i in 0..n {
        let at = 5 + i * 40;
        let child = ContentId(
            b[at..at + 32]
                .try_into()
                .map_err(|_| ManifestError::Malformed)?,
        );
        let expected = u64::from_le_bytes(
            b[at + 32..at + 40]
                .try_into()
                .map_err(|_| ManifestError::Malformed)?,
        );
        let before = out.iter().map(|(_, l)| *l).sum::<u64>();
        collect_leaf_ids(child, context, store, out)?;
        let after = out.iter().map(|(_, l)| *l).sum::<u64>();
        if after.checked_sub(before) != Some(expected) {
            return Err(ManifestError::Malformed);
        }
    }
    Ok(())
}
fn append_base_leaves(
    leaves: &[(ContentId, u64)],
    start: u64,
    length: u64,
    context: ContentReadContext,
    store: &mut dyn ImmutableContentStore,
    out: &mut Vec<ContentId>,
) -> Result<(), ManifestError> {
    let end = start.checked_add(length).ok_or(ManifestError::Malformed)?;
    let mut cursor: u64 = 0;
    for (id, len) in leaves {
        let next = cursor.checked_add(*len).ok_or(ManifestError::Malformed)?;
        let a = start.max(cursor);
        let z = end.min(next);
        if a < z {
            if a == cursor && z == next {
                out.push(*id)
            } else {
                let b = fetch(*id, ImmutableContentKind::Leaf, context, store)?;
                let x = usize::try_from(a - cursor).map_err(|_| ManifestError::Malformed)?;
                let y = usize::try_from(z - cursor).map_err(|_| ManifestError::Malformed)?;
                let fragment = b[4 + x..4 + y].to_vec();
                out.push(FileContentAdapter.put_leaf(&fragment, context, store)?);
            }
        }
        cursor = next;
    }
    if end > cursor {
        return Err(ManifestError::Malformed);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::content_manifest::MemoryImmutableContentStore;
    use crate::content_manifest::{
        ContentManifestRuntime, ContentManifestRuntimeProvider, global_content_manifest_adapters,
    };
    use crate::groove::records::Value;
    use crate::groove::schema::ColumnType;
    use crate::groove::storage::RocksDbStorage;
    use crate::ids::{AuthorId, NodeUuid, RowUuid};
    use crate::node::{MergeableCommit, NodeState};
    use crate::schema::{ColumnSchema as JazzColumnSchema, JazzSchema, TableSchema};
    use std::cell::Cell;
    use std::sync::Arc;
    struct CountingStore {
        inner: MemoryImmutableContentStore,
        reads: Cell<usize>,
    }
    impl CountingStore {
        fn new() -> Self {
            Self {
                inner: MemoryImmutableContentStore::default(),
                reads: Cell::new(0),
            }
        }
        fn reads(&self) -> usize {
            self.reads.get()
        }
        fn reset(&self) {
            self.reads.set(0);
        }
    }
    impl ImmutableContentStore for CountingStore {
        fn get(&self, context: ContentReadContext, id: ContentId) -> Option<&[u8]> {
            self.reads.set(self.reads.get() + 1);
            self.inner.get(context, id)
        }
        fn put_if_absent_or_identical(
            &mut self,
            address: ContentAddress<'_>,
            bytes: Vec<u8>,
        ) -> Result<ContentId, ManifestError> {
            self.inner.put_if_absent_or_identical(address, bytes)
        }
    }
    struct CorruptStore;
    impl ImmutableContentStore for CorruptStore {
        fn get(&self, _context: ContentReadContext, _id: ContentId) -> Option<&[u8]> {
            Some(b"FRT1this-is-not-a-content-addressed-root")
        }
        fn put_if_absent_or_identical(
            &mut self,
            _address: ContentAddress<'_>,
            _canonical_bytes: Vec<u8>,
        ) -> Result<ContentId, ManifestError> {
            unreachable!("corrupt fixture is read-only")
        }
    }
    fn ctx(n: u8) -> ContentReadContext {
        ContentReadContext {
            domain: ContentDomainId(uuid::Uuid::from_bytes([n; 16])),
        }
    }
    #[test]
    fn file_tail_is_multi_extent_and_history_is_exact() {
        let mut store = MemoryImmutableContentStore::default();
        let a = FileContentAdapter;
        let context = ctx(1);
        let root = a
            .store_bytes(&vec![b'a'; 9000], context, &mut store)
            .unwrap();
        let old = ContentManifest {
            root,
            edit_tail: vec![],
        };
        let current = ContentManifest {
            root,
            edit_tail: vec![
                FileContentAdapter::encode_edit(&FileEdit::Overwrite {
                    offset: 10,
                    delete: 2,
                    bytes: b"XY".to_vec(),
                }),
                FileContentAdapter::encode_edit(&FileEdit::Overwrite {
                    offset: 7000,
                    delete: 2,
                    bytes: b"ZZ".to_vec(),
                }),
            ],
        };
        assert_eq!(
            a.materialize(
                &old,
                &MaterializationRequest::Range {
                    offset: 10,
                    length: 2
                },
                context,
                &store
            )
            .unwrap(),
            b"aa"
        );
        assert_eq!(
            a.materialize(
                &current,
                &MaterializationRequest::Range {
                    offset: 7000,
                    length: 2
                },
                context,
                &store
            )
            .unwrap(),
            b"ZZ"
        );
        let compact = a.consolidate(&current, context, &mut store).unwrap();
        assert!(compact.edit_tail.is_empty());
        assert_ne!(compact.root, root);
    }
    #[test]
    fn cross_domain_and_corrupt_tree_fail_closed() {
        let mut store = MemoryImmutableContentStore::default();
        let a = FileContentAdapter;
        let one = ctx(1);
        let root = a.store_bytes(b"secret", one, &mut store).unwrap();
        assert!(
            a.materialize(
                &ContentManifest {
                    root,
                    edit_tail: vec![]
                },
                &MaterializationRequest::Full,
                ctx(2),
                &store
            )
            .is_err()
        );
        assert!(a.validate_operation(&[1, 2]).is_err());
        let root_bytes = store.get(one, root).unwrap().to_vec();
        let child = ContentId(root_bytes[4..36].try_into().unwrap());
        let forged = store
            .put_if_absent_or_identical(
                ContentAddress {
                    domain: one.domain,
                    adapter_kind: FILE_ADAPTER_KIND,
                    kind: ImmutableContentKind::Root,
                },
                {
                    let mut bytes = b"FRT1".to_vec();
                    bytes.extend_from_slice(&child.0);
                    bytes.extend_from_slice(&99u64.to_le_bytes());
                    bytes
                },
            )
            .unwrap();
        assert!(
            a.materialize(
                &ContentManifest {
                    root: forged,
                    edit_tail: vec![]
                },
                &MaterializationRequest::Range {
                    offset: 0,
                    length: 1
                },
                one,
                &store,
            )
            .is_err()
        );
        let left = store
            .put_if_absent_or_identical(
                ContentAddress {
                    domain: one.domain,
                    adapter_kind: FILE_ADAPTER_KIND,
                    kind: ImmutableContentKind::Leaf,
                },
                leaf_payload(b"left"),
            )
            .unwrap();
        let right = store
            .put_if_absent_or_identical(
                ContentAddress {
                    domain: one.domain,
                    adapter_kind: FILE_ADAPTER_KIND,
                    kind: ImmutableContentKind::Leaf,
                },
                leaf_payload(b"rght"),
            )
            .unwrap();
        let mut node_bytes = b"FND1".to_vec();
        node_bytes.push(2);
        node_bytes.extend_from_slice(&left.0);
        node_bytes.extend_from_slice(&3u64.to_le_bytes());
        node_bytes.extend_from_slice(&right.0);
        node_bytes.extend_from_slice(&5u64.to_le_bytes());
        let forged_node = store
            .put_if_absent_or_identical(
                ContentAddress {
                    domain: one.domain,
                    adapter_kind: FILE_ADAPTER_KIND,
                    kind: ImmutableContentKind::Node,
                },
                node_bytes,
            )
            .unwrap();
        let mut root_bytes = b"FRT1".to_vec();
        root_bytes.extend_from_slice(&forged_node.0);
        root_bytes.extend_from_slice(&8u64.to_le_bytes());
        let forged_root = store
            .put_if_absent_or_identical(
                ContentAddress {
                    domain: one.domain,
                    adapter_kind: FILE_ADAPTER_KIND,
                    kind: ImmutableContentKind::Root,
                },
                root_bytes,
            )
            .unwrap();
        let forged_manifest = ContentManifest {
            root: forged_root,
            edit_tail: vec![],
        };
        assert!(
            a.materialize(
                &forged_manifest,
                &MaterializationRequest::Range {
                    offset: 0,
                    length: 1
                },
                one,
                &store
            )
            .is_err()
        );
        let forged_edit = ContentManifest {
            root: forged_root,
            edit_tail: vec![FileContentAdapter::encode_edit(&FileEdit::Overwrite {
                offset: 0,
                delete: 1,
                bytes: b"X".to_vec(),
            })],
        };
        assert!(a.consolidate(&forged_edit, one, &mut store).is_err());
        assert!(
            a.materialize(
                &ContentManifest {
                    root,
                    edit_tail: vec![]
                },
                &MaterializationRequest::Full,
                one,
                &CorruptStore,
            )
            .is_err()
        );
    }
    #[test]
    fn merge_materializes_complete_tail_or_conflicts() {
        let mut store = MemoryImmutableContentStore::default();
        let a = FileContentAdapter;
        let context = ctx(1);
        let root = a.store_bytes(b"abcdef", context, &mut store).unwrap();
        let left = ContentManifest {
            root,
            edit_tail: vec![FileContentAdapter::encode_edit(&FileEdit::Overwrite {
                offset: 0,
                delete: 1,
                bytes: b"A".to_vec(),
            })],
        };
        let right = ContentManifest {
            root,
            edit_tail: vec![FileContentAdapter::encode_edit(&FileEdit::Overwrite {
                offset: 5,
                delete: 1,
                bytes: b"F".to_vec(),
            })],
        };
        let merged = a.merge(&[left, right], context, &store).unwrap();
        assert_eq!(
            a.materialize(&merged, &MaterializationRequest::Full, context, &store)
                .unwrap(),
            b"AbcdeF"
        );
        assert!(
            a.index_values(&merged, &["byte_length".into()], context, &store)
                .unwrap()
                .contains_key("byte_length")
        );
    }
    #[test]
    fn tail_offsets_are_anchored_to_the_immutable_base() {
        let mut store = MemoryImmutableContentStore::default();
        let a = FileContentAdapter;
        let context = ctx(1);
        let root = a.store_bytes(b"abcd", context, &mut store).unwrap();
        let insert = FileContentAdapter::encode_edit(&FileEdit::Insert {
            offset: 0,
            bytes: b"X".to_vec(),
        });
        let overwrite = FileContentAdapter::encode_edit(&FileEdit::Overwrite {
            offset: 2,
            delete: 1,
            bytes: b"Y".to_vec(),
        });
        let manifest = ContentManifest {
            root,
            edit_tail: vec![insert.clone(), overwrite.clone()],
        };
        assert_eq!(
            a.materialize(&manifest, &MaterializationRequest::Full, context, &store)
                .unwrap(),
            b"XabYd"
        );
        let merged = a
            .merge(
                &[
                    ContentManifest {
                        root,
                        edit_tail: vec![insert],
                    },
                    ContentManifest {
                        root,
                        edit_tail: vec![overwrite],
                    },
                ],
                context,
                &store,
            )
            .unwrap();
        assert_eq!(
            a.materialize(&merged, &MaterializationRequest::Full, context, &store)
                .unwrap(),
            b"XabYd"
        );
        let same_offset = ContentManifest {
            root,
            edit_tail: vec![
                FileContentAdapter::encode_edit(&FileEdit::Insert {
                    offset: 0,
                    bytes: b"X".to_vec(),
                }),
                FileContentAdapter::encode_edit(&FileEdit::Overwrite {
                    offset: 0,
                    delete: 1,
                    bytes: b"A".to_vec(),
                }),
            ],
        };
        assert_eq!(
            a.materialize(&same_offset, &MaterializationRequest::Full, context, &store)
                .unwrap(),
            b"XAbcd"
        );
        assert_eq!(
            a.materialize(
                &same_offset,
                &MaterializationRequest::Range {
                    offset: 0,
                    length: 2
                },
                context,
                &store
            )
            .unwrap(),
            b"XA"
        );
        let compact = a.consolidate(&same_offset, context, &mut store).unwrap();
        assert_eq!(
            a.materialize(&compact, &MaterializationRequest::Full, context, &store)
                .unwrap(),
            b"XAbcd"
        );
    }
    #[test]
    fn tail_range_and_consolidation_keep_unaffected_leaves_structural() {
        let mut store = CountingStore::new();
        let a = FileContentAdapter;
        let context = ctx(1);
        let source: Vec<u8> = (0..LEAF_BYTES * 9).map(|i| (i % 251) as u8).collect();
        let root = a.store_bytes(&source, context, &mut store).unwrap();
        let manifest = ContentManifest {
            root,
            edit_tail: vec![FileContentAdapter::encode_edit(&FileEdit::Overwrite {
                offset: 4_100,
                delete: 2,
                bytes: b"XY".to_vec(),
            })],
        };
        store.reset();
        assert_eq!(
            a.materialize(
                &manifest,
                &MaterializationRequest::Range {
                    offset: 32_000,
                    length: 16
                },
                context,
                &store,
            )
            .unwrap(),
            source[32_000..32_016]
        );
        // root-length + the selected root/node/leaf branch. A full walk reads
        // all nine leaves (at least eleven objects), so this plant is sensitive
        // to restoring the old full-materialization implementation.
        assert!(
            store.reads() <= 7,
            "unexpected full-tree range hydration: {}",
            store.reads()
        );
        let before = collect_leaves(root, context, &store).unwrap();
        store.reset();
        let compact = a.consolidate(&manifest, context, &mut store).unwrap();
        assert!(
            store.reads() <= 5,
            "consolidation hydrated disjoint leaf payloads: {}",
            store.reads()
        );
        let after = collect_leaves(compact.root, context, &store).unwrap();
        assert_eq!(before[0].0, after[0].0);
        assert!(after.iter().any(|(id, _)| *id == before[8].0));
        assert_eq!(
            a.materialize(
                &compact,
                &MaterializationRequest::Range {
                    offset: 32_000,
                    length: 16
                },
                context,
                &store
            )
            .unwrap(),
            source[32_000..32_016]
        );
    }
    #[test]
    fn registered_file_adapter_runs_through_real_node_manifest_seams() {
        struct Provider {
            context: ContentReadContext,
            store: MemoryImmutableContentStore,
        }
        impl ContentManifestRuntimeProvider for Provider {
            fn read_context(&self, _: NodeUuid) -> ContentReadContext {
                self.context
            }
            fn immutable_store(&self) -> &dyn ImmutableContentStore {
                &self.store
            }
        }
        let context = ctx(7);
        let adapter = FileContentAdapter;
        let mut immutable = MemoryImmutableContentStore::default();
        let root = adapter
            .store_bytes(b"normal jazz row", context, &mut immutable)
            .unwrap();
        let manifest_schema =
            crate::content_manifest::ContentManifestSchema::new(FILE_ADAPTER_KIND, 8, 1024)
                .unwrap();
        let candidate = |offset, byte| {
            Value::Bytes(
                ContentManifest {
                    root,
                    edit_tail: vec![FileContentAdapter::encode_edit(&FileEdit::Overwrite {
                        offset,
                        delete: 1,
                        bytes: vec![byte],
                    })],
                }
                .encode(&manifest_schema)
                .unwrap(),
            )
        };
        let runtime =
            ContentManifestRuntime::new(global_content_manifest_adapters(), context, &immutable);
        let merged = runtime
            .merge_cells(&manifest_schema, &[candidate(0, b'N'), candidate(14, b'!')])
            .unwrap();
        assert_eq!(
            runtime
                .materialize_cell(&manifest_schema, &merged, &MaterializationRequest::Full)
                .unwrap(),
            b"Normal jazz ro!"
        );
        let schema = JazzSchema::new([TableSchema::new(
            "documents",
            [
                JazzColumnSchema::new("owner", ColumnType::Uuid),
                JazzColumnSchema::content_manifest("file", manifest_schema.clone()),
            ],
        )]);
        let temp = tempfile::tempdir().unwrap();
        let cfs = schema.column_families();
        let storage = RocksDbStorage::open(
            temp.path(),
            &cfs.iter().map(String::as_str).collect::<Vec<_>>(),
        )
        .unwrap();
        let mut node = NodeState::new_with_content_manifest_provider(
            NodeUuid::from_bytes([9; 16]),
            schema,
            storage,
            Arc::new(Provider {
                context,
                store: immutable,
            }),
            false,
        )
        .unwrap();
        assert!(
            global_content_manifest_adapters()
                .get(FILE_ADAPTER_KIND)
                .is_ok()
        );
        let cell = Value::Bytes(
            ContentManifest {
                root,
                edit_tail: vec![FileContentAdapter::encode_edit(&FileEdit::Overwrite {
                    offset: 7,
                    delete: 4,
                    bytes: b"Jazz".to_vec(),
                })],
            }
            .encode(&manifest_schema)
            .unwrap(),
        );
        node.commit_mergeable_unit(
            MergeableCommit::new("documents", RowUuid::from_bytes([3; 16]), 1)
                .made_by(AuthorId::from_bytes([4; 16]))
                .cells(BTreeMap::from([
                    ("owner".into(), Value::Uuid(uuid::Uuid::from_bytes([4; 16]))),
                    ("file".into(), cell.clone()),
                ])),
        )
        .unwrap();
        assert_eq!(
            node.materialize_content_manifest(
                "documents",
                "file",
                &cell,
                &MaterializationRequest::Range {
                    offset: 7,
                    length: 4
                }
            )
            .unwrap(),
            b"Jazz"
        );
        let values = node
            .content_manifest_index_values(
                "documents",
                "file",
                &cell,
                &["byte_length".into(), "blake3".into()],
            )
            .unwrap();
        assert_eq!(values["byte_length"], 15u64.to_le_bytes());
    }
}
