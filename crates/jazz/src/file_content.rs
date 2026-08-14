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

/// A byte edit encoded in the `editTail`.  Offsets are against the root, and
/// operations are applied in vector order.  Multiple entries can target
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
        let bytes = self.materialize(manifest, &MaterializationRequest::Full, context, store)?;
        Ok(ContentManifest {
            root: self.store_bytes(&bytes, context, store)?,
            edit_tail: Vec::new(),
        })
    }

    fn root_bytes(
        &self,
        root: ContentId,
        context: ContentReadContext,
        store: &dyn ImmutableContentStore,
    ) -> Result<Vec<u8>, ManifestError> {
        let root = store.get(context, root).ok_or(ManifestError::Malformed)?;
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
        ) as usize;
        let mut out = Vec::with_capacity(length);
        read_object(id, context, store, &mut out)?;
        if out.len() != length {
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
        let mut value = self.root_bytes(manifest.root, context, store)?;
        for encoded in &manifest.edit_tail {
            apply(&mut value, &Self::decode_edit(encoded)?)?;
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
            .map(|raw| Ok((range(&Self::decode_edit(&raw)?).0, raw)))
            .collect::<Result<_, ManifestError>>()?;
        for (i, (_, raw)) in decoded.iter().enumerate() {
            let left = Self::decode_edit(raw)?;
            for (_, other) in &decoded[i + 1..] {
                let right = Self::decode_edit(other)?;
                if overlaps(&left, &right) {
                    return Err(ManifestError::Conflict("overlapping file edits"));
                }
            }
        }
        // Offsets are rooted in the immutable base. Apply right-to-left so
        // an insertion/deletion cannot shift an earlier edit's coordinate.
        decoded.sort_by(|(left, _), (right, _)| right.cmp(left));
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
        let len = object_length(bytes, context, store)?;
        out.extend_from_slice(&child.0);
        out.extend_from_slice(&(len as u64).to_le_bytes());
    }
    Ok(out)
}
fn object_length(
    bytes: &[u8],
    context: ContentReadContext,
    store: &dyn ImmutableContentStore,
) -> Result<usize, ManifestError> {
    if bytes.starts_with(b"FLF1") {
        Ok(bytes.len() - 4)
    } else if bytes.starts_with(b"FND1") {
        let mut out = Vec::new();
        let id = ContentId([0; 32]);
        let _ = id;
        let count = *bytes.get(4).ok_or(ManifestError::Malformed)? as usize;
        if bytes.len() != 5 + count * 40 {
            return Err(ManifestError::Malformed);
        }
        for i in 0..count {
            let p = 5 + i * 40 + 32;
            out.extend_from_slice(&bytes[p..p + 8]);
        }
        Ok(out
            .chunks_exact(8)
            .map(|b| u64::from_le_bytes(b.try_into().unwrap()) as usize)
            .sum())
    } else {
        let _ = (context, store);
        Err(ManifestError::Malformed)
    }
}
fn read_object(
    id: ContentId,
    context: ContentReadContext,
    store: &dyn ImmutableContentStore,
    out: &mut Vec<u8>,
) -> Result<(), ManifestError> {
    let bytes = store.get(context, id).ok_or(ManifestError::Malformed)?;
    if bytes.starts_with(b"FLF1") {
        out.extend_from_slice(&bytes[4..]);
        return Ok(());
    }
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
        read_object(child, context, store, out)?;
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::content_manifest::MemoryImmutableContentStore;
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
}
