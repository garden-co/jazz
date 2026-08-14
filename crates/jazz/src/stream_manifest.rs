//! Ordinary byte streams backed by an embedded content manifest.
//!
//! The owning application row holds the mutable `{ root, editTail }` cell.
//! This module owns only the stream-specific immutable byte tree and the
//! materializer used by manifest-aware consumers.
#![allow(missing_docs)]

use std::collections::BTreeMap;

use crate::content_manifest::{
    ContentAddress, ContentId, ContentManifest, ContentManifestAdapter, ContentReadContext,
    ImmutableContentKind, ImmutableContentStore, ManifestError, MaterializationRequest, content_id,
};

pub const DEFAULT_STREAM_INLINE_TAIL_BYTES: usize = 256;
pub const DEFAULT_STREAM_TREE_FANOUT: usize = 32;
pub const MAX_STREAM_PART_BYTES: usize = 1_048_576;
const ADAPTER_KIND: &str = "stream-v1";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TreeRef {
    id: ContentId,
    length: u64,
    height: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct StreamNode {
    height: u32,
    children: Vec<TreeRef>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct StreamRoot {
    tree: Option<TreeRef>,
    prefix_bytes: u64,
}

/// A byte-stream adapter.  Its tail is exactly zero or one byte operation: a
/// bounded logical inline suffix, not one mutable tail per tree node.
#[derive(Clone, Debug)]
pub struct StreamManifestAdapter {
    inline_tail_bytes: usize,
    fanout: usize,
}

impl Default for StreamManifestAdapter {
    fn default() -> Self {
        Self {
            inline_tail_bytes: DEFAULT_STREAM_INLINE_TAIL_BYTES,
            fanout: DEFAULT_STREAM_TREE_FANOUT,
        }
    }
}

impl StreamManifestAdapter {
    pub fn new(inline_tail_bytes: usize, fanout: usize) -> Result<Self, ManifestError> {
        if inline_tail_bytes == 0 || inline_tail_bytes > MAX_STREAM_PART_BYTES {
            return Err(ManifestError::Conflict(
                "stream inline tail bound is invalid",
            ));
        }
        if !(4..=256).contains(&fanout) || !fanout.is_multiple_of(2) {
            return Err(ManifestError::Conflict("stream tree fanout is invalid"));
        }
        Ok(Self {
            inline_tail_bytes,
            fanout,
        })
    }

    /// Create an empty immutable root and the manifest stored in an owner row.
    pub fn empty_manifest(
        &self,
        context: ContentReadContext,
        store: &mut dyn ImmutableContentStore,
    ) -> Result<ContentManifest, ManifestError> {
        Ok(ContentManifest {
            root: self.put_root(
                context,
                store,
                StreamRoot {
                    tree: None,
                    prefix_bytes: 0,
                },
            )?,
            edit_tail: Vec::new(),
        })
    }

    /// Produce the next owner-row manifest for an append.  Promotion and the
    /// owner-row replacement must be committed by the caller as one transaction.
    pub fn append(
        &self,
        manifest: &ContentManifest,
        bytes: &[u8],
        context: ContentReadContext,
        store: &mut dyn ImmutableContentStore,
    ) -> Result<ContentManifest, ManifestError> {
        let old_tail = self.tail(manifest)?;
        if bytes.is_empty() {
            return Ok(manifest.clone());
        }
        let mut combined = Vec::with_capacity(old_tail.len() + bytes.len());
        combined.extend_from_slice(old_tail);
        combined.extend_from_slice(bytes);
        if combined.len() <= self.inline_tail_bytes {
            return Ok(ContentManifest {
                root: manifest.root,
                edit_tail: vec![combined],
            });
        }

        let old_root = self.get_root(manifest.root, context, store)?;
        let mut tree = old_root.tree;
        for part in combined.chunks(MAX_STREAM_PART_BYTES) {
            let id = self.put_part(context, store, part)?;
            tree = Some(self.append_leaf(
                tree,
                TreeRef {
                    id,
                    length: part.len() as u64,
                    height: 0,
                },
                context,
                store,
            )?);
        }
        let prefix_bytes = old_root
            .prefix_bytes
            .checked_add(combined.len() as u64)
            .ok_or(ManifestError::Conflict("stream length overflow"))?;
        Ok(ContentManifest {
            root: self.put_root(context, store, StreamRoot { tree, prefix_bytes })?,
            edit_tail: Vec::new(),
        })
    }

    fn tail<'a>(&self, manifest: &'a ContentManifest) -> Result<&'a [u8], ManifestError> {
        match manifest.edit_tail.as_slice() {
            [] => Ok(&[]),
            [tail] if tail.len() <= self.inline_tail_bytes => Ok(tail),
            [_] => Err(ManifestError::TailTooLarge {
                actual: manifest.edit_tail[0].len(),
                maximum: self.inline_tail_bytes as u32,
            }),
            _ => Err(ManifestError::Conflict(
                "stream manifests have one logical tail",
            )),
        }
    }

    fn put_part(
        &self,
        context: ContentReadContext,
        store: &mut dyn ImmutableContentStore,
        bytes: &[u8],
    ) -> Result<ContentId, ManifestError> {
        store.put_if_absent_or_identical(
            ContentAddress {
                domain: context.domain,
                adapter_kind: ADAPTER_KIND,
                kind: ImmutableContentKind::Leaf,
            },
            bytes.to_vec(),
        )
    }

    fn put_node(
        &self,
        context: ContentReadContext,
        store: &mut dyn ImmutableContentStore,
        node: StreamNode,
    ) -> Result<ContentId, ManifestError> {
        if node.children.is_empty() || node.children.len() > self.fanout {
            return Err(ManifestError::Malformed);
        }
        if node
            .children
            .iter()
            .any(|child| child.height != node.height)
        {
            return Err(ManifestError::Malformed);
        }
        store.put_if_absent_or_identical(
            ContentAddress {
                domain: context.domain,
                adapter_kind: ADAPTER_KIND,
                kind: ImmutableContentKind::Node,
            },
            encode_node(&node),
        )
    }

    fn put_root(
        &self,
        context: ContentReadContext,
        store: &mut dyn ImmutableContentStore,
        root: StreamRoot,
    ) -> Result<ContentId, ManifestError> {
        if root.tree.map(|tree| tree.length) != Some(root.prefix_bytes) && root.tree.is_some() {
            return Err(ManifestError::Malformed);
        }
        if root.tree.is_none() && root.prefix_bytes != 0 {
            return Err(ManifestError::Malformed);
        }
        store.put_if_absent_or_identical(
            ContentAddress {
                domain: context.domain,
                adapter_kind: ADAPTER_KIND,
                kind: ImmutableContentKind::Root,
            },
            encode_root(root),
        )
    }

    fn get_root(
        &self,
        id: ContentId,
        context: ContentReadContext,
        store: &dyn ImmutableContentStore,
    ) -> Result<StreamRoot, ManifestError> {
        decode_root(self.get_object(context, id, ImmutableContentKind::Root, store)?)
    }

    fn get_node(
        &self,
        id: ContentId,
        context: ContentReadContext,
        store: &dyn ImmutableContentStore,
    ) -> Result<StreamNode, ManifestError> {
        decode_node(self.get_object(context, id, ImmutableContentKind::Node, store)?)
    }

    fn get_object<'a>(
        &self,
        context: ContentReadContext,
        id: ContentId,
        kind: ImmutableContentKind,
        store: &'a dyn ImmutableContentStore,
    ) -> Result<&'a [u8], ManifestError> {
        let bytes = store.get(context, id).ok_or(ManifestError::Malformed)?;
        if content_id(context.domain, ADAPTER_KIND, kind, bytes) != id {
            return Err(ManifestError::Malformed);
        }
        Ok(bytes)
    }

    fn append_leaf(
        &self,
        tree: Option<TreeRef>,
        leaf: TreeRef,
        context: ContentReadContext,
        store: &mut dyn ImmutableContentStore,
    ) -> Result<TreeRef, ManifestError> {
        let Some(tree) = tree else {
            let node = StreamNode {
                height: 0,
                children: vec![leaf],
            };
            return Ok(TreeRef {
                id: self.put_node(context, store, node)?,
                length: leaf.length,
                height: 1,
            });
        };
        let (replacement, split) = self.append_at(tree, leaf, context, store)?;
        if let Some(split) = split {
            let node = StreamNode {
                height: replacement.height,
                children: vec![replacement, split],
            };
            let length = replacement.length + split.length;
            return Ok(TreeRef {
                id: self.put_node(context, store, node)?,
                length,
                height: replacement.height + 1,
            });
        }
        Ok(replacement)
    }

    fn append_at(
        &self,
        tree: TreeRef,
        leaf: TreeRef,
        context: ContentReadContext,
        store: &mut dyn ImmutableContentStore,
    ) -> Result<(TreeRef, Option<TreeRef>), ManifestError> {
        let node = self.get_node(tree.id, context, store)?;
        if tree.height != node.height + 1 || node.children.is_empty() {
            return Err(ManifestError::Malformed);
        }
        let mut children = node.children;
        if node.height == 0 {
            if leaf.height != 0 {
                return Err(ManifestError::Malformed);
            }
            children.push(leaf);
        } else {
            let last = children.pop().ok_or(ManifestError::Malformed)?;
            let (replacement, split) = self.append_at(last, leaf, context, store)?;
            children.push(replacement);
            if let Some(split) = split {
                children.push(split);
            }
        }
        self.split_or_store_node(node.height, children, context, store)
    }

    fn split_or_store_node(
        &self,
        height: u32,
        children: Vec<TreeRef>,
        context: ContentReadContext,
        store: &mut dyn ImmutableContentStore,
    ) -> Result<(TreeRef, Option<TreeRef>), ManifestError> {
        if children.len() <= self.fanout {
            let length = children.iter().map(|child| child.length).sum();
            return Ok((
                TreeRef {
                    id: self.put_node(context, store, StreamNode { height, children })?,
                    length,
                    height: height + 1,
                },
                None,
            ));
        }
        let split_at = children.len() / 2;
        let right_children = children[split_at..].to_vec();
        let left_children = children[..split_at].to_vec();
        let left_length = left_children.iter().map(|child| child.length).sum();
        let right_length = right_children.iter().map(|child| child.length).sum();
        let left = TreeRef {
            id: self.put_node(
                context,
                store,
                StreamNode {
                    height,
                    children: left_children,
                },
            )?,
            length: left_length,
            height: height + 1,
        };
        let right = TreeRef {
            id: self.put_node(
                context,
                store,
                StreamNode {
                    height,
                    children: right_children,
                },
            )?,
            length: right_length,
            height: height + 1,
        };
        Ok((left, Some(right)))
    }

    fn tree_range(
        &self,
        tree: TreeRef,
        start: u64,
        end: u64,
        context: ContentReadContext,
        store: &dyn ImmutableContentStore,
        out: &mut Vec<u8>,
    ) -> Result<(), ManifestError> {
        if end > tree.length || start > end {
            return Err(ManifestError::Malformed);
        }
        let node = self.get_node(tree.id, context, store)?;
        if tree.height != node.height + 1 {
            return Err(ManifestError::Malformed);
        }
        let mut offset = 0;
        for child in node.children {
            let child_end = offset + child.length;
            if child_end > start && offset < end {
                let child_start = start.saturating_sub(offset);
                let child_limit = end.min(child_end) - offset;
                if node.height == 0 {
                    let bytes =
                        self.get_object(context, child.id, ImmutableContentKind::Leaf, store)?;
                    if bytes.len() as u64 != child.length {
                        return Err(ManifestError::Malformed);
                    }
                    out.extend_from_slice(&bytes[child_start as usize..child_limit as usize]);
                } else {
                    self.tree_range(child, child_start, child_limit, context, store, out)?;
                }
            }
            offset = child_end;
        }
        if offset != tree.length {
            return Err(ManifestError::Malformed);
        }
        Ok(())
    }

    fn materialize_range(
        &self,
        manifest: &ContentManifest,
        start: u64,
        end: u64,
        context: ContentReadContext,
        store: &dyn ImmutableContentStore,
    ) -> Result<Vec<u8>, ManifestError> {
        let root = self.get_root(manifest.root, context, store)?;
        let tail = self.tail(manifest)?;
        let total = root.prefix_bytes + tail.len() as u64;
        if start > end || end > total {
            return Err(ManifestError::Conflict("stream range is out of bounds"));
        }
        let mut out = Vec::with_capacity((end - start) as usize);
        if let Some(tree) = root.tree {
            if start < root.prefix_bytes {
                self.tree_range(
                    tree,
                    start,
                    end.min(root.prefix_bytes),
                    context,
                    store,
                    &mut out,
                )?;
            }
        }
        if end > root.prefix_bytes {
            let tail_start = start.saturating_sub(root.prefix_bytes) as usize;
            let tail_end = (end - root.prefix_bytes) as usize;
            out.extend_from_slice(&tail[tail_start..tail_end]);
        }
        Ok(out)
    }
}

impl ContentManifestAdapter for StreamManifestAdapter {
    fn adapter_kind(&self) -> &str {
        ADAPTER_KIND
    }

    fn validate_operation(&self, operation: &[u8]) -> Result<(), ManifestError> {
        if operation.len() > self.inline_tail_bytes {
            return Err(ManifestError::TailTooLarge {
                actual: operation.len(),
                maximum: self.inline_tail_bytes as u32,
            });
        }
        Ok(())
    }

    fn materialize(
        &self,
        manifest: &ContentManifest,
        request: &MaterializationRequest,
        context: ContentReadContext,
        store: &dyn ImmutableContentStore,
    ) -> Result<Vec<u8>, ManifestError> {
        let root = self.get_root(manifest.root, context, store)?;
        let total = root.prefix_bytes + self.tail(manifest)?.len() as u64;
        match request {
            MaterializationRequest::Full => {
                self.materialize_range(manifest, 0, total, context, store)
            }
            MaterializationRequest::Range { offset, length } => self.materialize_range(
                manifest,
                *offset,
                offset
                    .checked_add(*length)
                    .ok_or(ManifestError::Conflict("stream range overflow"))?,
                context,
                store,
            ),
            MaterializationRequest::Projection(_) => {
                Err(ManifestError::Conflict("streams have no projections"))
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
            return Err(ManifestError::Conflict("stream merge needs a manifest"));
        };
        if manifests.iter().all(|manifest| manifest == first) {
            Ok(first.clone())
        } else {
            Err(ManifestError::Conflict(
                "concurrent stream manifests need an append merge",
            ))
        }
    }

    fn index_values(
        &self,
        manifest: &ContentManifest,
        requested: &[String],
        context: ContentReadContext,
        store: &dyn ImmutableContentStore,
    ) -> Result<BTreeMap<String, Vec<u8>>, ManifestError> {
        let root = self.get_root(manifest.root, context, store)?;
        let length = root.prefix_bytes + self.tail(manifest)?.len() as u64;
        let mut values = BTreeMap::new();
        for name in requested {
            match name.as_str() {
                "length" => {
                    values.insert(name.clone(), length.to_le_bytes().to_vec());
                }
                _ => return Err(ManifestError::Conflict("unknown stream index value")),
            }
        }
        Ok(values)
    }
}

fn encode_root(root: StreamRoot) -> Vec<u8> {
    let mut out = Vec::with_capacity(45);
    out.extend_from_slice(b"JSR1");
    match root.tree {
        Some(tree) => {
            out.push(1);
            out.extend_from_slice(&tree.id.0);
            out.extend_from_slice(&tree.length.to_le_bytes());
            out.extend_from_slice(&tree.height.to_le_bytes());
        }
        None => out.push(0),
    }
    out.extend_from_slice(&root.prefix_bytes.to_le_bytes());
    out
}

fn decode_root(bytes: &[u8]) -> Result<StreamRoot, ManifestError> {
    if bytes.len() < 13 || &bytes[..4] != b"JSR1" {
        return Err(ManifestError::Malformed);
    }
    let (tree, cursor) = match bytes[4] {
        0 => (None, 5),
        1 if bytes.len() >= 49 => {
            let mut id = [0; 32];
            id.copy_from_slice(&bytes[5..37]);
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
            (
                Some(TreeRef {
                    id: ContentId(id),
                    length,
                    height,
                }),
                49,
            )
        }
        _ => return Err(ManifestError::Malformed),
    };
    if bytes.len() != cursor + 8 {
        return Err(ManifestError::Malformed);
    }
    let prefix_bytes = u64::from_le_bytes(
        bytes[cursor..]
            .try_into()
            .map_err(|_| ManifestError::Malformed)?,
    );
    if tree.map(|tree| tree.length) != Some(prefix_bytes) && tree.is_some() {
        return Err(ManifestError::Malformed);
    }
    if tree.is_none() && prefix_bytes != 0 {
        return Err(ManifestError::Malformed);
    }
    Ok(StreamRoot { tree, prefix_bytes })
}

fn encode_node(node: &StreamNode) -> Vec<u8> {
    let mut out = Vec::with_capacity(12 + node.children.len() * 44);
    out.extend_from_slice(b"JSN1");
    out.extend_from_slice(&node.height.to_le_bytes());
    out.extend_from_slice(&(node.children.len() as u32).to_le_bytes());
    for child in &node.children {
        out.extend_from_slice(&child.id.0);
        out.extend_from_slice(&child.length.to_le_bytes());
        out.extend_from_slice(&child.height.to_le_bytes());
    }
    out
}

fn decode_node(bytes: &[u8]) -> Result<StreamNode, ManifestError> {
    if bytes.len() < 12 || &bytes[..4] != b"JSN1" {
        return Err(ManifestError::Malformed);
    }
    let height = u32::from_le_bytes(
        bytes[4..8]
            .try_into()
            .map_err(|_| ManifestError::Malformed)?,
    );
    let count = u32::from_le_bytes(
        bytes[8..12]
            .try_into()
            .map_err(|_| ManifestError::Malformed)?,
    ) as usize;
    if count == 0 || bytes.len() != 12 + count * 44 {
        return Err(ManifestError::Malformed);
    }
    let mut children = Vec::with_capacity(count);
    for chunk in bytes[12..].chunks_exact(44) {
        let mut id = [0; 32];
        id.copy_from_slice(&chunk[..32]);
        let length = u64::from_le_bytes(
            chunk[32..40]
                .try_into()
                .map_err(|_| ManifestError::Malformed)?,
        );
        let child_height = u32::from_le_bytes(
            chunk[40..]
                .try_into()
                .map_err(|_| ManifestError::Malformed)?,
        );
        if child_height != height {
            return Err(ManifestError::Malformed);
        }
        children.push(TreeRef {
            id: ContentId(id),
            length,
            height: child_height,
        });
    }
    Ok(StreamNode { height, children })
}

#[cfg(test)]
mod tests {
    // Internal because the foundation's adapter/store seam is not yet exposed
    // through a public client API. These tests exercise the canonical content
    // codec and corruption boundary directly until that public vertical slice exists.
    use super::*;
    use crate::content_manifest::{ContentDomainId, MemoryImmutableContentStore};

    fn context() -> ContentReadContext {
        ContentReadContext {
            domain: ContentDomainId(uuid::Uuid::from_bytes([7; 16])),
        }
    }

    #[test]
    fn tail_is_part_of_every_materialized_owner_snapshot() {
        let adapter = StreamManifestAdapter::new(4, 4).unwrap();
        let mut store = MemoryImmutableContentStore::default();
        let empty = adapter.empty_manifest(context(), &mut store).unwrap();
        let first = adapter
            .append(&empty, b"ab", context(), &mut store)
            .unwrap();
        let promoted = adapter
            .append(&first, b"cde", context(), &mut store)
            .unwrap();
        let current = adapter
            .append(&promoted, b"fg", context(), &mut store)
            .unwrap();
        assert_eq!(
            adapter
                .materialize(&first, &MaterializationRequest::Full, context(), &store)
                .unwrap(),
            b"ab"
        );
        assert_eq!(
            adapter
                .materialize(&promoted, &MaterializationRequest::Full, context(), &store)
                .unwrap(),
            b"abcde"
        );
        assert_eq!(
            adapter
                .materialize(
                    &current,
                    &MaterializationRequest::Range {
                        offset: 3,
                        length: 4
                    },
                    context(),
                    &store
                )
                .unwrap(),
            b"defg"
        );
    }

    #[test]
    fn promotion_is_content_addressed_and_keeps_one_logical_tail() {
        let adapter = StreamManifestAdapter::new(3, 4).unwrap();
        let mut store = MemoryImmutableContentStore::default();
        let empty = adapter.empty_manifest(context(), &mut store).unwrap();
        let tail = adapter
            .append(&empty, b"abc", context(), &mut store)
            .unwrap();
        let promoted = adapter.append(&tail, b"d", context(), &mut store).unwrap();
        assert_eq!(tail.edit_tail, vec![b"abc".to_vec()]);
        assert!(promoted.edit_tail.is_empty());
        let repeated = adapter
            .append(&empty, b"abcd", context(), &mut store)
            .unwrap();
        assert_eq!(
            promoted.root, repeated.root,
            "identical immutable stream roots deduplicate"
        );
    }

    #[test]
    fn merge_and_index_see_the_complete_manifest_not_only_its_root() {
        let adapter = StreamManifestAdapter::new(8, 4).unwrap();
        let mut store = MemoryImmutableContentStore::default();
        let empty = adapter.empty_manifest(context(), &mut store).unwrap();
        let tail = adapter
            .append(&empty, b"tail", context(), &mut store)
            .unwrap();
        assert_eq!(
            adapter.merge(&[empty.clone(), tail.clone()], context(), &store),
            Err(ManifestError::Conflict(
                "concurrent stream manifests need an append merge"
            ))
        );
        let index = adapter
            .index_values(&tail, &["length".into()], context(), &store)
            .unwrap();
        assert_eq!(
            u64::from_le_bytes(index["length"].as_slice().try_into().unwrap()),
            4
        );
    }

    #[test]
    fn corrupted_tree_cannot_return_bytes() {
        let adapter = StreamManifestAdapter::new(1, 4).unwrap();
        let mut store = MemoryImmutableContentStore::default();
        let empty = adapter.empty_manifest(context(), &mut store).unwrap();
        let manifest = adapter
            .append(&empty, b"ab", context(), &mut store)
            .unwrap();
        let root = adapter.get_root(manifest.root, context(), &store).unwrap();
        let node = root.tree.unwrap();
        let bad = StreamNode {
            height: 0,
            children: vec![TreeRef {
                id: ContentId([9; 32]),
                length: 2,
                height: 0,
            }],
        };
        let bad_id = store
            .put_if_absent_or_identical(
                ContentAddress {
                    domain: context().domain,
                    adapter_kind: ADAPTER_KIND,
                    kind: ImmutableContentKind::Node,
                },
                encode_node(&bad),
            )
            .unwrap();
        let bad_root = adapter
            .put_root(
                context(),
                &mut store,
                StreamRoot {
                    tree: Some(TreeRef { id: bad_id, ..node }),
                    prefix_bytes: 2,
                },
            )
            .unwrap();
        assert!(
            adapter
                .materialize(
                    &ContentManifest {
                        root: bad_root,
                        edit_tail: vec![]
                    },
                    &MaterializationRequest::Full,
                    context(),
                    &store
                )
                .is_err()
        );
    }
}
