//! Ordinary byte streams backed by an embedded content manifest.
//!
//! The owning application row holds the mutable `{ root, editTail }` cell.
//! This module owns only the stream-specific immutable byte tree and the
//! materializer used by manifest-aware consumers.
#![allow(missing_docs)]

use std::collections::BTreeMap;

use groove::records::{Value, ValueType};

use crate::content_manifest::{
    ContentAddress, ContentId, ContentManifest, ContentManifestAdapter, ContentManifestSchema,
    ContentReadContext, ImmutableContentKind, ImmutableContentStore, ManifestError,
    MaterializationRequest, content_id,
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
    #[cfg(test)]
    fn with_layout_for_test(inline_tail_bytes: usize, fanout: usize) -> Self {
        assert!((1..=MAX_STREAM_PART_BYTES).contains(&inline_tail_bytes));
        assert!((4..=256).contains(&fanout) && fanout.is_multiple_of(2));
        Self {
            inline_tail_bytes,
            fanout,
        }
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
        let combined_len = old_tail
            .len()
            .checked_add(bytes.len())
            .ok_or(ManifestError::Malformed)?;
        let mut combined = Vec::new();
        combined
            .try_reserve_exact(combined_len)
            .map_err(|_| ManifestError::Malformed)?;
        combined.extend_from_slice(old_tail);
        combined.extend_from_slice(bytes);
        if combined.len() <= self.inline_tail_bytes {
            return Ok(ContentManifest {
                root: manifest.root,
                edit_tail: vec![Value::Bytes(combined)],
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
                    length: u64::try_from(part.len()).map_err(|_| ManifestError::Malformed)?,
                    height: 0,
                },
                context,
                store,
            )?);
        }
        let prefix_bytes = old_root
            .prefix_bytes
            .checked_add(u64::try_from(combined.len()).map_err(|_| ManifestError::Malformed)?)
            .ok_or(ManifestError::Malformed)?;
        Ok(ContentManifest {
            root: self.put_root(context, store, StreamRoot { tree, prefix_bytes })?,
            edit_tail: Vec::new(),
        })
    }

    fn tail<'a>(&self, manifest: &'a ContentManifest) -> Result<&'a [u8], ManifestError> {
        match manifest.edit_tail.as_slice() {
            [] => Ok(&[]),
            [Value::Bytes(tail)] if tail.len() <= self.inline_tail_bytes => Ok(tail),
            [Value::Bytes(tail)] => Err(ManifestError::TailTooLarge {
                actual: tail.len(),
                maximum: self.inline_tail_bytes as u32,
            }),
            [_] => Err(ManifestError::Malformed),
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
        let node = decode_node(self.get_object(context, id, ImmutableContentKind::Node, store)?)?;
        if node.children.len() > self.fanout {
            return Err(ManifestError::Malformed);
        }
        Ok(node)
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
            let length = replacement
                .length
                .checked_add(split.length)
                .ok_or(ManifestError::Malformed)?;
            let height = replacement
                .height
                .checked_add(1)
                .ok_or(ManifestError::Malformed)?;
            return Ok(TreeRef {
                id: self.put_node(context, store, node)?,
                length,
                height,
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
        if node.height.checked_add(1) != Some(tree.height) || node.children.is_empty() {
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
            let length = checked_tree_length(&children)?;
            let tree_height = height.checked_add(1).ok_or(ManifestError::Malformed)?;
            return Ok((
                TreeRef {
                    id: self.put_node(context, store, StreamNode { height, children })?,
                    length,
                    height: tree_height,
                },
                None,
            ));
        }
        let split_at = children.len() / 2;
        let right_children = children[split_at..].to_vec();
        let left_children = children[..split_at].to_vec();
        let left_length = checked_tree_length(&left_children)?;
        let right_length = checked_tree_length(&right_children)?;
        let tree_height = height.checked_add(1).ok_or(ManifestError::Malformed)?;
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
            height: tree_height,
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
            height: tree_height,
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
        if node.height.checked_add(1) != Some(tree.height) {
            return Err(ManifestError::Malformed);
        }
        let mut offset = 0_u64;
        for child in node.children {
            let child_end = offset
                .checked_add(child.length)
                .ok_or(ManifestError::Malformed)?;
            if child_end > start && offset < end {
                let child_start = start.saturating_sub(offset);
                let child_limit = end.min(child_end) - offset;
                if node.height == 0 {
                    let bytes =
                        self.get_object(context, child.id, ImmutableContentKind::Leaf, store)?;
                    if u64::try_from(bytes.len()).map_err(|_| ManifestError::Malformed)?
                        != child.length
                    {
                        return Err(ManifestError::Malformed);
                    }
                    let child_start =
                        usize::try_from(child_start).map_err(|_| ManifestError::Malformed)?;
                    let child_limit =
                        usize::try_from(child_limit).map_err(|_| ManifestError::Malformed)?;
                    let range = bytes
                        .get(child_start..child_limit)
                        .ok_or(ManifestError::Malformed)?;
                    out.extend_from_slice(range);
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
        let total = checked_total_length(root.prefix_bytes, tail.len())?;
        if start > end || end > total {
            return Err(ManifestError::Conflict("stream range is out of bounds"));
        }
        let requested_len = usize::try_from(end - start).map_err(|_| ManifestError::Malformed)?;
        let mut out = Vec::new();
        out.try_reserve_exact(requested_len)
            .map_err(|_| ManifestError::Malformed)?;
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
            let tail_start = usize::try_from(start.saturating_sub(root.prefix_bytes))
                .map_err(|_| ManifestError::Malformed)?;
            let tail_end =
                usize::try_from(end - root.prefix_bytes).map_err(|_| ManifestError::Malformed)?;
            let range = tail
                .get(tail_start..tail_end)
                .ok_or(ManifestError::Malformed)?;
            out.extend_from_slice(range);
        }
        Ok(out)
    }
}

impl ContentManifestAdapter for StreamManifestAdapter {
    fn adapter_kind(&self) -> &str {
        ADAPTER_KIND
    }

    fn validate_schema(&self, schema: &ContentManifestSchema) -> Result<(), ManifestError> {
        if schema.adapter_kind != ADAPTER_KIND || schema.tail_entry_type != ValueType::Bytes {
            return Err(ManifestError::InvalidSchema);
        }
        Ok(())
    }

    fn validate_operation(&self, operation: &Value) -> Result<(), ManifestError> {
        let Value::Bytes(operation) = operation else {
            return Err(ManifestError::Malformed);
        };
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
        let total = checked_total_length(root.prefix_bytes, self.tail(manifest)?.len())?;
        match request {
            MaterializationRequest::Full => {
                self.materialize_range(manifest, 0, total, context, store)
            }
            MaterializationRequest::Range { offset, length } => self.materialize_range(
                manifest,
                *offset,
                offset
                    .checked_add(*length)
                    .ok_or(ManifestError::Malformed)?,
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
        let length = checked_total_length(root.prefix_bytes, self.tail(manifest)?.len())?;
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

fn checked_tree_length(children: &[TreeRef]) -> Result<u64, ManifestError> {
    children.iter().try_fold(0_u64, |total, child| {
        total
            .checked_add(child.length)
            .ok_or(ManifestError::Malformed)
    })
}

fn checked_total_length(prefix_bytes: u64, tail_bytes: usize) -> Result<u64, ManifestError> {
    prefix_bytes
        .checked_add(u64::try_from(tail_bytes).map_err(|_| ManifestError::Malformed)?)
        .ok_or(ManifestError::Malformed)
}

fn decode_root(bytes: &[u8]) -> Result<StreamRoot, ManifestError> {
    if bytes.len() < 13 || &bytes[..4] != b"JSR1" {
        return Err(ManifestError::Malformed);
    }
    let (tree, cursor) = match bytes[4] {
        0 => (None, 5_usize),
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
                49_usize,
            )
        }
        _ => return Err(ManifestError::Malformed),
    };
    let expected_len = cursor.checked_add(8).ok_or(ManifestError::Malformed)?;
    if bytes.len() != expected_len {
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
    let encoded_len = node
        .children
        .len()
        .checked_mul(44)
        .and_then(|children_len| 12_usize.checked_add(children_len))
        .expect("bounded stream node encoding length");
    let mut out = Vec::with_capacity(encoded_len);
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
    let count = usize::try_from(u32::from_le_bytes(
        bytes[8..12]
            .try_into()
            .map_err(|_| ManifestError::Malformed)?,
    ))
    .map_err(|_| ManifestError::Malformed)?;
    let encoded_len = count
        .checked_mul(44)
        .and_then(|children_len| 12_usize.checked_add(children_len))
        .ok_or(ManifestError::Malformed)?;
    if count == 0 || bytes.len() != encoded_len {
        return Err(ManifestError::Malformed);
    }
    let mut children = Vec::new();
    children
        .try_reserve_exact(count)
        .map_err(|_| ManifestError::Malformed)?;
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
        let adapter = StreamManifestAdapter::with_layout_for_test(4, 4);
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
        let adapter = StreamManifestAdapter::with_layout_for_test(3, 4);
        let mut store = MemoryImmutableContentStore::default();
        let empty = adapter.empty_manifest(context(), &mut store).unwrap();
        let tail = adapter
            .append(&empty, b"abc", context(), &mut store)
            .unwrap();
        let promoted = adapter.append(&tail, b"d", context(), &mut store).unwrap();
        assert_eq!(tail.edit_tail, vec![Value::Bytes(b"abc".to_vec())]);
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
        let adapter = StreamManifestAdapter::with_layout_for_test(8, 4);
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
        let adapter = StreamManifestAdapter::with_layout_for_test(1, 4);
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

    #[test]
    fn correctly_hashed_oversized_node_is_rejected_by_every_read_path() {
        let adapter = StreamManifestAdapter::with_layout_for_test(1, 4);
        let mut store = MemoryImmutableContentStore::default();
        let children = (0..5)
            .map(|byte| {
                Ok(TreeRef {
                    id: adapter.put_part(context(), &mut store, &[byte])?,
                    length: 1,
                    height: 0,
                })
            })
            .collect::<Result<Vec<_>, ManifestError>>()
            .unwrap();
        let oversized = StreamNode {
            height: 0,
            children,
        };
        // Bypass the writer-side guard to model a correctly content-addressed
        // node received from an untrusted or differently configured writer.
        let node_id = store
            .put_if_absent_or_identical(
                ContentAddress {
                    domain: context().domain,
                    adapter_kind: ADAPTER_KIND,
                    kind: ImmutableContentKind::Node,
                },
                encode_node(&oversized),
            )
            .unwrap();
        let root = adapter
            .put_root(
                context(),
                &mut store,
                StreamRoot {
                    tree: Some(TreeRef {
                        id: node_id,
                        length: 5,
                        height: 1,
                    }),
                    prefix_bytes: 5,
                },
            )
            .unwrap();
        let manifest = ContentManifest {
            root,
            edit_tail: Vec::new(),
        };

        let full = adapter.materialize(&manifest, &MaterializationRequest::Full, context(), &store);
        let range = adapter.materialize(
            &manifest,
            &MaterializationRequest::Range {
                offset: 1,
                length: 2,
            },
            context(),
            &store,
        );
        let append = adapter.append(&manifest, b"xy", context(), &mut store);
        assert_eq!(
            (full, range, append),
            (
                Err(ManifestError::Malformed),
                Err(ManifestError::Malformed),
                Err(ManifestError::Malformed),
            ),
            "full reads, ranges, and later appends must all enforce the fetched-node fanout"
        );
    }

    #[test]
    fn extreme_declared_lengths_fail_closed_without_panicking() {
        let adapter = StreamManifestAdapter::with_layout_for_test(256, 4);
        let mut store = MemoryImmutableContentStore::default();
        let children = [u64::MAX, 1, 1, 1]
            .into_iter()
            .map(|length| TreeRef {
                id: ContentId([3; 32]),
                length,
                height: 0,
            })
            .collect::<Vec<_>>();
        let node_bytes = encode_node(&StreamNode {
            height: 0,
            children,
        });
        let node_id = store
            .put_if_absent_or_identical(
                ContentAddress {
                    domain: context().domain,
                    adapter_kind: ADAPTER_KIND,
                    kind: ImmutableContentKind::Node,
                },
                node_bytes,
            )
            .unwrap();
        let root_bytes = encode_root(StreamRoot {
            tree: Some(TreeRef {
                id: node_id,
                length: u64::MAX,
                height: 1,
            }),
            prefix_bytes: u64::MAX,
        });
        let root = store
            .put_if_absent_or_identical(
                ContentAddress {
                    domain: context().domain,
                    adapter_kind: ADAPTER_KIND,
                    kind: ImmutableContentKind::Root,
                },
                root_bytes,
            )
            .unwrap();
        let without_tail = ContentManifest {
            root,
            edit_tail: Vec::new(),
        };
        let with_tail = ContentManifest {
            root,
            edit_tail: vec![Value::Bytes(vec![1])],
        };

        let outcomes = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let full =
                adapter.materialize(&with_tail, &MaterializationRequest::Full, context(), &store);
            let request_overflow = adapter.materialize(
                &without_tail,
                &MaterializationRequest::Range {
                    offset: u64::MAX,
                    length: 1,
                },
                context(),
                &store,
            );
            let empty_boundary_range = adapter.materialize(
                &without_tail,
                &MaterializationRequest::Range {
                    offset: u64::MAX,
                    length: 0,
                },
                context(),
                &store,
            );
            let mut traversal_bytes = Vec::new();
            let traversal_overflow = adapter.tree_range(
                TreeRef {
                    id: node_id,
                    length: u64::MAX,
                    height: 1,
                },
                u64::MAX,
                u64::MAX,
                context(),
                &store,
                &mut traversal_bytes,
            );
            let index = adapter.index_values(&with_tail, &["length".into()], context(), &store);
            let append = adapter.append(&without_tail, &[9; 257], context(), &mut store);
            (
                full,
                request_overflow,
                empty_boundary_range,
                traversal_overflow,
                index,
                append,
            )
        }))
        .expect("untrusted stream lengths must never panic");

        assert_eq!(
            outcomes,
            (
                Err(ManifestError::Malformed),
                Err(ManifestError::Malformed),
                Ok(Vec::new()),
                Err(ManifestError::Malformed),
                Err(ManifestError::Malformed),
                Err(ManifestError::Malformed),
            ),
            "full/range/index/append/promotion paths reject extreme metadata"
        );
    }

    #[test]
    fn multi_leaf_split_reuses_unchanged_spine_and_ranges_across_parts() {
        let adapter = StreamManifestAdapter::with_layout_for_test(1, 4);
        let mut store = MemoryImmutableContentStore::default();
        let mut manifest = adapter.empty_manifest(context(), &mut store).unwrap();
        let mut after_five = None;
        for byte in 1..=6 {
            manifest = adapter
                .append(
                    &manifest,
                    &vec![byte; MAX_STREAM_PART_BYTES],
                    context(),
                    &mut store,
                )
                .unwrap();
            if byte == 5 {
                after_five = Some(manifest.clone());
            }
        }

        let after_five = after_five.unwrap();
        let five_root = adapter
            .get_root(after_five.root, context(), &store)
            .unwrap()
            .tree
            .unwrap();
        let six_root = adapter
            .get_root(manifest.root, context(), &store)
            .unwrap()
            .tree
            .unwrap();
        assert_eq!(five_root.height, 2, "the fifth leaf must split fanout four");
        assert_eq!(six_root.height, 2);
        let five_children = adapter
            .get_node(five_root.id, context(), &store)
            .unwrap()
            .children;
        let six_children = adapter
            .get_node(six_root.id, context(), &store)
            .unwrap()
            .children;
        assert_eq!(five_children.len(), 2);
        assert_eq!(six_children.len(), 2);
        assert_eq!(
            five_children[0].id, six_children[0].id,
            "appending on the right must reuse the untouched left subtree"
        );

        assert_eq!(
            adapter
                .materialize(
                    &manifest,
                    &MaterializationRequest::Range {
                        offset: MAX_STREAM_PART_BYTES as u64 - 2,
                        length: 4,
                    },
                    context(),
                    &store,
                )
                .unwrap(),
            [1, 1, 2, 2]
        );
        assert_eq!(
            adapter
                .materialize(
                    &after_five,
                    &MaterializationRequest::Range {
                        offset: 5 * MAX_STREAM_PART_BYTES as u64 - 2,
                        length: 2,
                    },
                    context(),
                    &store,
                )
                .unwrap(),
            [5, 5],
            "the older root remains directly readable after right-spine copying"
        );
    }
}
