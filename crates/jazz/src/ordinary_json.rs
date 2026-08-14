//! JSON adapter for the embedded ordinary-content manifest substrate.
//!
//! This module owns JSON's stable logical-node identities and its immutable
//! physical tree.  The owning application row owns the mutable manifest; there
//! is intentionally no JSON-specific mutable head row.
#![allow(missing_docs)]

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::content_manifest::{
    ContentAddress, ContentId, ContentManifest, ContentManifestAdapter, ContentReadContext,
    ImmutableContentKind, ImmutableContentStore, ManifestError, MaterializationRequest,
};

/// Schema adapter discriminator for JSON manifests.
pub const JSON_ADAPTER_KIND: &str = "json-v1";
const ORDER_FANOUT: usize = 32;

/// A typed JSON operation carried by an un-consolidated manifest tail.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum JsonOperation {
    SetScalar {
        op: Uuid,
        target: Uuid,
        value: JsonScalar,
    },
    SetMember {
        op: Uuid,
        object: Uuid,
        key: String,
        value: JsonLiteral,
    },
    RemoveMember {
        op: Uuid,
        object: Uuid,
        key: String,
    },
    InsertArray {
        op: Uuid,
        array: Uuid,
        element: Uuid,
        anchor: Option<Uuid>,
        after: bool,
        value: JsonLiteral,
    },
    Delete {
        op: Uuid,
        target: Uuid,
    },
}

impl JsonOperation {
    pub fn encode(&self) -> Result<Vec<u8>, ManifestError> {
        postcard::to_allocvec(self).map_err(|_| ManifestError::Malformed)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, ManifestError> {
        postcard::from_bytes(bytes).map_err(|_| ManifestError::Malformed)
    }
    fn id(&self) -> Uuid {
        match self {
            Self::SetScalar { op, .. }
            | Self::SetMember { op, .. }
            | Self::RemoveMember { op, .. }
            | Self::InsertArray { op, .. }
            | Self::Delete { op, .. } => *op,
        }
    }
    fn conflict_key(&self) -> String {
        match self {
            Self::SetScalar { target, .. } | Self::Delete { target, .. } => {
                format!("node:{target}")
            }
            Self::SetMember { object, key, .. } | Self::RemoveMember { object, key, .. } => {
                format!("member:{object}:{key}")
            }
            // An insertion is anchored to stable identity. Two authors at one anchor need a
            // rebase rule, rather than an invented order in this first adapter.
            Self::InsertArray { array, anchor, .. } => format!("array:{array}:{anchor:?}"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum JsonScalar {
    Null,
    Bool(bool),
    Number(i64),
    String(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum JsonLiteral {
    Scalar(JsonScalar),
    Object(BTreeMap<String, JsonLiteral>),
    Array(Vec<JsonLiteral>),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum StoredObject {
    Root {
        document: ContentId,
        parent: Option<ContentId>,
    },
    Node(StoredNode),
    Order(OrderNode),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StoredNode {
    logical_id: Uuid,
    kind: StoredKind,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum StoredKind {
    Scalar(JsonScalar),
    Object(BTreeMap<String, ContentId>),
    Array(ContentId),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum OrderNode {
    Leaf(Vec<ContentId>),
    Branch(Vec<(ContentId, u32)>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Materialized {
    id: Uuid,
    value: MaterializedValue,
}
#[derive(Clone, Debug, PartialEq, Eq)]
enum MaterializedValue {
    Scalar(JsonScalar),
    Object(BTreeMap<String, Materialized>),
    Array(Vec<Materialized>),
}

/// JSON implementation of [`ContentManifestAdapter`].
#[derive(Default)]
pub struct OrdinaryJsonAdapter;

/// A disposable broad projection derived from one exact manifest. It is not an
/// authoritative row value and callers must check `manifest_fingerprint` before
/// using it for an eventual index query.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JsonEventualProjectionBundle {
    manifest_fingerprint: [u8; 32],
    values: BTreeMap<String, Vec<u8>>,
}

impl OrdinaryJsonAdapter {
    /// Materialize the small, strongly consistent projections belonging to an
    /// atomic manifest candidate. Jazz currently has no queryable atomic
    /// subfield carrier, so these values stay inside the candidate-level hook
    /// rather than being written as independently mergeable columns.
    pub fn synchronous_projections(
        &self,
        manifest: &ContentManifest,
        requested: &[String],
        context: ContentReadContext,
        store: &dyn ImmutableContentStore,
    ) -> Result<BTreeMap<String, Vec<u8>>, ManifestError> {
        self.index_values(manifest, requested, context, store)
    }

    /// Build a broad eventual projection bundle from an exact manifest.
    pub fn eventual_projection_bundle(
        &self,
        manifest: &ContentManifest,
        requested: &[String],
        context: ContentReadContext,
        store: &dyn ImmutableContentStore,
    ) -> Result<JsonEventualProjectionBundle, ManifestError> {
        Ok(JsonEventualProjectionBundle {
            manifest_fingerprint: Self::manifest_fingerprint(manifest),
            values: self.index_values(manifest, requested, context, store)?,
        })
    }

    /// Read an eventual bundle only when it was derived from this exact root
    /// and tail. This forbids an index result from being paired with a newer or
    /// differently merged manifest.
    pub fn checked_eventual_projection<'a>(
        &self,
        bundle: &'a JsonEventualProjectionBundle,
        manifest: &ContentManifest,
    ) -> Result<&'a BTreeMap<String, Vec<u8>>, ManifestError> {
        (bundle.manifest_fingerprint == Self::manifest_fingerprint(manifest))
            .then_some(&bundle.values)
            .ok_or(ManifestError::Conflict(
                "eventual projection belongs to another manifest",
            ))
    }

    fn manifest_fingerprint(manifest: &ContentManifest) -> [u8; 32] {
        let mut hash = blake3::Hasher::new();
        hash.update(b"jazz-json-manifest-projection-v1\0");
        hash.update(&manifest.root.0);
        for operation in &manifest.edit_tail {
            hash.update(&(operation.len() as u64).to_le_bytes());
            hash.update(operation);
        }
        *hash.finalize().as_bytes()
    }
    /// Store a content-addressed immutable JSON root. This is the publication
    /// half of consolidation; callers publish its returned id in one ordinary
    /// application-row manifest candidate.
    pub fn publish_literal(
        &self,
        literal: &JsonLiteral,
        parent: Option<ContentId>,
        context: ContentReadContext,
        store: &mut dyn ImmutableContentStore,
    ) -> Result<ContentId, ManifestError> {
        let document =
            self.publish_node(&Self::literal_node(Uuid::new_v4(), literal), context, store)?;
        self.put(
            ImmutableContentKind::Root,
            &StoredObject::Root { document, parent },
            context,
            store,
        )
    }

    /// Consolidate a bounded tail into a fresh immutable root without changing
    /// the old root. The caller still owns the atomic application-row update.
    pub fn consolidate(
        &self,
        manifest: &ContentManifest,
        context: ContentReadContext,
        store: &mut dyn ImmutableContentStore,
    ) -> Result<ContentManifest, ManifestError> {
        let mut value = self.load_root(manifest.root, context, store)?;
        for bytes in &manifest.edit_tail {
            self.apply(&mut value, &JsonOperation::decode(bytes)?)?;
        }
        let document = self.publish_node(&value, context, store)?;
        let root = self.put(
            ImmutableContentKind::Root,
            &StoredObject::Root {
                document,
                parent: Some(manifest.root),
            },
            context,
            store,
        )?;
        Ok(ContentManifest {
            root,
            edit_tail: Vec::new(),
        })
    }

    /// Resolve a numeric array position against the authoring manifest into a
    /// stable before/after anchor. The resulting operation never retains the
    /// numeric position, so a concurrent insertion cannot retarget it.
    pub fn author_insert_at_index(
        &self,
        manifest: &ContentManifest,
        array_pointer: &str,
        index: usize,
        op: Uuid,
        element: Uuid,
        value: JsonLiteral,
        context: ContentReadContext,
        store: &dyn ImmutableContentStore,
    ) -> Result<JsonOperation, ManifestError> {
        let mut root = self.load_root(manifest.root, context, store)?;
        for bytes in &manifest.edit_tail {
            self.apply(&mut root, &JsonOperation::decode(bytes)?)?;
        }
        let array = Self::pointer(&root, array_pointer)?;
        let MaterializedValue::Array(children) = &array.value else {
            return Err(ManifestError::Conflict(
                "numeric position target is non-array",
            ));
        };
        if index > children.len() {
            return Err(ManifestError::Conflict("numeric position is out of bounds"));
        }
        let (anchor, after) = if index == 0 {
            (children.first().map(|child| child.id), false)
        } else {
            (Some(children[index - 1].id), true)
        };
        Ok(JsonOperation::InsertArray {
            op,
            array: array.id,
            element,
            anchor,
            after,
            value,
        })
    }

    fn put(
        &self,
        kind: ImmutableContentKind,
        value: &StoredObject,
        context: ContentReadContext,
        store: &mut dyn ImmutableContentStore,
    ) -> Result<ContentId, ManifestError> {
        let bytes = postcard::to_allocvec(value).map_err(|_| ManifestError::Malformed)?;
        store.put_if_absent_or_identical(
            ContentAddress {
                domain: context.domain,
                adapter_kind: JSON_ADAPTER_KIND,
                kind,
            },
            bytes,
        )
    }
    fn get(
        &self,
        id: ContentId,
        context: ContentReadContext,
        store: &dyn ImmutableContentStore,
    ) -> Result<StoredObject, ManifestError> {
        postcard::from_bytes(store.get(context, id).ok_or(ManifestError::Malformed)?)
            .map_err(|_| ManifestError::Malformed)
    }
    fn publish_node(
        &self,
        node: &Materialized,
        context: ContentReadContext,
        store: &mut dyn ImmutableContentStore,
    ) -> Result<ContentId, ManifestError> {
        let kind = match &node.value {
            MaterializedValue::Scalar(value) => StoredKind::Scalar(value.clone()),
            MaterializedValue::Object(members) => {
                let mut stored = BTreeMap::new();
                for (key, child) in members {
                    stored.insert(key.clone(), self.publish_node(child, context, store)?);
                }
                StoredKind::Object(stored)
            }
            MaterializedValue::Array(children) => {
                let ids = children
                    .iter()
                    .map(|child| self.publish_node(child, context, store))
                    .collect::<Result<Vec<_>, _>>()?;
                StoredKind::Array(self.publish_order(&ids, context, store)?)
            }
        };
        self.put(
            ImmutableContentKind::Node,
            &StoredObject::Node(StoredNode {
                logical_id: node.id,
                kind,
            }),
            context,
            store,
        )
    }
    fn publish_order(
        &self,
        ids: &[ContentId],
        context: ContentReadContext,
        store: &mut dyn ImmutableContentStore,
    ) -> Result<ContentId, ManifestError> {
        let mut level = ids
            .chunks(ORDER_FANOUT)
            .map(|chunk| {
                self.put(
                    ImmutableContentKind::Leaf,
                    &StoredObject::Order(OrderNode::Leaf(chunk.to_vec())),
                    context,
                    store,
                )
                .map(|id| (id, chunk.len() as u32))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if level.is_empty() {
            return self.put(
                ImmutableContentKind::Leaf,
                &StoredObject::Order(OrderNode::Leaf(Vec::new())),
                context,
                store,
            );
        }
        while level.len() > 1 {
            level = level
                .chunks(ORDER_FANOUT)
                .map(|chunk| {
                    self.put(
                        ImmutableContentKind::Node,
                        &StoredObject::Order(OrderNode::Branch(chunk.to_vec())),
                        context,
                        store,
                    )
                    .map(|id| (id, chunk.iter().map(|(_, n)| n).sum()))
                })
                .collect::<Result<Vec<_>, _>>()?;
        }
        Ok(level[0].0)
    }
    fn load_root(
        &self,
        root: ContentId,
        context: ContentReadContext,
        store: &dyn ImmutableContentStore,
    ) -> Result<Materialized, ManifestError> {
        match self.get(root, context, store)? {
            StoredObject::Root { document, .. } => self.load_node(document, context, store),
            _ => Err(ManifestError::Malformed),
        }
    }
    fn load_node(
        &self,
        id: ContentId,
        context: ContentReadContext,
        store: &dyn ImmutableContentStore,
    ) -> Result<Materialized, ManifestError> {
        let StoredObject::Node(StoredNode { logical_id, kind }) = self.get(id, context, store)?
        else {
            return Err(ManifestError::Malformed);
        };
        let value = match kind {
            StoredKind::Scalar(value) => MaterializedValue::Scalar(value),
            StoredKind::Object(members) => MaterializedValue::Object(
                members
                    .into_iter()
                    .map(|(key, id)| self.load_node(id, context, store).map(|node| (key, node)))
                    .collect::<Result<_, _>>()?,
            ),
            StoredKind::Array(order) => MaterializedValue::Array(
                self.load_order(order, context, store)?
                    .into_iter()
                    .map(|id| self.load_node(id, context, store))
                    .collect::<Result<_, _>>()?,
            ),
        };
        Ok(Materialized {
            id: logical_id,
            value,
        })
    }
    fn load_order(
        &self,
        id: ContentId,
        context: ContentReadContext,
        store: &dyn ImmutableContentStore,
    ) -> Result<Vec<ContentId>, ManifestError> {
        match self.get(id, context, store)? {
            StoredObject::Order(OrderNode::Leaf(ids)) => Ok(ids),
            StoredObject::Order(OrderNode::Branch(children)) => children
                .into_iter()
                .map(|(id, _)| self.load_order(id, context, store))
                .collect::<Result<Vec<_>, _>>()
                .map(|parts| parts.into_iter().flatten().collect()),
            _ => Err(ManifestError::Malformed),
        }
    }
    fn literal_node(id: Uuid, literal: &JsonLiteral) -> Materialized {
        Materialized {
            id,
            value: match literal {
                JsonLiteral::Scalar(value) => MaterializedValue::Scalar(value.clone()),
                JsonLiteral::Object(values) => MaterializedValue::Object(
                    values
                        .iter()
                        .map(|(key, value)| {
                            (key.clone(), Self::literal_node(Uuid::new_v4(), value))
                        })
                        .collect(),
                ),
                JsonLiteral::Array(values) => MaterializedValue::Array(
                    values
                        .iter()
                        .map(|value| Self::literal_node(Uuid::new_v4(), value))
                        .collect(),
                ),
            },
        }
    }
    fn apply(&self, node: &mut Materialized, op: &JsonOperation) -> Result<(), ManifestError> {
        match op {
            JsonOperation::SetScalar { target, value, .. } => Self::find_mut(node, *target)
                .filter(|node| matches!(node.value, MaterializedValue::Scalar(_)))
                .map(|node| node.value = MaterializedValue::Scalar(value.clone()))
                .ok_or(ManifestError::Conflict(
                    "scalar target is absent or non-scalar",
                )),
            JsonOperation::SetMember {
                object, key, value, ..
            } => Self::find_mut(node, *object)
                .and_then(|node| match &mut node.value {
                    MaterializedValue::Object(members) => Some(members),
                    _ => None,
                })
                .map(|members| {
                    members.insert(key.clone(), Self::literal_node(Uuid::new_v4(), value));
                })
                .ok_or(ManifestError::Conflict(
                    "member target is absent or non-object",
                )),
            JsonOperation::RemoveMember { object, key, .. } => Self::find_mut(node, *object)
                .and_then(|node| match &mut node.value {
                    MaterializedValue::Object(members) => Some(members),
                    _ => None,
                })
                .and_then(|members| members.remove(key))
                .map(|_| ())
                .ok_or(ManifestError::Conflict(
                    "member is absent or target is non-object",
                )),
            JsonOperation::InsertArray {
                array,
                element,
                anchor,
                after,
                value,
                ..
            } => {
                let children = Self::find_mut(node, *array)
                    .and_then(|node| match &mut node.value {
                        MaterializedValue::Array(children) => Some(children),
                        _ => None,
                    })
                    .ok_or(ManifestError::Conflict(
                        "array target is absent or non-array",
                    ))?;
                let at = match anchor {
                    None => {
                        if *after {
                            children.len()
                        } else {
                            0
                        }
                    }
                    Some(anchor) => children
                        .iter()
                        .position(|child| child.id == *anchor)
                        .map(|index| index + usize::from(*after))
                        .ok_or(ManifestError::Conflict("array anchor is absent"))?,
                };
                children.insert(at, Self::literal_node(*element, value));
                Ok(())
            }
            JsonOperation::Delete { target, .. } => Self::delete(node, *target)
                .then_some(())
                .ok_or(ManifestError::Conflict(
                    "delete target is absent or is root",
                )),
        }
    }
    fn find_mut(node: &mut Materialized, id: Uuid) -> Option<&mut Materialized> {
        if node.id == id {
            return Some(node);
        }
        match &mut node.value {
            MaterializedValue::Object(members) => members
                .values_mut()
                .find_map(|child| Self::find_mut(child, id)),
            MaterializedValue::Array(children) => children
                .iter_mut()
                .find_map(|child| Self::find_mut(child, id)),
            MaterializedValue::Scalar(_) => None,
        }
    }
    fn pointer<'a>(
        node: &'a Materialized,
        pointer: &str,
    ) -> Result<&'a Materialized, ManifestError> {
        if pointer.is_empty() {
            return Ok(node);
        }
        let mut tokens = pointer
            .strip_prefix('/')
            .ok_or(ManifestError::Conflict(
                "JSON pointer must start with slash",
            ))?
            .split('/')
            .map(|token| token.replace("~1", "/").replace("~0", "~"));
        tokens.try_fold(node, |node, token| match &node.value {
            MaterializedValue::Object(members) => members
                .get(&token)
                .ok_or(ManifestError::Conflict("JSON pointer target is absent")),
            MaterializedValue::Array(children) => token
                .parse::<usize>()
                .ok()
                .and_then(|index| children.get(index))
                .ok_or(ManifestError::Conflict("JSON pointer target is absent")),
            MaterializedValue::Scalar(_) => Err(ManifestError::Conflict(
                "JSON pointer descends through scalar",
            )),
        })
    }
    fn delete(node: &mut Materialized, id: Uuid) -> bool {
        match &mut node.value {
            MaterializedValue::Object(members) => {
                if let Some(key) = members
                    .iter()
                    .find_map(|(key, value)| (value.id == id).then(|| key.clone()))
                {
                    members.remove(&key);
                    true
                } else {
                    members.values_mut().any(|child| Self::delete(child, id))
                }
            }
            MaterializedValue::Array(children) => {
                if let Some(index) = children.iter().position(|child| child.id == id) {
                    children.remove(index);
                    true
                } else {
                    children.iter_mut().any(|child| Self::delete(child, id))
                }
            }
            MaterializedValue::Scalar(_) => false,
        }
    }
    fn json(node: &Materialized) -> serde_json::Value {
        match &node.value {
            MaterializedValue::Scalar(JsonScalar::Null) => serde_json::Value::Null,
            MaterializedValue::Scalar(JsonScalar::Bool(value)) => (*value).into(),
            MaterializedValue::Scalar(JsonScalar::Number(value)) => (*value).into(),
            MaterializedValue::Scalar(JsonScalar::String(value)) => value.clone().into(),
            MaterializedValue::Object(members) => serde_json::Value::Object(
                members
                    .iter()
                    .map(|(key, value)| (key.clone(), Self::json(value)))
                    .collect(),
            ),
            MaterializedValue::Array(values) => {
                serde_json::Value::Array(values.iter().map(Self::json).collect())
            }
        }
    }
}

impl ContentManifestAdapter for OrdinaryJsonAdapter {
    fn adapter_kind(&self) -> &str {
        JSON_ADAPTER_KIND
    }
    fn validate_operation(&self, bytes: &[u8]) -> Result<(), ManifestError> {
        JsonOperation::decode(bytes).map(|_| ())
    }
    fn materialize(
        &self,
        manifest: &ContentManifest,
        request: &MaterializationRequest,
        context: ContentReadContext,
        store: &dyn ImmutableContentStore,
    ) -> Result<Vec<u8>, ManifestError> {
        let mut node = self.load_root(manifest.root, context, store)?;
        for bytes in &manifest.edit_tail {
            self.apply(&mut node, &JsonOperation::decode(bytes)?)?;
        }
        let full = Self::json(&node);
        match request {
            MaterializationRequest::Full | MaterializationRequest::Range { .. } => {
                serde_json::to_vec(&full).map_err(|_| ManifestError::Malformed)
            }
            MaterializationRequest::Projection(paths) => serde_json::to_vec(
                &paths
                    .iter()
                    .map(|path| (path, full.pointer(path)))
                    .collect::<BTreeMap<_, _>>(),
            )
            .map_err(|_| ManifestError::Malformed),
        }
    }
    fn merge(
        &self,
        manifests: &[ContentManifest],
        _: ContentReadContext,
        _: &dyn ImmutableContentStore,
    ) -> Result<ContentManifest, ManifestError> {
        let Some(first) = manifests.first() else {
            return Err(ManifestError::Conflict("no candidates"));
        };
        if manifests.iter().any(|manifest| manifest.root != first.root) {
            return Err(ManifestError::Conflict("unproven root descendant"));
        }
        let mut operations = BTreeMap::new();
        let mut keys = BTreeSet::new();
        for manifest in manifests {
            for bytes in &manifest.edit_tail {
                let op = JsonOperation::decode(bytes)?;
                if !operations.contains_key(&op.id()) && !keys.insert(op.conflict_key()) {
                    return Err(ManifestError::Conflict("noncommuting typed operations"));
                }
                operations.insert(op.id(), bytes.clone());
            }
        }
        Ok(ContentManifest {
            root: first.root,
            edit_tail: operations.into_values().collect(),
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
        let json: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(|_| ManifestError::Malformed)?;
        requested
            .iter()
            .map(|path| {
                let value = json
                    .pointer(path)
                    .ok_or(ManifestError::Conflict("requested projection is absent"))?;
                serde_json::to_vec(value)
                    .map(|value| (path.clone(), value))
                    .map_err(|_| ManifestError::Malformed)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::content_manifest::{ContentDomainId, MemoryImmutableContentStore};

    fn context() -> ContentReadContext {
        ContentReadContext {
            domain: ContentDomainId(Uuid::from_bytes([9; 16])),
        }
    }
    fn seeded() -> (
        OrdinaryJsonAdapter,
        MemoryImmutableContentStore,
        ContentManifest,
        Materialized,
    ) {
        let adapter = OrdinaryJsonAdapter;
        let mut store = MemoryImmutableContentStore::default();
        let root = adapter
            .publish_literal(
                &JsonLiteral::Object(BTreeMap::from([
                    (
                        "status".to_owned(),
                        JsonLiteral::Scalar(JsonScalar::String("open".to_owned())),
                    ),
                    (
                        "items".to_owned(),
                        JsonLiteral::Array(vec![JsonLiteral::Scalar(JsonScalar::Number(1))]),
                    ),
                ])),
                None,
                context(),
                &mut store,
            )
            .unwrap();
        let node = adapter.load_root(root, context(), &store).unwrap();
        (
            adapter,
            store,
            ContentManifest {
                root,
                edit_tail: Vec::new(),
            },
            node,
        )
    }

    #[test]
    fn tail_is_materialized_for_reads_projections_and_consolidation() {
        let (adapter, mut store, base, node) = seeded();
        let status = match &node.value {
            MaterializedValue::Object(values) => values["status"].id,
            _ => unreachable!(),
        };
        let manifest = ContentManifest {
            root: base.root,
            edit_tail: vec![
                JsonOperation::SetScalar {
                    op: Uuid::from_bytes([1; 16]),
                    target: status,
                    value: JsonScalar::String("closed".to_owned()),
                }
                .encode()
                .unwrap(),
            ],
        };
        assert_eq!(
            adapter
                .materialize(&manifest, &MaterializationRequest::Full, context(), &store)
                .unwrap(),
            br#"{"items":[1],"status":"closed"}"#
        );
        assert_eq!(
            adapter
                .index_values(&manifest, &["/status".to_owned()], context(), &store)
                .unwrap()["/status"],
            br#""closed""#
        );
        let consolidated = adapter
            .consolidate(&manifest, context(), &mut store)
            .unwrap();
        assert!(consolidated.edit_tail.is_empty());
        assert_eq!(
            adapter
                .materialize(&base, &MaterializationRequest::Full, context(), &store)
                .unwrap(),
            br#"{"items":[1],"status":"open"}"#
        );
        assert_eq!(
            adapter
                .materialize(
                    &consolidated,
                    &MaterializationRequest::Full,
                    context(),
                    &store
                )
                .unwrap(),
            br#"{"items":[1],"status":"closed"}"#
        );
    }

    #[test]
    fn numeric_authoring_position_becomes_a_stable_array_anchor() {
        let (adapter, store, base, node) = seeded();
        let anchor = match &node.value {
            MaterializedValue::Object(values) => match &values["items"].value {
                MaterializedValue::Array(values) => values[0].id,
                _ => unreachable!(),
            },
            _ => unreachable!(),
        };
        let operation = adapter
            .author_insert_at_index(
                &base,
                "/items",
                1,
                Uuid::from_bytes([2; 16]),
                Uuid::from_bytes([3; 16]),
                JsonLiteral::Scalar(JsonScalar::Number(2)),
                context(),
                &store,
            )
            .unwrap();
        assert!(
            matches!(operation, JsonOperation::InsertArray { anchor: Some(found), after: true, .. } if found == anchor)
        );
        let manifest = ContentManifest {
            root: base.root,
            edit_tail: vec![operation.encode().unwrap()],
        };
        assert_eq!(
            adapter
                .materialize(&manifest, &MaterializationRequest::Full, context(), &store)
                .unwrap(),
            br#"{"items":[1,2],"status":"open"}"#
        );
    }

    #[test]
    fn merge_refuses_noncommuting_or_unproven_roots() {
        let (adapter, store, base, node) = seeded();
        let status = match &node.value {
            MaterializedValue::Object(values) => values["status"].id,
            _ => unreachable!(),
        };
        let a = ContentManifest {
            root: base.root,
            edit_tail: vec![
                JsonOperation::SetScalar {
                    op: Uuid::from_bytes([4; 16]),
                    target: status,
                    value: JsonScalar::String("a".to_owned()),
                }
                .encode()
                .unwrap(),
            ],
        };
        let b = ContentManifest {
            root: base.root,
            edit_tail: vec![
                JsonOperation::SetScalar {
                    op: Uuid::from_bytes([5; 16]),
                    target: status,
                    value: JsonScalar::String("b".to_owned()),
                }
                .encode()
                .unwrap(),
            ],
        };
        assert_eq!(
            adapter.merge(&[a.clone(), b], context(), &store),
            Err(ManifestError::Conflict("noncommuting typed operations"))
        );
        assert_eq!(
            adapter.merge(
                &[
                    a,
                    ContentManifest {
                        root: ContentId([8; 32]),
                        edit_tail: Vec::new()
                    }
                ],
                context(),
                &store
            ),
            Err(ManifestError::Conflict("unproven root descendant"))
        );
    }

    #[test]
    fn synchronous_and_eventual_projections_are_bound_to_the_full_manifest() {
        let (adapter, store, base, node) = seeded();
        let status = match &node.value {
            MaterializedValue::Object(values) => values["status"].id,
            _ => unreachable!(),
        };
        let edited = ContentManifest {
            root: base.root,
            edit_tail: vec![
                JsonOperation::SetScalar {
                    op: Uuid::from_bytes([6; 16]),
                    target: status,
                    value: JsonScalar::String("closed".to_owned()),
                }
                .encode()
                .unwrap(),
            ],
        };
        assert_eq!(
            adapter
                .synchronous_projections(&edited, &["/status".to_owned()], context(), &store)
                .unwrap()["/status"],
            br#""closed""#
        );
        let bundle = adapter
            .eventual_projection_bundle(&base, &["/status".to_owned()], context(), &store)
            .unwrap();
        assert_eq!(
            adapter.checked_eventual_projection(&bundle, &base).unwrap()["/status"],
            br#""open""#
        );
        assert_eq!(
            adapter.checked_eventual_projection(&bundle, &edited),
            Err(ManifestError::Conflict(
                "eventual projection belongs to another manifest"
            ))
        );
        assert_eq!(
            adapter.validate_operation(b"not a JSON operation"),
            Err(ManifestError::Malformed)
        );
    }
}
