//! JSON adapter for the embedded ordinary-content manifest substrate.
//!
//! This module owns JSON's stable logical-node identities and its immutable
//! physical tree.  The owning application row owns the mutable manifest; there
//! is intentionally no JSON-specific mutable head row.
#![allow(missing_docs)]

use std::collections::{BTreeMap, BTreeSet};

use groove::records::{Value, ValueType};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::content_manifest::{
    ContentAddress, ContentId, ContentManifest, ContentManifestAdapter, ContentManifestSchema,
    ContentReadContext, ImmutableContentKind, ImmutableContentStore, ManifestError,
    MaterializationRequest, content_id,
};

/// Schema adapter discriminator for JSON manifests.
pub const JSON_ADAPTER_KIND: &str = "json-v1";
const ORDER_FANOUT: usize = 32;
const MAX_MERGE_CANDIDATES: usize = 32;
const MAX_MERGE_OPERATIONS: usize = 256;

fn value_bytes(value: &Value) -> Result<&[u8], ManifestError> {
    match value {
        Value::Bytes(bytes) => Ok(bytes),
        _ => Err(ManifestError::Malformed),
    }
}

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
    /// Encode this operation as the JSON manifest schema's typed tail value.
    pub fn encode(&self) -> Result<Value, ManifestError> {
        Ok(Value::Bytes(self.encode_bytes()?))
    }
    fn encode_bytes(&self) -> Result<Vec<u8>, ManifestError> {
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
            let bytes = value_bytes(operation).unwrap_or_default();
            hash.update(&(bytes.len() as u64).to_le_bytes());
            hash.update(bytes);
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
        for operation in Self::decoded_tail(manifest)? {
            self.apply(&mut value, &operation)?;
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
        for operation in Self::decoded_tail(manifest)? {
            self.apply(&mut root, &operation)?;
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
        let bytes = store.get(context, id).ok_or(ManifestError::Malformed)?;
        let object: StoredObject =
            postcard::from_bytes(bytes).map_err(|_| ManifestError::Malformed)?;
        let kind = match &object {
            StoredObject::Root { .. } => ImmutableContentKind::Root,
            StoredObject::Node(_) | StoredObject::Order(OrderNode::Branch(_)) => {
                ImmutableContentKind::Node
            }
            StoredObject::Order(OrderNode::Leaf(_)) => ImmutableContentKind::Leaf,
        };
        if content_id(context.domain, JSON_ADAPTER_KIND, kind, bytes) != id {
            return Err(ManifestError::Conflict(
                "immutable JSON object content id mismatch",
            ));
        }
        Ok(object)
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
                .map(|(id, expected)| {
                    let values = self.load_order(id, context, store)?;
                    if values.len() != expected as usize {
                        return Err(ManifestError::Conflict(
                            "JSON order-tree rank count mismatch",
                        ));
                    }
                    Ok(values)
                })
                .collect::<Result<Vec<_>, _>>()
                .map(|parts| parts.into_iter().flatten().collect()),
            _ => Err(ManifestError::Malformed),
        }
    }
    fn child_id(parent: Uuid, label: &[u8]) -> Uuid {
        Uuid::new_v5(&parent, label)
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
                            let mut label = b"object:".to_vec();
                            label.extend_from_slice(key.as_bytes());
                            (
                                key.clone(),
                                Self::literal_node(Self::child_id(id, &label), value),
                            )
                        })
                        .collect(),
                ),
                JsonLiteral::Array(values) => MaterializedValue::Array(
                    values
                        .iter()
                        .enumerate()
                        .map(|(index, value)| {
                            Self::literal_node(
                                Self::child_id(id, &(index as u64).to_le_bytes()),
                                value,
                            )
                        })
                        .collect(),
                ),
            },
        }
    }
    fn apply(&self, node: &mut Materialized, op: &JsonOperation) -> Result<(), ManifestError> {
        let inserted = match op {
            JsonOperation::SetMember { op, value, .. } => Some(Self::literal_node(*op, value)),
            JsonOperation::InsertArray { element, value, .. } => {
                Some(Self::literal_node(*element, value))
            }
            _ => None,
        };
        if let Some(inserted) = &inserted {
            let mut live = BTreeSet::new();
            let mut new = BTreeSet::new();
            Self::collect_ids(node, &mut live);
            Self::collect_ids(inserted, &mut new);
            if new.iter().any(|id| live.contains(id)) {
                return Err(ManifestError::Conflict(
                    "inserted JSON literal logical id already exists",
                ));
            }
        }
        match op {
            JsonOperation::SetScalar { target, value, .. } => Self::find_mut(node, *target)
                .filter(|node| matches!(node.value, MaterializedValue::Scalar(_)))
                .map(|node| node.value = MaterializedValue::Scalar(value.clone()))
                .ok_or(ManifestError::Conflict(
                    "scalar target is absent or non-scalar",
                )),
            JsonOperation::SetMember { object, key, .. } => Self::find_mut(node, *object)
                .and_then(|node| match &mut node.value {
                    MaterializedValue::Object(members) => Some(members),
                    _ => None,
                })
                .map(|members| {
                    members.insert(key.clone(), inserted.clone().expect("SetMember inserts"));
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
                anchor,
                after,
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
                children.insert(at, inserted.expect("InsertArray inserts"));
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
    fn find(node: &Materialized, id: Uuid) -> Option<&Materialized> {
        if node.id == id {
            return Some(node);
        }
        match &node.value {
            MaterializedValue::Object(members) => {
                members.values().find_map(|child| Self::find(child, id))
            }
            MaterializedValue::Array(children) => {
                children.iter().find_map(|child| Self::find(child, id))
            }
            MaterializedValue::Scalar(_) => None,
        }
    }
    fn collect_ids(node: &Materialized, ids: &mut BTreeSet<Uuid>) {
        ids.insert(node.id);
        match &node.value {
            MaterializedValue::Object(members) => {
                for child in members.values() {
                    Self::collect_ids(child, ids);
                }
            }
            MaterializedValue::Array(children) => {
                for child in children {
                    Self::collect_ids(child, ids);
                }
            }
            MaterializedValue::Scalar(_) => {}
        }
    }
    fn decoded_tail(manifest: &ContentManifest) -> Result<Vec<JsonOperation>, ManifestError> {
        let mut ids = BTreeSet::new();
        manifest
            .edit_tail
            .iter()
            .map(|value| {
                let operation = JsonOperation::decode(value_bytes(value)?)?;
                if !ids.insert(operation.id()) {
                    return Err(ManifestError::Conflict(
                        "duplicate operation id within one tail",
                    ));
                }
                Ok(operation)
            })
            .collect()
    }
    fn footprint(
        root: &Materialized,
        operation: &JsonOperation,
    ) -> Result<(BTreeSet<Uuid>, BTreeSet<Uuid>), ManifestError> {
        let mut requires = BTreeSet::new();
        let mut removes = BTreeSet::new();
        match operation {
            JsonOperation::SetScalar { target, .. } => {
                requires.insert(*target);
            }
            JsonOperation::SetMember { object, key, .. }
            | JsonOperation::RemoveMember { object, key, .. } => {
                requires.insert(*object);
                if let Some(Materialized {
                    value: MaterializedValue::Object(members),
                    ..
                }) = Self::find(root, *object)
                {
                    if let Some(previous) = members.get(key) {
                        Self::collect_ids(previous, &mut removes);
                    }
                }
            }
            JsonOperation::InsertArray { array, anchor, .. } => {
                requires.insert(*array);
                requires.extend(anchor);
            }
            JsonOperation::Delete { target, .. } => {
                let deleted = Self::find(root, *target)
                    .ok_or(ManifestError::Conflict("delete target is absent"))?;
                Self::collect_ids(deleted, &mut removes);
            }
        }
        Ok((requires, removes))
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
    fn validate_schema(&self, schema: &ContentManifestSchema) -> Result<(), ManifestError> {
        if schema.adapter_kind != JSON_ADAPTER_KIND || schema.tail_entry_type != ValueType::Bytes {
            return Err(ManifestError::InvalidSchema);
        }
        Ok(())
    }

    fn validate_operation(&self, value: &Value) -> Result<(), ManifestError> {
        JsonOperation::decode(value_bytes(value)?).map(|_| ())
    }
    fn materialize(
        &self,
        manifest: &ContentManifest,
        request: &MaterializationRequest,
        context: ContentReadContext,
        store: &dyn ImmutableContentStore,
    ) -> Result<Vec<u8>, ManifestError> {
        let mut node = self.load_root(manifest.root, context, store)?;
        for operation in Self::decoded_tail(manifest)? {
            self.apply(&mut node, &operation)?;
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
        context: ContentReadContext,
        store: &dyn ImmutableContentStore,
    ) -> Result<ContentManifest, ManifestError> {
        let Some(first) = manifests.first() else {
            return Err(ManifestError::Conflict("no candidates"));
        };
        if manifests.iter().any(|manifest| manifest.root != first.root) {
            return Err(ManifestError::Conflict("unproven root descendant"));
        }
        if manifests.len() > MAX_MERGE_CANDIDATES
            || manifests
                .iter()
                .map(|manifest| manifest.edit_tail.len())
                .sum::<usize>()
                > MAX_MERGE_OPERATIONS
        {
            return Err(ManifestError::Conflict("JSON merge work budget exceeded"));
        }
        let root = self.load_root(first.root, context, store)?;
        let mut decoded = Vec::with_capacity(manifests.len());
        let mut summaries = Vec::with_capacity(manifests.len());
        for manifest in manifests {
            let operations = Self::decoded_tail(manifest)?;
            let mut candidate = root.clone();
            let mut required = BTreeSet::new();
            let mut removed = BTreeSet::new();
            for operation in &operations {
                let (step_required, step_removed) = Self::footprint(&candidate, operation)?;
                required.extend(step_required);
                removed.extend(step_removed);
                self.apply(&mut candidate, operation)?;
            }
            summaries.push((required, removed));
            decoded.push(operations);
        }
        if manifests.len() == 1 || manifests.iter().all(|manifest| manifest == first) {
            return Ok(first.clone());
        }
        let mut operations: BTreeMap<Uuid, Vec<u8>> = BTreeMap::new();
        let mut keys: BTreeMap<String, (usize, Uuid, Vec<u8>)> = BTreeMap::new();
        for (candidate_index, manifest) in manifests.iter().enumerate() {
            for value in &manifest.edit_tail {
                let bytes = value_bytes(value)?;
                let op = JsonOperation::decode(bytes)?;
                if let Some(existing) = operations.get(&op.id()) {
                    if existing != bytes {
                        return Err(ManifestError::Conflict(
                            "duplicate operation id equivocation",
                        ));
                    }
                    continue;
                }
                let key = op.conflict_key();
                if let Some((owner, existing_id, existing_bytes)) = keys.get(&key) {
                    if *owner != candidate_index
                        && (*existing_id != op.id() || existing_bytes != bytes)
                    {
                        return Err(ManifestError::Conflict("noncommuting typed operations"));
                    }
                } else {
                    keys.insert(key, (candidate_index, op.id(), bytes.to_vec()));
                }
                operations.insert(op.id(), bytes.to_vec());
            }
        }
        let mut required_by_prior = BTreeSet::new();
        let mut removed_by_prior = BTreeSet::new();
        for (required, removed) in summaries {
            if required.iter().any(|id| removed_by_prior.contains(id))
                || removed.iter().any(|id| required_by_prior.contains(id))
            {
                return Err(ManifestError::Conflict(
                    "typed operation tails have ancestor or removal dependencies",
                ));
            }
            required_by_prior.extend(required);
            removed_by_prior.extend(removed);
        }
        let mut tails = manifests
            .iter()
            .map(|manifest| {
                manifest
                    .edit_tail
                    .iter()
                    .map(|value| value_bytes(value).map(ToOwned::to_owned))
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?;
        tails.sort();
        let mut emitted = BTreeSet::new();
        let edit_tail = tails
            .into_iter()
            .flatten()
            .filter(|bytes| {
                JsonOperation::decode(bytes).is_ok_and(|operation| emitted.insert(operation.id()))
            })
            .map(Value::Bytes)
            .collect();
        let merged = ContentManifest {
            root: first.root,
            edit_tail,
        };
        // Validate the exact deterministic tail being returned, not only each
        // authored candidate in isolation. Cross-candidate created IDs and
        // dependencies become visible only in this combined sequence.
        let mut combined = root;
        for operation in Self::decoded_tail(&merged)? {
            self.apply(&mut combined, &operation)?;
        }
        Ok(merged)
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
            adapter.validate_operation(&Value::Bytes(b"not a JSON operation".to_vec())),
            Err(ManifestError::Malformed)
        );
    }

    #[test]
    fn array_over_fanout_is_a_ranked_immutable_order_tree() {
        let adapter = OrdinaryJsonAdapter;
        let mut store = MemoryImmutableContentStore::default();
        let root = adapter
            .publish_literal(
                &JsonLiteral::Array(
                    (0..=ORDER_FANOUT)
                        .map(|value| JsonLiteral::Scalar(JsonScalar::Number(value as i64)))
                        .collect(),
                ),
                None,
                context(),
                &mut store,
            )
            .unwrap();
        let StoredObject::Root { document, .. } = adapter.get(root, context(), &store).unwrap()
        else {
            unreachable!()
        };
        let StoredObject::Node(StoredNode {
            kind: StoredKind::Array(order),
            ..
        }) = adapter.get(document, context(), &store).unwrap()
        else {
            unreachable!()
        };
        assert!(
            matches!(adapter.get(order, context(), &store).unwrap(), StoredObject::Order(OrderNode::Branch(children)) if children.iter().map(|(_, count)| *count).sum::<u32>() == 33)
        );
        assert_eq!(
            adapter
                .materialize(
                    &ContentManifest {
                        root,
                        edit_tail: Vec::new()
                    },
                    &MaterializationRequest::Full,
                    context(),
                    &store
                )
                .unwrap(),
            serde_json::to_vec(&(0..=ORDER_FANOUT).collect::<Vec<_>>()).unwrap()
        );
    }

    #[test]
    fn consolidation_is_deterministic_for_nested_inserted_literals() {
        let (adapter, mut store, base, node) = seeded();
        let object = node.id;
        let manifest = ContentManifest {
            root: base.root,
            edit_tail: vec![
                JsonOperation::SetMember {
                    op: Uuid::from_bytes([0x71; 16]),
                    object,
                    key: "nested".into(),
                    value: JsonLiteral::Object(BTreeMap::from([(
                        "array".into(),
                        JsonLiteral::Array(vec![JsonLiteral::Scalar(JsonScalar::Number(7))]),
                    )])),
                }
                .encode()
                .unwrap(),
            ],
        };
        let first = adapter
            .consolidate(&manifest, context(), &mut store)
            .unwrap();
        let second = adapter
            .consolidate(&manifest, context(), &mut store)
            .unwrap();
        assert_eq!(first.root, second.root);
    }

    #[test]
    fn merge_rejects_duplicate_operation_equivocation_and_introduced_anchor_dependency() {
        let (adapter, store, base, node) = seeded();
        let status = match &node.value {
            MaterializedValue::Object(values) => values["status"].id,
            _ => unreachable!(),
        };
        let op = Uuid::from_bytes([0x72; 16]);
        let candidate = |value: &str| ContentManifest {
            root: base.root,
            edit_tail: vec![
                JsonOperation::SetScalar {
                    op,
                    target: status,
                    value: JsonScalar::String(value.into()),
                }
                .encode()
                .unwrap(),
            ],
        };
        assert_eq!(
            adapter.merge(&[candidate("a"), candidate("b")], context(), &store),
            Err(ManifestError::Conflict(
                "duplicate operation id equivocation"
            ))
        );

        let array = match &node.value {
            MaterializedValue::Object(values) => values["items"].id,
            _ => unreachable!(),
        };
        let introduced = Uuid::from_bytes([0x73; 16]);
        let insert = JsonOperation::InsertArray {
            op: Uuid::from_bytes([0x74; 16]),
            array,
            element: introduced,
            anchor: None,
            after: true,
            value: JsonLiteral::Scalar(JsonScalar::Number(2)),
        };
        let dependent = JsonOperation::InsertArray {
            op: Uuid::from_bytes([0x75; 16]),
            array,
            element: Uuid::from_bytes([0x76; 16]),
            anchor: Some(introduced),
            after: true,
            value: JsonLiteral::Scalar(JsonScalar::Number(3)),
        };
        assert!(
            adapter
                .merge(
                    &[
                        ContentManifest {
                            root: base.root,
                            edit_tail: vec![insert.encode().unwrap()]
                        },
                        ContentManifest {
                            root: base.root,
                            edit_tail: vec![dependent.encode().unwrap()]
                        },
                    ],
                    context(),
                    &store,
                )
                .is_err()
        );
        let duplicate = ContentManifest {
            root: base.root,
            edit_tail: vec![insert.encode().unwrap(), insert.encode().unwrap()],
        };
        assert_eq!(
            adapter.materialize(&duplicate, &MaterializationRequest::Full, context(), &store),
            Err(ManifestError::Conflict(
                "duplicate operation id within one tail"
            ))
        );
        let colliding = ContentManifest {
            root: base.root,
            edit_tail: vec![
                JsonOperation::InsertArray {
                    op: Uuid::from_bytes([0x7a; 16]),
                    array,
                    element: status,
                    anchor: None,
                    after: true,
                    value: JsonLiteral::Scalar(JsonScalar::Number(4)),
                }
                .encode()
                .unwrap(),
            ],
        };
        assert_eq!(
            adapter.materialize(&colliding, &MaterializationRequest::Full, context(), &store),
            Err(ManifestError::Conflict(
                "inserted JSON literal logical id already exists"
            ))
        );
    }

    #[test]
    fn distinct_keys_conflict_when_one_removes_the_others_target() {
        let (adapter, store, base, node) = seeded();
        let (object, status) = match &node.value {
            MaterializedValue::Object(values) => (node.id, values["status"].id),
            _ => unreachable!(),
        };
        let replace_member = ContentManifest {
            root: base.root,
            edit_tail: vec![
                JsonOperation::SetMember {
                    op: Uuid::from_bytes([0x7b; 16]),
                    object,
                    key: "status".into(),
                    value: JsonLiteral::Scalar(JsonScalar::String("replacement".into())),
                }
                .encode()
                .unwrap(),
            ],
        };
        let edit_old_child = ContentManifest {
            root: base.root,
            edit_tail: vec![
                JsonOperation::SetScalar {
                    op: Uuid::from_bytes([0x7c; 16]),
                    target: status,
                    value: JsonScalar::String("edited".into()),
                }
                .encode()
                .unwrap(),
            ],
        };
        // Planted positive: removing the required-vs-removed footprint guard
        // makes these distinct conflict keys merge and silently lose one edit.
        assert_eq!(
            adapter.merge(&[replace_member, edit_old_child], context(), &store),
            Err(ManifestError::Conflict(
                "typed operation tails have ancestor or removal dependencies"
            ))
        );
    }

    #[test]
    fn nested_array_child_label_has_fixed_width_golden_identity() {
        let parent = Uuid::from_bytes([0x44; 16]);
        assert_eq!(
            OrdinaryJsonAdapter::child_id(parent, &0u64.to_le_bytes()).to_string(),
            "7504e01a-542a-5a18-a3ff-e671a11a74e1"
        );
    }

    #[test]
    fn sequential_same_path_ops_are_one_authored_candidate_not_a_conflict() {
        let (adapter, store, base, node) = seeded();
        let status = match &node.value {
            MaterializedValue::Object(values) => values["status"].id,
            _ => unreachable!(),
        };
        let candidate = ContentManifest {
            root: base.root,
            edit_tail: ["first", "second"]
                .into_iter()
                .enumerate()
                .map(|(index, value)| {
                    JsonOperation::SetScalar {
                        op: Uuid::from_u128(0x8000 + index as u128),
                        target: status,
                        value: JsonScalar::String(value.into()),
                    }
                    .encode()
                    .unwrap()
                })
                .collect(),
        };
        assert_eq!(
            adapter.merge(std::slice::from_ref(&candidate), context(), &store),
            Ok(candidate.clone())
        );
        assert_eq!(
            adapter.merge(&[candidate.clone(), candidate.clone()], context(), &store),
            Ok(candidate.clone())
        );
        assert_eq!(
            adapter
                .materialize(&candidate, &MaterializationRequest::Full, context(), &store)
                .unwrap(),
            br#"{"items":[1],"status":"second"}"#
        );
    }

    #[test]
    fn cross_candidate_created_ids_must_be_distinct_even_at_different_anchors() {
        let (adapter, store, base, node) = seeded();
        let (array, anchor) = match &node.value {
            MaterializedValue::Object(values) => match &values["items"].value {
                MaterializedValue::Array(items) => (values["items"].id, items[0].id),
                _ => unreachable!(),
            },
            _ => unreachable!(),
        };
        let candidate = |op, element, anchor, after| ContentManifest {
            root: base.root,
            edit_tail: vec![
                JsonOperation::InsertArray {
                    op,
                    array,
                    element,
                    anchor,
                    after,
                    value: JsonLiteral::Scalar(JsonScalar::Number(2)),
                }
                .encode()
                .unwrap(),
            ],
        };
        let shared = Uuid::from_bytes([0x83; 16]);
        let front = candidate(Uuid::from_bytes([0x84; 16]), shared, None, false);
        let after = candidate(Uuid::from_bytes([0x85; 16]), shared, Some(anchor), true);
        assert_eq!(
            adapter.merge(&[front, after], context(), &store),
            Err(ManifestError::Conflict(
                "inserted JSON literal logical id already exists"
            ))
        );

        let distinct = adapter
            .merge(
                &[
                    candidate(
                        Uuid::from_bytes([0x86; 16]),
                        Uuid::from_bytes([0x87; 16]),
                        None,
                        false,
                    ),
                    candidate(
                        Uuid::from_bytes([0x88; 16]),
                        Uuid::from_bytes([0x89; 16]),
                        Some(anchor),
                        true,
                    ),
                ],
                context(),
                &store,
            )
            .unwrap();
        assert!(
            adapter
                .materialize(&distinct, &MaterializationRequest::Full, context(), &store)
                .is_ok()
        );
    }

    #[test]
    fn set_member_rejects_collision_with_a_recursively_derived_inserted_id() {
        let adapter = OrdinaryJsonAdapter;
        let mut store = MemoryImmutableContentStore::default();
        let op = Uuid::from_bytes([0x81; 16]);
        let derived = OrdinaryJsonAdapter::child_id(op, b"object:x");
        let root_node = Materialized {
            id: Uuid::from_bytes([0x82; 16]),
            value: MaterializedValue::Object(BTreeMap::from([(
                "existing".into(),
                Materialized {
                    id: derived,
                    value: MaterializedValue::Scalar(JsonScalar::Number(1)),
                },
            )])),
        };
        let document = adapter
            .publish_node(&root_node, context(), &mut store)
            .unwrap();
        let root = adapter
            .put(
                ImmutableContentKind::Root,
                &StoredObject::Root {
                    document,
                    parent: None,
                },
                context(),
                &mut store,
            )
            .unwrap();
        let manifest = ContentManifest {
            root,
            edit_tail: vec![
                JsonOperation::SetMember {
                    op,
                    object: root_node.id,
                    key: "new".into(),
                    value: JsonLiteral::Object(BTreeMap::from([(
                        "x".into(),
                        JsonLiteral::Scalar(JsonScalar::Number(2)),
                    )])),
                }
                .encode()
                .unwrap(),
            ],
        };
        assert_eq!(
            adapter.materialize(&manifest, &MaterializationRequest::Full, context(), &store),
            Err(ManifestError::Conflict(
                "inserted JSON literal logical id already exists"
            ))
        );
    }

    #[test]
    fn fetched_ids_and_order_rank_counts_are_verified() {
        let adapter = OrdinaryJsonAdapter;
        let mut store = MemoryImmutableContentStore::default();
        let child = adapter
            .publish_node(
                &Materialized {
                    id: Uuid::from_bytes([0x77; 16]),
                    value: MaterializedValue::Scalar(JsonScalar::Number(1)),
                },
                context(),
                &mut store,
            )
            .unwrap();
        let leaf = adapter
            .put(
                ImmutableContentKind::Leaf,
                &StoredObject::Order(OrderNode::Leaf(vec![child])),
                context(),
                &mut store,
            )
            .unwrap();
        let bad_order = adapter
            .put(
                ImmutableContentKind::Node,
                &StoredObject::Order(OrderNode::Branch(vec![(leaf, 2)])),
                context(),
                &mut store,
            )
            .unwrap();
        let document = adapter
            .put(
                ImmutableContentKind::Node,
                &StoredObject::Node(StoredNode {
                    logical_id: Uuid::from_bytes([0x78; 16]),
                    kind: StoredKind::Array(bad_order),
                }),
                context(),
                &mut store,
            )
            .unwrap();
        let root = adapter
            .put(
                ImmutableContentKind::Root,
                &StoredObject::Root {
                    document,
                    parent: None,
                },
                context(),
                &mut store,
            )
            .unwrap();
        assert_eq!(
            adapter.materialize(
                &ContentManifest {
                    root,
                    edit_tail: Vec::new()
                },
                &MaterializationRequest::Full,
                context(),
                &store
            ),
            Err(ManifestError::Conflict(
                "JSON order-tree rank count mismatch"
            ))
        );
        assert_eq!(
            adapter.materialize(
                &ContentManifest {
                    root: ContentId([0x79; 32]),
                    edit_tail: Vec::new()
                },
                &MaterializationRequest::Full,
                context(),
                &store
            ),
            Err(ManifestError::Malformed)
        );
    }

    #[test]
    fn fetched_object_bytes_must_rehash_to_the_requested_id() {
        struct CorruptingStore {
            inner: MemoryImmutableContentStore,
            target: ContentId,
            replacement: Vec<u8>,
        }
        impl ImmutableContentStore for CorruptingStore {
            fn get(&self, context: ContentReadContext, id: ContentId) -> Option<&[u8]> {
                if id == self.target {
                    Some(&self.replacement)
                } else {
                    self.inner.get(context, id)
                }
            }
            fn put_if_absent_or_identical(
                &mut self,
                address: ContentAddress<'_>,
                bytes: Vec<u8>,
            ) -> Result<ContentId, ManifestError> {
                self.inner.put_if_absent_or_identical(address, bytes)
            }
        }
        let adapter = OrdinaryJsonAdapter;
        let mut store = MemoryImmutableContentStore::default();
        let target = adapter
            .publish_literal(
                &JsonLiteral::Scalar(JsonScalar::Number(1)),
                None,
                context(),
                &mut store,
            )
            .unwrap();
        let replacement_id = adapter
            .publish_literal(
                &JsonLiteral::Scalar(JsonScalar::Number(2)),
                None,
                context(),
                &mut store,
            )
            .unwrap();
        let replacement = store.get(context(), replacement_id).unwrap().to_vec();
        let store = CorruptingStore {
            inner: store,
            target,
            replacement,
        };
        assert_eq!(
            adapter.materialize(
                &ContentManifest {
                    root: target,
                    edit_tail: Vec::new()
                },
                &MaterializationRequest::Full,
                context(),
                &store
            ),
            Err(ManifestError::Conflict(
                "immutable JSON object content id mismatch"
            ))
        );
    }
}
