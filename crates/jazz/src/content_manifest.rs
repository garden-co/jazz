//! Adapter-neutral substrate for content stored outside an ordinary Jazz row.
//!
//! A manifest is one *atomic* application cell.  Its root names an immutable
//! content graph and its bounded tail contains the not-yet-consolidated typed
//! edits.  The concrete adapters (text, streams, files, and JSON) own the
//! interpretation of both byte strings; this module owns the common boundary.
//!
//! The trait method contracts are intentionally documented together here and
//! in `SPEC/19_content_manifests.md` while adapters are still being introduced.
#![allow(missing_docs)]

use std::collections::BTreeMap;

use thiserror::Error;

/// A domain-scoped, content-addressed immutable object id.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct ContentId(pub [u8; 32]);

/// Authorization/encryption namespace used when deriving immutable ids.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct ContentDomainId(pub uuid::Uuid);

/// The immutable object category being addressed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImmutableContentKind {
    Leaf,
    Node,
    Root,
}

impl ImmutableContentKind {
    fn tag(self) -> &'static [u8] {
        match self {
            Self::Leaf => b"leaf",
            Self::Node => b"node",
            Self::Root => b"root",
        }
    }
}

/// Derive an id from a canonical payload.  The kind, codec version, and
/// authorization domain are deliberately part of the preimage: equal content
/// in different domains must not become a cross-domain equality oracle.
pub fn content_id(
    domain: ContentDomainId,
    adapter_kind: &str,
    kind: ImmutableContentKind,
    canonical_payload: &[u8],
) -> ContentId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"jazz-content-id-v1\0");
    hasher.update(kind.tag());
    hasher.update(&[0]);
    hasher.update(domain.0.as_bytes());
    hasher.update(&(adapter_kind.len() as u64).to_le_bytes());
    hasher.update(adapter_kind.as_bytes());
    hasher.update(&(canonical_payload.len() as u64).to_le_bytes());
    hasher.update(canonical_payload);
    ContentId(*hasher.finalize().as_bytes())
}

/// Schema metadata for one embedded content-manifest column.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ContentManifestSchema {
    /// Stable adapter name, e.g. `text-v1`; ordinary typed columns do not
    /// repeat this discriminant in every stored value.
    pub adapter_kind: String,
    /// Maximum number of encoded operations held in the un-consolidated tail.
    pub max_tail_entries: u32,
    /// Maximum aggregate tail bytes.  Adapters may impose a lower limit.
    pub max_tail_bytes: u32,
}

impl ContentManifestSchema {
    /// Construct a bounded manifest declaration.
    pub fn new(
        adapter_kind: impl Into<String>,
        max_tail_entries: u32,
        max_tail_bytes: u32,
    ) -> Result<Self, ManifestError> {
        let adapter_kind = adapter_kind.into();
        if adapter_kind.is_empty() || max_tail_entries == 0 || max_tail_bytes == 0 {
            return Err(ManifestError::InvalidSchema);
        }
        Ok(Self {
            adapter_kind,
            max_tail_entries,
            max_tail_bytes,
        })
    }
}

/// The atomic cell stored in an application row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContentManifest {
    pub root: ContentId,
    pub edit_tail: Vec<Vec<u8>>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ManifestError {
    #[error("content manifest schema must use nonempty adapter kind and nonzero bounds")]
    InvalidSchema,
    #[error("manifest bytes are truncated or malformed")]
    Malformed,
    #[error("manifest tail has {actual} entries, maximum is {maximum}")]
    TooManyTailEntries { actual: usize, maximum: u32 },
    #[error("manifest tail uses {actual} bytes, maximum is {maximum}")]
    TailTooLarge { actual: usize, maximum: u32 },
    #[error("content object {0:?} exists with different canonical bytes")]
    IdCollision(ContentId),
    #[error("content candidates cannot be merged: {0}")]
    Conflict(&'static str),
}

impl ContentManifest {
    /// Validate the adapter-independent tail bounds.
    pub fn validate(&self, schema: &ContentManifestSchema) -> Result<(), ManifestError> {
        if self.edit_tail.len() > schema.max_tail_entries as usize {
            return Err(ManifestError::TooManyTailEntries {
                actual: self.edit_tail.len(),
                maximum: schema.max_tail_entries,
            });
        }
        let bytes = self.edit_tail.iter().map(Vec::len).sum();
        if bytes > schema.max_tail_bytes as usize {
            return Err(ManifestError::TailTooLarge {
                actual: bytes,
                maximum: schema.max_tail_bytes,
            });
        }
        Ok(())
    }

    /// Canonical record-shaped encoding for the one atomic Jazz cell.
    pub fn encode(&self, schema: &ContentManifestSchema) -> Result<Vec<u8>, ManifestError> {
        self.validate(schema)?;
        let mut out = Vec::with_capacity(40 + self.edit_tail.iter().map(Vec::len).sum::<usize>());
        out.extend_from_slice(b"JCM1");
        out.extend_from_slice(&self.root.0);
        out.extend_from_slice(&(self.edit_tail.len() as u32).to_le_bytes());
        for operation in &self.edit_tail {
            out.extend_from_slice(&(operation.len() as u32).to_le_bytes());
            out.extend_from_slice(operation);
        }
        Ok(out)
    }

    /// Decode and validate a canonical manifest cell.
    pub fn decode(bytes: &[u8], schema: &ContentManifestSchema) -> Result<Self, ManifestError> {
        if bytes.len() < 40 || &bytes[..4] != b"JCM1" {
            return Err(ManifestError::Malformed);
        }
        let mut root = [0; 32];
        root.copy_from_slice(&bytes[4..36]);
        let count = u32::from_le_bytes(
            bytes[36..40]
                .try_into()
                .map_err(|_| ManifestError::Malformed)?,
        ) as usize;
        if count > schema.max_tail_entries as usize {
            return Err(ManifestError::TooManyTailEntries {
                actual: count,
                maximum: schema.max_tail_entries,
            });
        }
        let mut cursor = 40;
        let mut edit_tail = Vec::with_capacity(count);
        for _ in 0..count {
            let length = bytes
                .get(cursor..cursor + 4)
                .ok_or(ManifestError::Malformed)?;
            let length =
                u32::from_le_bytes(length.try_into().map_err(|_| ManifestError::Malformed)?)
                    as usize;
            cursor += 4;
            let operation = bytes
                .get(cursor..cursor + length)
                .ok_or(ManifestError::Malformed)?;
            edit_tail.push(operation.to_vec());
            cursor += length;
        }
        if cursor != bytes.len() {
            return Err(ManifestError::Malformed);
        }
        let manifest = Self {
            root: ContentId(root),
            edit_tail,
        };
        manifest.validate(schema)?;
        Ok(manifest)
    }
}

/// What an adapter is asked to materialize.  A partial request is advisory;
/// an adapter may load the whole immutable value when its representation needs it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MaterializationRequest {
    Full,
    Range { offset: u64, length: u64 },
    Projection(Vec<String>),
}

/// Domain context required to read an immutable object; an id alone is not a capability.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ContentReadContext {
    pub domain: ContentDomainId,
}

/// Ingredients from which a store derives an immutable address.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ContentAddress<'a> {
    pub domain: ContentDomainId,
    pub adapter_kind: &'a str,
    pub kind: ImmutableContentKind,
}

/// Immutable content lookup, deliberately separate from ordinary row reads.
pub trait ImmutableContentStore {
    fn get(&self, context: ContentReadContext, id: ContentId) -> Option<&[u8]>;
    /// Insert is idempotent only when the canonical bytes are identical.
    fn put_if_absent_or_identical(
        &mut self,
        address: ContentAddress<'_>,
        canonical_bytes: Vec<u8>,
    ) -> Result<ContentId, ManifestError>;
}

/// Adapter seam used by merge strategies and interior query/index lowering.
/// Both consumers receive the complete `{ root, editTail }`, never one field
/// independently, preserving atomic conflict-unit semantics.
pub trait ContentManifestAdapter {
    fn adapter_kind(&self) -> &str;
    fn validate_operation(&self, operation: &[u8]) -> Result<(), ManifestError>;
    fn materialize(
        &self,
        manifest: &ContentManifest,
        request: &MaterializationRequest,
        context: ContentReadContext,
        store: &dyn ImmutableContentStore,
    ) -> Result<Vec<u8>, ManifestError>;
    fn merge(
        &self,
        manifests: &[ContentManifest],
        context: ContentReadContext,
        store: &dyn ImmutableContentStore,
    ) -> Result<ContentManifest, ManifestError>;
    fn index_values(
        &self,
        manifest: &ContentManifest,
        requested: &[String],
        context: ContentReadContext,
        store: &dyn ImmutableContentStore,
    ) -> Result<BTreeMap<String, Vec<u8>>, ManifestError>;
}

/// Small in-memory fixture for adapters and tests. Production immutable stores
/// must provide the same if-absent-or-identical contract.
#[derive(Default)]
pub struct MemoryImmutableContentStore(BTreeMap<(ContentDomainId, ContentId), Vec<u8>>);
impl ImmutableContentStore for MemoryImmutableContentStore {
    fn get(&self, context: ContentReadContext, id: ContentId) -> Option<&[u8]> {
        self.0.get(&(context.domain, id)).map(Vec::as_slice)
    }
    fn put_if_absent_or_identical(
        &mut self,
        address: ContentAddress<'_>,
        bytes: Vec<u8>,
    ) -> Result<ContentId, ManifestError> {
        let id = content_id(address.domain, address.adapter_kind, address.kind, &bytes);
        match self.0.get(&(address.domain, id)) {
            Some(existing) if existing != &bytes => Err(ManifestError::IdCollision(id)),
            Some(_) => Ok(id),
            None => {
                self.0.insert((address.domain, id), bytes);
                Ok(id)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn manifest_codec_is_atomic_and_enforces_its_boundaries() {
        // Internal because this is a wire codec boundary with no public client API yet.
        let schema = ContentManifestSchema::new("fixture-v1", 2, 3).unwrap();
        let value = ContentManifest {
            root: ContentId([7; 32]),
            edit_tail: vec![b"a".to_vec(), b"bc".to_vec()],
        };
        assert_eq!(
            ContentManifest::decode(&value.encode(&schema).unwrap(), &schema).unwrap(),
            value
        );
        assert_eq!(
            value.encode(&ContentManifestSchema::new("fixture-v1", 1, 3).unwrap()),
            Err(ManifestError::TooManyTailEntries {
                actual: 2,
                maximum: 1
            })
        );
        assert!(ContentManifest::decode(b"JCM1", &schema).is_err());
    }
    #[test]
    fn ids_are_domain_and_kind_scoped_and_put_is_identical_only() {
        let domain = ContentDomainId(uuid::Uuid::from_bytes([1; 16]));
        let leaf = content_id(domain, "fixture-v1", ImmutableContentKind::Leaf, b"same");
        assert_ne!(
            leaf,
            content_id(domain, "fixture-v1", ImmutableContentKind::Node, b"same")
        );
        // Exact historical collision pair. Without explicit adapter/payload
        // length boundaries, these two inputs share the same old preimage;
        // the current length-prefixed encoding keeps their identities apart.
        let historical_left = content_id(domain, "a", ImmutableContentKind::Leaf, &[0; 9]);
        let historical_right = content_id(
            domain,
            "a\0\x09\0\0\0\0\0\0\0",
            ImmutableContentKind::Leaf,
            b"",
        );
        assert_ne!(historical_left, historical_right);
        assert_eq!(
            ContentManifestSchema::new("", 1, 1),
            Err(ManifestError::InvalidSchema)
        );
        assert_ne!(
            leaf,
            content_id(
                ContentDomainId(uuid::Uuid::from_bytes([2; 16])),
                "fixture-v1",
                ImmutableContentKind::Leaf,
                b"same"
            )
        );
        let mut store = MemoryImmutableContentStore::default();
        let address = ContentAddress {
            domain,
            adapter_kind: "fixture-v1",
            kind: ImmutableContentKind::Leaf,
        };
        store
            .put_if_absent_or_identical(address, b"same".to_vec())
            .unwrap();
        store
            .put_if_absent_or_identical(address, b"same".to_vec())
            .unwrap();
        let different = store
            .put_if_absent_or_identical(address, b"different".to_vec())
            .unwrap();
        assert_ne!(different, leaf, "the store derives, never trusts, the id");
    }
}
