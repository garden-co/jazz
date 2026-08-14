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
use std::sync::{Arc, OnceLock, RwLock};

use groove::records::{OwnedRecord, RecordDescriptor, Value, ValueType};

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
    /// The schema-owned type of one edit-tail entry.  Different content
    /// variants deliberately choose different types; it is never an opaque
    /// byte convention shared by all adapters.
    pub tail_entry_type: ValueType,
    /// Maximum number of encoded operations held in the un-consolidated tail.
    pub max_tail_entries: u32,
    /// Maximum aggregate tail bytes.  Adapters may impose a lower limit.
    pub max_tail_bytes: u32,
}

impl ContentManifestSchema {
    /// Construct a bounded manifest whose typed tail entries are bytes. This
    /// is shorthand for byte-oriented content variants.
    pub fn new(
        adapter_kind: impl Into<String>,
        max_tail_entries: u32,
        max_tail_bytes: u32,
    ) -> Result<Self, ManifestError> {
        Self::with_tail_entry_type(
            adapter_kind,
            ValueType::Bytes,
            max_tail_entries,
            max_tail_bytes,
        )
    }

    /// Construct a bounded manifest declaration with the concrete content
    /// variant's typed tail entry.
    pub fn with_tail_entry_type(
        adapter_kind: impl Into<String>,
        tail_entry_type: ValueType,
        max_tail_entries: u32,
        max_tail_bytes: u32,
    ) -> Result<Self, ManifestError> {
        let adapter_kind = adapter_kind.into();
        if adapter_kind.is_empty() || max_tail_entries == 0 || max_tail_bytes == 0 {
            return Err(ManifestError::InvalidSchema);
        }
        Ok(Self {
            adapter_kind,
            tail_entry_type,
            max_tail_entries,
            max_tail_bytes,
        })
    }

    /// The physical type of this one atomic user cell.  `root` is a checked
    /// 32-byte content id and `editTail` is an array whose element type is
    /// fixed by the owning content variant's schema.
    pub fn cell_type(&self) -> ValueType {
        ValueType::Record(Box::new(self.cell_descriptor()))
    }

    /// Descriptor used by the nested `Record` cell value.
    pub fn cell_descriptor(&self) -> RecordDescriptor {
        RecordDescriptor::new([
            ("root", ValueType::Bytes),
            (
                "editTail",
                ValueType::Array(Box::new(self.tail_entry_type.clone())),
            ),
        ])
    }
}

/// The atomic cell stored in an application row.
#[derive(Clone, Debug, PartialEq)]
pub struct ContentManifest {
    pub root: ContentId,
    pub edit_tail: Vec<Value>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ManifestError {
    #[error("content manifest schema must use nonempty adapter kind and nonzero bounds")]
    InvalidSchema,
    #[error("manifest cell is malformed or has the wrong record shape")]
    Malformed,
    #[error("manifest tail has {actual} entries, maximum is {maximum}")]
    TooManyTailEntries { actual: usize, maximum: u32 },
    #[error("manifest tail uses {actual} bytes, maximum is {maximum}")]
    TailTooLarge { actual: usize, maximum: u32 },
    #[error("content object {0:?} exists with different canonical bytes")]
    IdCollision(ContentId),
    #[error("content candidates cannot be merged: {0}")]
    Conflict(&'static str),
    #[error("no content-manifest adapter is registered for kind {0:?}")]
    UnknownAdapter(String),
    #[error("a different content-manifest adapter is already registered for kind {0:?}")]
    AdapterAlreadyRegistered(String),
    #[error("content-manifest runtime received a non-record cell")]
    NonRecordCell,
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
        let bytes = self
            .edit_tail
            .iter()
            .map(|entry| encoded_tail_entry_len(&schema.tail_entry_type, entry))
            .try_fold(0usize, |total, length| length.map(|length| total + length))?;
        if bytes > schema.max_tail_bytes as usize {
            return Err(ManifestError::TailTooLarge {
                actual: bytes,
                maximum: schema.max_tail_bytes,
            });
        }
        Ok(())
    }

    /// Convert this manifest into the actual typed record cell stored in the
    /// owning application row.  The record codec checks nested descriptor
    /// identity and canonical encoding, while `validate` enforces the
    /// manifest-specific root and tail bounds.
    pub fn into_value(&self, schema: &ContentManifestSchema) -> Result<Value, ManifestError> {
        self.validate(schema)?;
        let descriptor = schema.cell_descriptor();
        let raw = descriptor
            .create(&[
                Value::Bytes(self.root.0.to_vec()),
                Value::Array(self.edit_tail.clone()),
            ])
            .map_err(|_| ManifestError::Malformed)?;
        Ok(Value::Record(OwnedRecord::new(raw, descriptor)))
    }

    /// Decode and validate an actual typed record cell.
    pub fn from_value(
        value: &Value,
        schema: &ContentManifestSchema,
    ) -> Result<Self, ManifestError> {
        let Value::Record(record) = value else {
            return Err(ManifestError::NonRecordCell);
        };
        let descriptor = schema.cell_descriptor();
        if record.descriptor() != &descriptor {
            return Err(ManifestError::Malformed);
        }
        let values = record.to_values().map_err(|_| ManifestError::Malformed)?;
        let [Value::Bytes(root), Value::Array(edit_tail)] = values.as_slice() else {
            return Err(ManifestError::Malformed);
        };
        let root: [u8; 32] = root
            .as_slice()
            .try_into()
            .map_err(|_| ManifestError::Malformed)?;
        let manifest = Self {
            root: ContentId(root),
            edit_tail: edit_tail.clone(),
        };
        manifest.validate(schema)?;
        Ok(manifest)
    }
}

fn encoded_tail_entry_len(value_type: &ValueType, value: &Value) -> Result<usize, ManifestError> {
    let descriptor = RecordDescriptor::new([("entry", value_type.clone())]);
    descriptor
        .create(std::slice::from_ref(value))
        .map(|raw| raw.len())
        .map_err(|_| ManifestError::Malformed)
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

/// Production-owned source of the authorization domain and immutable object
/// store used by a node. Core never substitutes a test memory store here:
/// embedders install the service that owns their durable/encrypted objects.
pub trait ContentManifestRuntimeProvider: Send + Sync + 'static {
    fn read_context(&self, node: crate::ids::NodeUuid) -> ContentReadContext;
    fn immutable_store(&self) -> &dyn ImmutableContentStore;
}

struct UnavailableImmutableContentStore;
impl ImmutableContentStore for UnavailableImmutableContentStore {
    fn get(&self, _: ContentReadContext, _: ContentId) -> Option<&[u8]> {
        None
    }
    fn put_if_absent_or_identical(
        &mut self,
        _: ContentAddress<'_>,
        _: Vec<u8>,
    ) -> Result<ContentId, ManifestError> {
        Err(ManifestError::Conflict(
            "no immutable content store configured",
        ))
    }
}

/// Default service permits schema/row admission but fails any operation that
/// needs immutable bytes. Applications with content columns install a provider
/// through `NodeState::new_with_content_manifest_provider`.
pub struct UnavailableContentManifestRuntimeProvider;
impl ContentManifestRuntimeProvider for UnavailableContentManifestRuntimeProvider {
    fn read_context(&self, node: crate::ids::NodeUuid) -> ContentReadContext {
        ContentReadContext {
            domain: ContentDomainId(node.0),
        }
    }
    fn immutable_store(&self) -> &dyn ImmutableContentStore {
        static STORE: UnavailableImmutableContentStore = UnavailableImmutableContentStore;
        &STORE
    }
}

/// Adapter seam used by merge strategies and interior query/index lowering.
/// Both consumers receive the complete `{ root, editTail }`, never one field
/// independently, preserving atomic conflict-unit semantics.
pub trait ContentManifestAdapter: Send + Sync + 'static {
    fn adapter_kind(&self) -> &str;
    /// Validate adapter-specific schema metadata at schema activation time.
    /// The default admits third-party adapters that do not impose tighter
    /// constraints than the generic typed-record bounds.
    fn validate_schema(&self, schema: &ContentManifestSchema) -> Result<(), ManifestError> {
        (schema.adapter_kind == self.adapter_kind())
            .then_some(())
            .ok_or(ManifestError::InvalidSchema)
    }
    /// Validate one already type-checked tail entry.  The concrete type comes
    /// from `ContentManifestSchema::tail_entry_type`, so adapters never need
    /// a shared byte envelope or a per-row discriminant.
    fn validate_operation(&self, operation: &Value) -> Result<(), ManifestError>;
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

/// Process-local registry of adapter implementations.
///
/// Registrations are deliberately append-only.  An adapter kind is part of a
/// replicated schema identity, so replacing its implementation while values
/// are live could make the same manifest mean different things in different
/// worker threads.  Register adapters during process startup, before opening
/// nodes which can accept that schema.  The registry holds adapters behind an
/// `Arc`, so readers never hold its lock while executing adapter code.
#[derive(Default)]
pub struct ContentManifestAdapterRegistry {
    adapters: RwLock<BTreeMap<String, Arc<dyn ContentManifestAdapter>>>,
}

impl ContentManifestAdapterRegistry {
    /// Register a new adapter. Registering the exact same `Arc` again is
    /// idempotent; registering a different implementation for the same kind
    /// fails closed.
    pub fn register(&self, adapter: Arc<dyn ContentManifestAdapter>) -> Result<(), ManifestError> {
        let kind = adapter.adapter_kind().to_owned();
        if kind.is_empty() {
            return Err(ManifestError::InvalidSchema);
        }
        // A previous adapter panic must not turn an unknown cell into an
        // unchecked one. The map remains valid, and adapter errors are
        // returned by the execution call itself.
        let mut adapters = self
            .adapters
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match adapters.get(&kind) {
            Some(existing) if Arc::ptr_eq(existing, &adapter) => Ok(()),
            Some(_) => Err(ManifestError::AdapterAlreadyRegistered(kind)),
            None => {
                adapters.insert(kind, adapter);
                Ok(())
            }
        }
    }

    /// Resolve an adapter by the schema-owned stable kind.
    pub fn get(&self, kind: &str) -> Result<Arc<dyn ContentManifestAdapter>, ManifestError> {
        self.adapters
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(kind)
            .cloned()
            .ok_or_else(|| ManifestError::UnknownAdapter(kind.to_owned()))
    }
}

/// The process-wide registry used by row codecs. It has no unregister API:
/// adapter lifetime is the process lifetime, avoiding use-after-unregister
/// races with node/query worker threads.
pub fn global_content_manifest_adapters() -> &'static ContentManifestAdapterRegistry {
    static REGISTRY: OnceLock<ContentManifestAdapterRegistry> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        let registry = ContentManifestAdapterRegistry::default();
        registry
            .register(Arc::new(crate::text_content::TextContentAdapter::default()))
            .expect("built-in text content adapter registers exactly once");
        registry
            .register(Arc::new(
                crate::stream_manifest::StreamManifestAdapter::default(),
            ))
            .expect("built-in stream manifest adapter must register once");
        registry
            .register(Arc::new(crate::file_content::FileContentAdapter))
            .expect("built-in file content adapter registers exactly once");
        registry
    })
}

/// Validate adapter-specific schema limits when the adapter is built in or has
/// already been registered. Unknown third-party adapters remain declarable so
/// applications can register them before opening a node.
pub fn validate_content_manifest_schema(
    schema: &ContentManifestSchema,
) -> Result<(), ManifestError> {
    match global_content_manifest_adapters().get(&schema.adapter_kind) {
        Ok(adapter) => adapter.validate_schema(schema),
        Err(ManifestError::UnknownAdapter(_)) => Ok(()),
        Err(error) => Err(error),
    }
}

/// The runtime bridge from a schema-declared content-manifest cell to an
/// adapter. All operations decode and validate the whole atomic cell before
/// calling the adapter; neither query/index nor merge callers can accidentally
/// observe a root while omitting its live edit tail.
pub struct ContentManifestRuntime<'a> {
    registry: &'a ContentManifestAdapterRegistry,
    context: ContentReadContext,
    store: &'a dyn ImmutableContentStore,
}

impl<'a> ContentManifestRuntime<'a> {
    pub fn new(
        registry: &'a ContentManifestAdapterRegistry,
        context: ContentReadContext,
        store: &'a dyn ImmutableContentStore,
    ) -> Self {
        Self {
            registry,
            context,
            store,
        }
    }

    /// Decode an actual row cell and check every typed tail operation through
    /// its registered adapter. Unknown adapter kinds fail closed.
    pub fn decode_cell(
        &self,
        schema: &ContentManifestSchema,
        value: &Value,
    ) -> Result<ContentManifest, ManifestError> {
        let manifest = ContentManifest::from_value(value, schema)?;
        let adapter = self.registry.get(&schema.adapter_kind)?;
        for operation in &manifest.edit_tail {
            adapter.validate_operation(operation)?;
        }
        Ok(manifest)
    }

    pub fn materialize_cell(
        &self,
        schema: &ContentManifestSchema,
        value: &Value,
        request: &MaterializationRequest,
    ) -> Result<Vec<u8>, ManifestError> {
        let manifest = self.decode_cell(schema, value)?;
        self.registry.get(&schema.adapter_kind)?.materialize(
            &manifest,
            request,
            self.context,
            self.store,
        )
    }

    /// Merge complete candidate cells using the schema's adapter and return one
    /// canonical atomic `Record` cell. This is the only adapter merge entry
    /// point; callers never receive independently mergeable root/tail fields.
    pub fn merge_cells(
        &self,
        schema: &ContentManifestSchema,
        values: &[Value],
    ) -> Result<Value, ManifestError> {
        let manifests = values
            .iter()
            .map(|value| self.decode_cell(schema, value))
            .collect::<Result<Vec<_>, _>>()?;
        let merged =
            self.registry
                .get(&schema.adapter_kind)?
                .merge(&manifests, self.context, self.store)?;
        merged.validate(schema)?;
        for operation in &merged.edit_tail {
            self.registry
                .get(&schema.adapter_kind)?
                .validate_operation(operation)?;
        }
        merged.into_value(schema)
    }

    /// Derive interior query/index values from a full manifest. Values are
    /// adapter-owned and intentionally not silently persisted as ordinary
    /// columns; adapters decide which projections are safe to expose.
    pub fn index_values_for_cell(
        &self,
        schema: &ContentManifestSchema,
        value: &Value,
        requested: &[String],
    ) -> Result<BTreeMap<String, Vec<u8>>, ManifestError> {
        let manifest = self.decode_cell(schema, value)?;
        self.registry.get(&schema.adapter_kind)?.index_values(
            &manifest,
            requested,
            self.context,
            self.store,
        )
    }
}

/// Validate a manifest cell at the ordinary row-codec boundary. The global
/// registry supplies the schema-defined operation validation; execution paths
/// that need immutable reads use [`ContentManifestRuntime`] with their own
/// store and authorization domain.
pub fn validate_registered_cell(
    schema: &ContentManifestSchema,
    value: &Value,
) -> Result<(), ManifestError> {
    let manifest = ContentManifest::from_value(value, schema)?;
    let adapter = global_content_manifest_adapters().get(&schema.adapter_kind)?;
    for operation in &manifest.edit_tail {
        adapter.validate_operation(operation)?;
    }
    Ok(())
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct FixtureAdapter {
        materializations: AtomicUsize,
        merges: AtomicUsize,
        indices: AtomicUsize,
    }

    impl FixtureAdapter {
        fn registered() -> Arc<Self> {
            static ADAPTER: OnceLock<Arc<FixtureAdapter>> = OnceLock::new();
            let adapter = ADAPTER
                .get_or_init(|| {
                    Arc::new(FixtureAdapter {
                        materializations: AtomicUsize::new(0),
                        merges: AtomicUsize::new(0),
                        indices: AtomicUsize::new(0),
                    })
                })
                .clone();
            global_content_manifest_adapters()
                .register(adapter.clone())
                .unwrap();
            adapter
        }
    }

    impl ContentManifestAdapter for FixtureAdapter {
        fn adapter_kind(&self) -> &str {
            "manifest-runtime-fixture-v1"
        }
        fn validate_operation(&self, operation: &Value) -> Result<(), ManifestError> {
            if matches!(operation, Value::Bytes(bytes) if bytes.starts_with(b"+")) {
                Ok(())
            } else {
                Err(ManifestError::Conflict("fixture operation"))
            }
        }
        fn materialize(
            &self,
            manifest: &ContentManifest,
            request: &MaterializationRequest,
            context: ContentReadContext,
            store: &dyn ImmutableContentStore,
        ) -> Result<Vec<u8>, ManifestError> {
            self.materializations.fetch_add(1, Ordering::Relaxed);
            let mut full = store
                .get(context, manifest.root)
                .ok_or(ManifestError::Conflict("fixture root missing"))?
                .to_vec();
            for operation in &manifest.edit_tail {
                let Value::Bytes(operation) = operation else {
                    return Err(ManifestError::Conflict("fixture operation type"));
                };
                full.extend_from_slice(&operation[1..]);
            }
            match request {
                MaterializationRequest::Full | MaterializationRequest::Projection(_) => Ok(full),
                MaterializationRequest::Range { offset, length } => Ok(full
                    .get(*offset as usize..offset.saturating_add(*length) as usize)
                    .ok_or(ManifestError::Conflict("fixture range"))?
                    .to_vec()),
            }
        }
        fn merge(
            &self,
            manifests: &[ContentManifest],
            _context: ContentReadContext,
            _store: &dyn ImmutableContentStore,
        ) -> Result<ContentManifest, ManifestError> {
            self.merges.fetch_add(1, Ordering::Relaxed);
            let first = manifests
                .first()
                .ok_or(ManifestError::Conflict("fixture empty merge"))?;
            let mut edit_tail = Vec::new();
            for manifest in manifests {
                edit_tail.extend(manifest.edit_tail.clone());
            }
            Ok(ContentManifest {
                root: first.root,
                edit_tail,
            })
        }
        fn index_values(
            &self,
            manifest: &ContentManifest,
            requested: &[String],
            context: ContentReadContext,
            store: &dyn ImmutableContentStore,
        ) -> Result<BTreeMap<String, Vec<u8>>, ManifestError> {
            self.indices.fetch_add(1, Ordering::Relaxed);
            let full = self.materialize(manifest, &MaterializationRequest::Full, context, store)?;
            Ok(requested
                .iter()
                .cloned()
                .map(|name| (name, full.clone()))
                .collect())
        }
    }
    #[test]
    fn manifest_codec_is_atomic_and_enforces_its_boundaries() {
        // Internal because this is a wire codec boundary with no public client API yet.
        let schema =
            ContentManifestSchema::with_tail_entry_type("fixture-v1", ValueType::Bytes, 2, 3)
                .unwrap();
        let value = ContentManifest {
            root: ContentId([7; 32]),
            edit_tail: vec![Value::Bytes(b"a".to_vec()), Value::Bytes(b"bc".to_vec())],
        };
        assert_eq!(
            ContentManifest::from_value(&value.into_value(&schema).unwrap(), &schema).unwrap(),
            value
        );
        assert_eq!(
            value.into_value(
                &ContentManifestSchema::with_tail_entry_type("fixture-v1", ValueType::Bytes, 1, 3)
                    .unwrap()
            ),
            Err(ManifestError::TooManyTailEntries {
                actual: 2,
                maximum: 1
            })
        );
        assert!(
            ContentManifest::from_value(&Value::Bytes(b"not a record".to_vec()), &schema).is_err()
        );
    }

    #[test]
    fn schema_owned_tail_type_is_an_actual_record_field_not_a_byte_convention() {
        let schema = ContentManifestSchema::with_tail_entry_type(
            "typed-tail-fixture-v1",
            ValueType::String,
            2,
            64,
        )
        .unwrap();
        let manifest = ContentManifest {
            root: ContentId([3; 32]),
            edit_tail: vec![Value::String("insert: hello".into())],
        };
        let value = manifest.into_value(&schema).unwrap();
        assert!(matches!(value, Value::Record(_)));
        assert_eq!(
            ContentManifest::from_value(&value, &schema).unwrap(),
            manifest
        );

        // Planted negative: treating this tail as the old shared byte carrier
        // fails in the record codec before an adapter sees it.
        assert_eq!(
            ContentManifest {
                root: ContentId([3; 32]),
                edit_tail: vec![Value::Bytes(b"insert: hello".to_vec())],
            }
            .into_value(&schema),
            Err(ManifestError::Malformed)
        );
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
            ContentManifestSchema::with_tail_entry_type("", ValueType::Bytes, 1, 1),
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

    #[test]
    fn registered_adapter_runs_for_actual_schema_column_and_all_runtime_seams() {
        let adapter = FixtureAdapter::registered();
        let schema = ContentManifestSchema::with_tail_entry_type(
            "manifest-runtime-fixture-v1",
            ValueType::Bytes,
            4,
            64,
        )
        .unwrap();
        let column = crate::schema::ColumnSchema::content_manifest("body", schema.clone());
        let domain = ContentDomainId(uuid::Uuid::from_bytes([9; 16]));
        let context = ContentReadContext { domain };
        let mut store = MemoryImmutableContentStore::default();
        let root = store
            .put_if_absent_or_identical(
                ContentAddress {
                    domain,
                    adapter_kind: &schema.adapter_kind,
                    kind: ImmutableContentKind::Root,
                },
                b"root".to_vec(),
            )
            .unwrap();
        let left = ContentManifest {
            root,
            edit_tail: vec![Value::Bytes(b"+ left".to_vec())],
        }
        .into_value(&schema)
        .unwrap();
        let right = ContentManifest {
            root,
            edit_tail: vec![Value::Bytes(b"+ right".to_vec())],
        }
        .into_value(&schema)
        .unwrap();

        // This is the ordinary row codec boundary, not a direct adapter call.
        crate::node::codec::validate_cell_value(&column, &left).unwrap();
        let runtime =
            ContentManifestRuntime::new(global_content_manifest_adapters(), context, &store);
        assert_eq!(
            runtime
                .materialize_cell(&schema, &left, &MaterializationRequest::Full)
                .unwrap(),
            b"root left"
        );
        assert_eq!(
            runtime
                .materialize_cell(
                    &schema,
                    &left,
                    &MaterializationRequest::Range {
                        offset: 4,
                        length: 5
                    }
                )
                .unwrap(),
            b" left"
        );
        let indexed = runtime
            .index_values_for_cell(&schema, &left, &["search".into()])
            .unwrap();
        assert_eq!(indexed["search"], b"root left");
        let merged = runtime
            .merge_cells(&schema, &[left.clone(), right])
            .unwrap();
        assert_eq!(
            ContentManifest::from_value(&merged, &schema)
                .unwrap()
                .edit_tail,
            vec![
                Value::Bytes(b"+ left".to_vec()),
                Value::Bytes(b"+ right".to_vec())
            ]
        );
        assert!(adapter.materializations.load(Ordering::Relaxed) >= 3);
        assert_eq!(adapter.merges.load(Ordering::Relaxed), 1);
        assert_eq!(adapter.indices.load(Ordering::Relaxed), 1);

        // Planted sensitivity: a tail that the adapter rejects must be refused
        // at the actual row codec, proving it was not merely shape-checked.
        let invalid = ContentManifest {
            root,
            edit_tail: vec![Value::Bytes(b"not-an-operation".to_vec())],
        }
        .into_value(&schema)
        .unwrap();
        assert!(crate::node::codec::validate_cell_value(&column, &invalid).is_err());
    }

    #[test]
    fn unknown_adapter_fails_closed_at_row_codec_boundary() {
        let schema = ContentManifestSchema::with_tail_entry_type(
            "intentionally-unregistered-manifest-v1",
            ValueType::Bytes,
            1,
            8,
        )
        .unwrap();
        let column = crate::schema::ColumnSchema::content_manifest("body", schema.clone());
        let value = ContentManifest {
            root: ContentId([1; 32]),
            edit_tail: vec![],
        }
        .into_value(&schema)
        .unwrap();
        assert!(crate::node::codec::validate_cell_value(&column, &value).is_err());
    }
}
