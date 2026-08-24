//! Schema-aware database facade over records, storage, and the IVM runtime.
//!
//! This module owns the public [`Database`] API: opening a schema on an
//! [`OrderedKvStorage`], encoding user rows through [`RecordDescriptor`],
//! maintaining primary/secondary durable storage entries, and synchronously
//! ticking [`IvmRuntime`] after committed batches. Query planning and graph
//! execution live in [`crate::ivm`]; binary row layout lives in
//! [`crate::records`]; storage durability lives below the [`OrderedKvStorage`]
//! seam. New readers should start here to see how commits become table deltas
//! and how subscriptions are exposed above the engine.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::rc::Rc;
use std::str;
use std::task::{Poll, Waker};

use futures::lock::Mutex as AsyncMutex;
use web_time::{Duration, Instant};

use crate::ivm::runtime::{durable_index_key_prefix, encode_key_part};
use crate::ivm::{
    IvmRuntime, PlannerError, PublicationId, QueryParameter, RecordDelta, RecordDeltas,
    RuntimeStats, TableDelta, TickMetrics, plan_prepared_shape, plan_query,
};
use crate::queries::Query;
use crate::records::{
    self, BorrowedRecord, EnumSchema, OwnedRecord, Record, RecordDescriptor, Value, VariantRecord,
    encode_variant_record, split_variant_record,
};
use crate::schema::{
    ColumnType, DatabaseSchema, DirectRecordStoreSchema, IndexSchema, IntegerKeyType, PrimaryKey,
    PrimaryKeyColumn, PrimaryKeyType, TableSchema, TableVariant,
};
use crate::storage::{
    BoxedStorage, LayoutStorage, OrderedKvStorage, OwnedStorage, OwnedWriteOperation, RecordStore,
    ReopenableStorage, StagedWriteOverlay, StagedWriteState, StorageLayout,
};
use thiserror::Error;

/// Reserved Groove-owned metadata plane for staged roots and persisted
/// reference accounting. It is never exposed as an application table.
pub const LARGE_VALUE_METADATA_CF: &str = "__groove_large_values";

fn staged_large_value_key(id: crate::large_values::StagedLargeValueId) -> Vec<u8> {
    let mut key = b"staged/".to_vec();
    key.extend_from_slice(&id.0);
    key
}

fn pending_large_value_upload_key(id: crate::large_values::StagedLargeValueId) -> Vec<u8> {
    let mut key = b"upload/".to_vec();
    key.extend_from_slice(&id.0);
    key
}

fn large_value_root_key(node_ref: &crate::large_values::NodeRef) -> Result<Vec<u8>, Error> {
    let mut key = b"root/".to_vec();
    key.extend(postcard::to_allocvec(node_ref).map_err(|error| {
        Error::InvalidLargeValueMetadata(format!("cannot encode root identity: {error}"))
    })?);
    Ok(key)
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
struct LargeValueRootReferences {
    durable: u64,
    staged: u64,
    node_active: bool,
}

fn large_value_node_key(node_ref: &crate::large_values::NodeRef) -> Result<Vec<u8>, Error> {
    let mut key = b"node/".to_vec();
    key.extend(postcard::to_allocvec(node_ref).map_err(|error| {
        Error::InvalidLargeValueMetadata(format!("cannot encode node identity: {error}"))
    })?);
    Ok(key)
}

fn large_value_reclaim_key(node_ref: &crate::large_values::NodeRef) -> Result<Vec<u8>, Error> {
    let mut key = b"reclaim/".to_vec();
    key.extend(postcard::to_allocvec(node_ref).map_err(|error| {
        Error::InvalidLargeValueMetadata(format!("cannot encode reclaim identity: {error}"))
    })?);
    Ok(key)
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
struct LargeValueNodeReferences {
    references: u64,
    #[serde(default)]
    upload_references: u64,
    children: Vec<crate::large_values::NodeRef>,
}

#[derive(Clone)]
struct MetadataChunkInstallObserver {
    storage: std::rc::Weak<LayoutStorage>,
}

impl crate::chunks::ChunkInstallObserver for MetadataChunkInstallObserver {
    fn installed(
        &self,
        node_ref: crate::large_values::NodeRef,
        encoded: bytes::Bytes,
    ) -> crate::chunks::ChunkFuture<'_, Result<(), crate::chunks::ChunkError>> {
        Box::pin(async move {
            let storage = self.storage.upgrade().ok_or_else(|| {
                crate::chunks::ChunkError::Backend(
                    "database storage closed during chunk installation".to_owned(),
                )
            })?;
            let node: crate::large_values::ChunkNode = postcard::from_bytes(&encoded)
                .map_err(|error| crate::chunks::ChunkError::Backend(error.to_string()))?;
            let children = match node {
                crate::large_values::ChunkNode::Leaf { .. } => Vec::new(),
                crate::large_values::ChunkNode::Branch { children, .. } => children
                    .into_iter()
                    .map(|child| child.node_ref)
                    .collect::<Vec<_>>(),
            };
            let node_key = large_value_node_key(&node_ref)
                .map_err(|error| crate::chunks::ChunkError::Backend(error.to_string()))?;
            let existing = storage
                .get(LARGE_VALUE_METADATA_CF.to_owned(), node_key.clone())
                .await
                .map_err(|error| crate::chunks::ChunkError::Backend(error.to_string()))?;
            let mut metadata: LargeValueNodeReferences = existing
                .as_deref()
                .map(postcard::from_bytes)
                .transpose()
                .map_err(|error| crate::chunks::ChunkError::Backend(error.to_string()))?
                .unwrap_or_default();
            if !metadata.children.is_empty() && metadata.children != children {
                return Err(crate::chunks::ChunkError::Integrity);
            }
            let newly_discovered_active_children =
                metadata.references > 0 && metadata.children.is_empty() && !children.is_empty();
            metadata.children = children.clone();

            let root_key = large_value_root_key(&node_ref)
                .map_err(|error| crate::chunks::ChunkError::Backend(error.to_string()))?;
            let root_encoded = storage
                .get(LARGE_VALUE_METADATA_CF.to_owned(), root_key.clone())
                .await
                .map_err(|error| crate::chunks::ChunkError::Backend(error.to_string()))?;
            let mut root_references: LargeValueRootReferences = root_encoded
                .as_deref()
                .map(postcard::from_bytes)
                .transpose()
                .map_err(|error| crate::chunks::ChunkError::Backend(error.to_string()))?
                .unwrap_or_default();
            let activate_root = root_references
                .durable
                .saturating_add(root_references.staged)
                > 0
                && !root_references.node_active;
            if activate_root {
                root_references.node_active = true;
                metadata.references = metadata.references.checked_add(1).ok_or_else(|| {
                    crate::chunks::ChunkError::Backend("node reference count overflow".to_owned())
                })?;
            }
            let activate_children =
                newly_discovered_active_children || (activate_root && metadata.references == 1);
            let mut operations = vec![OwnedWriteOperation::Set {
                cf: LARGE_VALUE_METADATA_CF.to_owned(),
                key: node_key,
                value: postcard::to_allocvec(&metadata)
                    .map_err(|error| crate::chunks::ChunkError::Backend(error.to_string()))?,
            }];
            if activate_root {
                operations.push(OwnedWriteOperation::Set {
                    cf: LARGE_VALUE_METADATA_CF.to_owned(),
                    key: root_key,
                    value: postcard::to_allocvec(&root_references)
                        .map_err(|error| crate::chunks::ChunkError::Backend(error.to_string()))?,
                });
            }
            if activate_children {
                for child in children {
                    let child_key = large_value_node_key(&child)
                        .map_err(|error| crate::chunks::ChunkError::Backend(error.to_string()))?;
                    let encoded = storage
                        .get(LARGE_VALUE_METADATA_CF.to_owned(), child_key.clone())
                        .await
                        .map_err(|error| crate::chunks::ChunkError::Backend(error.to_string()))?;
                    let mut child_metadata: LargeValueNodeReferences = encoded
                        .as_deref()
                        .map(postcard::from_bytes)
                        .transpose()
                        .map_err(|error| crate::chunks::ChunkError::Backend(error.to_string()))?
                        .unwrap_or_default();
                    child_metadata.references =
                        child_metadata.references.checked_add(1).ok_or_else(|| {
                            crate::chunks::ChunkError::Backend(
                                "child reference count overflow".to_owned(),
                            )
                        })?;
                    operations.push(OwnedWriteOperation::Set {
                        cf: LARGE_VALUE_METADATA_CF.to_owned(),
                        key: child_key,
                        value: postcard::to_allocvec(&child_metadata).map_err(|error| {
                            crate::chunks::ChunkError::Backend(error.to_string())
                        })?,
                    });
                }
            }
            storage
                .write_many(operations)
                .await
                .map_err(|error| crate::chunks::ChunkError::Backend(error.to_string()))
        })
    }
}

pub use crate::ivm::{
    CollectByField, GraphBuilder, IvmRuntimeError, MultisinkDeltas, MultisinkSubscription,
    PredicateExpr, PreparedShapeId, ProjectField, PublicationUpdate, RoutedMultisinkTerminal,
    Subscription, SubscriptionError, SubscriptionEvent, SubscriptionId,
};

/// Schema-aware database facade over storage and IVM subscriptions.
pub struct Database {
    storage: Rc<LayoutStorage>,
    chunk_storage: Rc<dyn crate::chunks::ChunkStorage>,
    resident_chunks: crate::chunks::ResidentChunkStorage,
    chunk_resolver: Rc<dyn crate::chunks::MissingChunkResolver>,
    /// Owns query/index maintenance over the storage-backed base tables.
    ivm_runtime: IvmRuntime,
    last_commit_metrics: Option<CommitMetrics>,
    last_tick_metrics: Option<TickMetrics>,
    storage_read_metrics: Rc<RefCell<StorageReadMetrics>>,
    next_publication_id: u64,
    durable_publication_frontier: Option<PublicationId>,
    resident_publications: BTreeMap<PublicationId, ResidentPublication>,
    persisted_publications: BTreeSet<PublicationId>,
    retracted_publications: BTreeSet<PublicationId>,
    resident_writes: Rc<RefCell<StagedWriteState>>,
    publication_persistence: Rc<RefCell<PersistenceOrder>>,
    /// Serializes the durable upload journal, separate blob staging, expiry,
    /// promotion, and reclamation lifecycle. The blob backend may be separate
    /// from metadata storage, so this boundary prevents both intent eviction
    /// during an in-flight put and lost reference-count updates across uploads.
    large_value_lifecycle: Rc<AsyncMutex<()>>,
    abandoned_application: Rc<Cell<bool>>,
    poisoned: bool,
}

/// All state Grove must retain to undo one not-yet-durable resident
/// publication through the same IVM runtime that made it visible.
///
/// This deliberately lives below Jazz: it is a general publication ownership
/// primitive, not a large-value-specific overlay or evaluator.
#[derive(Clone)]
struct ResidentPublication {
    operations: Vec<OwnedWriteOperation>,
    inverse_table_deltas: Vec<TableDelta>,
    resident_node_refs: Vec<crate::large_values::NodeRef>,
    notifications_deferred: bool,
    retractable: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AppliedBatchLifecycle {
    Applied,
    Persisting,
    PersistenceComplete,
    Finished,
    Abandoned,
}

/// One resident publication whose ordered storage write can progress without
/// borrowing the database runtime.
#[must_use = "an immediate publication must be persisted and settled"]
pub struct AppliedBatch {
    publication: PublicationId,
    storage: Rc<LayoutStorage>,
    operations: Vec<OwnedWriteOperation>,
    order: Rc<RefCell<PersistenceOrder>>,
    ivm_tick_time: Duration,
    storage_writes: StorageWriteMetrics,
    tick: TickMetrics,
    notifications_deferred: bool,
    retractable: bool,
    lifecycle: Rc<Cell<AppliedBatchLifecycle>>,
    abandoned_application: Rc<Cell<bool>>,
}

impl AppliedBatch {
    pub fn publication(&self) -> PublicationId {
        self.publication
    }

    /// Whether a failed receipt must be settled through the reversible
    /// resident-publication path.
    pub fn is_retractable(&self) -> bool {
        self.retractable
    }

    pub async fn persist(&self) -> PersistedBatch {
        assert_eq!(
            self.lifecycle.replace(AppliedBatchLifecycle::Persisting),
            AppliedBatchLifecycle::Applied,
            "an applied batch may have only one persistence attempt at a time",
        );
        let mut attempt = PersistenceAttempt {
            lifecycle: Rc::clone(&self.lifecycle),
            completed: false,
        };
        let turn = std::future::poll_fn(|cx| {
            let mut order = self.order.borrow_mut();
            if let Some(message) = &order.failure {
                return Poll::Ready(Err(crate::storage::Error::Backend {
                    backend: "publication order",
                    message: message.clone(),
                }));
            }
            if order.next == self.publication.0 {
                return Poll::Ready(Ok(()));
            }
            order.waiters.insert(self.publication.0, cx.waker().clone());
            Poll::Pending
        })
        .await;
        let storage_start = Instant::now();
        let result = match turn {
            Ok(()) => self.storage.write_many(self.operations.clone()).await,
            Err(error) => Err(error),
        };
        let storage_write_time = storage_start.elapsed();
        self.lifecycle
            .set(AppliedBatchLifecycle::PersistenceComplete);
        attempt.completed = true;
        let waiter = {
            let mut order = self.order.borrow_mut();
            if result.is_ok() {
                order.next = order.next.saturating_add(1);
                let next = order.next;
                order.waiters.remove(&next)
            } else {
                order.failure = Some(
                    result
                        .as_ref()
                        .expect_err("failed persistence has an error")
                        .to_string(),
                );
                let waiters = std::mem::take(&mut order.waiters);
                for (_, waiter) in waiters {
                    waiter.wake();
                }
                None
            }
        };
        if let Some(waiter) = waiter {
            waiter.wake();
        }
        PersistedBatch {
            publication: self.publication,
            result,
            notifications_deferred: self.notifications_deferred,
            metrics: CommitMetrics {
                storage_write_time,
                ivm_tick_time: self.ivm_tick_time,
                storage_write_count: self.storage_writes.total.count,
                storage_write_bytes: self.storage_writes.total.bytes,
                storage_writes: self.storage_writes,
                tick: self.tick.clone(),
            },
            receipt: PersistenceReceipt {
                lifecycle: Rc::clone(&self.lifecycle),
                abandoned_application: Rc::clone(&self.abandoned_application),
            },
        }
    }
}

struct PersistenceAttempt {
    lifecycle: Rc<Cell<AppliedBatchLifecycle>>,
    completed: bool,
}

impl Drop for PersistenceAttempt {
    fn drop(&mut self) {
        if !self.completed && self.lifecycle.get() == AppliedBatchLifecycle::Persisting {
            self.lifecycle.set(AppliedBatchLifecycle::Applied);
        }
    }
}

impl Drop for AppliedBatch {
    fn drop(&mut self) {
        if self.lifecycle.get() == AppliedBatchLifecycle::Applied {
            self.lifecycle.set(AppliedBatchLifecycle::Abandoned);
            self.abandoned_application.set(true);
        }
    }
}

struct PersistenceOrder {
    next: u64,
    waiters: BTreeMap<u64, Waker>,
    failure: Option<String>,
    /// Publication ids deliberately retracted before they reached storage.
    /// They advance ordering without ever being written.
    cancelled: BTreeSet<u64>,
}

fn advance_cancelled_publications(order: &mut PersistenceOrder) {
    while order.cancelled.remove(&order.next) {
        order.next = order.next.saturating_add(1);
    }
    if let Some(waiter) = order.waiters.remove(&order.next) {
        waiter.wake();
    }
}

/// Completion of one owned publication persistence operation.
#[must_use = "persistence completion must be settled on its database"]
pub struct PersistedBatch {
    publication: PublicationId,
    result: Result<(), crate::storage::Error>,
    notifications_deferred: bool,
    metrics: CommitMetrics,
    receipt: PersistenceReceipt,
}

struct PersistenceReceipt {
    lifecycle: Rc<Cell<AppliedBatchLifecycle>>,
    abandoned_application: Rc<Cell<bool>>,
}

impl PersistenceReceipt {
    fn finish(&self) {
        self.lifecycle.set(AppliedBatchLifecycle::Finished);
    }
}

impl Drop for PersistenceReceipt {
    fn drop(&mut self) {
        if self.lifecycle.get() == AppliedBatchLifecycle::PersistenceComplete {
            self.lifecycle.set(AppliedBatchLifecycle::Abandoned);
            self.abandoned_application.set(true);
        }
    }
}

mod batch;
mod commit;
mod encoding;
mod facade;
mod primary_storage;
mod query;
mod schema_admission;
mod storage_helpers;

pub use batch::*;
use encoding::*;
pub(crate) use encoding::{index_record_descriptor, persisted_index_primary_key};
use schema_admission::*;
pub(crate) use storage_helpers::MeteredStorage;
use storage_helpers::*;
pub use storage_helpers::{
    CommitMetrics, DirectRecordStore, DirectRecordStoreEntry, DirectRecordStoreWrite,
    EncodedKeyValue, PreparedShape, StorageReadBucket, StorageReadMetrics, StorageWriteBucket,
    StorageWriteMetrics,
};

#[derive(Debug, Error)]
pub enum Error {
    #[error("database instance is poisoned after a failed atomic commit")]
    DatabasePoisoned,
    #[error("publication does not belong to this database: {0:?}")]
    PublicationNotFound(PublicationId),
    #[error("resident publication was retracted after its durability attempt failed: {0:?}")]
    PublicationRetracted(PublicationId),
    #[error("publication is not marked retractable: {0:?}")]
    NonRetractablePublication(PublicationId),
    #[error("cannot safely retract resident publication: {0}")]
    InvalidResidentRetraction(String),
    #[error("subscription ended")]
    SubscriptionEnded,
    #[error(transparent)]
    SubscriptionFailed(#[from] SubscriptionError),
    #[error("duplicate primary key for table {table}: {key:?}")]
    DuplicatePrimaryKey { table: String, key: Vec<u8> },
    #[error("duplicate schema version {version} for table {table}")]
    DuplicateTableVariant { table: String, version: u64 },
    #[error("table {table} variant tag {tag} exceeds the bounded u32 tag space")]
    TableVariantTagOutOfRange { table: String, tag: u64 },
    #[error("duplicate query parameter binding: {0}")]
    DuplicateParameter(String),
    #[error(transparent)]
    IvmRuntime(#[from] IvmRuntimeError),
    #[error("invalid persisted index contents: {0}")]
    InvalidPersistedIndex(String),
    #[error("invalid persisted large-value metadata: {0}")]
    InvalidLargeValueMetadata(String),
    #[error("index key arity mismatch for {index}: expected at most {expected}, got {actual}")]
    IndexKeyArity {
        index: String,
        expected: usize,
        actual: usize,
    },
    #[error("index not found: {table}.{index}")]
    IndexNotFound { table: String, index: String },
    #[error("missing query parameter binding: {0}")]
    MissingParameter(String),
    #[error("table has no primary key: {0}")]
    MissingPrimaryKey(String),
    #[error("invalid field {field} in schema version {version} for table {table}")]
    InvalidTableVariantField {
        table: String,
        version: u64,
        field: String,
    },
    #[error("primary key arity mismatch for {table}: expected at most {expected}, got {actual}")]
    PrimaryKeyArity {
        table: String,
        expected: usize,
        actual: usize,
    },
    #[error("primary key type mismatch for {table}.{column}")]
    PrimaryKeyTypeMismatch { table: String, column: String },
    #[error(transparent)]
    QueryPlanning(#[from] PlannerError),
    #[error(transparent)]
    RecordEncoding(#[from] records::Error),
    #[error("direct record store not found: {0}")]
    DirectRecordStoreNotFound(String),
    #[error("invalid direct record store key: {0}")]
    InvalidDirectRecordStoreKey(String),
    #[error(transparent)]
    Storage(Box<crate::storage::Error>),
    #[error("table not found: {0}")]
    TableNotFound(String),
    #[error("table already exists: {0}")]
    TableAlreadyExists(String),
    #[error("field definition does not match the live catalogue: {table}.{field}")]
    TableFieldDefinitionMismatch { table: String, field: String },
    #[error("index definition does not match the live catalogue: {table}.{index}")]
    TableIndexDefinitionMismatch { table: String, index: String },
    #[error("index {table}.{index} references unknown field {field}")]
    TableIndexFieldNotFound {
        table: String,
        index: String,
        field: String,
    },
    #[error("schema version {version} for table {table} omits primary-key column {column}")]
    SchemaVersionMissingPrimaryKey {
        table: String,
        version: u64,
        column: String,
    },
    #[error("schema-variant table uses foreign keys, which are not supported yet: {0}")]
    UnsupportedSchemaVariantTableFeature(String),
    #[error("record descriptor does not match schema version {version} for table {table}")]
    SchemaVersionDescriptorMismatch { table: String, version: u64 },
    #[error("schema version 0 is reserved for Groove's implicit table layout: {0}")]
    ReservedTableVariant(String),
    #[error("cannot add the first explicit schema version to a live homogeneous table: {0}")]
    CannotPromoteLiveTableToSchemaVariants(String),
    #[error("unknown schema version {version} for table {table}")]
    UnknownTableVariant { table: String, version: u64 },
    #[error("unknown query parameter binding: {0}")]
    UnknownParameter(String),
}

impl From<crate::storage::Error> for Error {
    fn from(error: crate::storage::Error) -> Self {
        Self::Storage(Box::new(error))
    }
}

#[cfg(test)]
mod tests;
