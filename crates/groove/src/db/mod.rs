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

pub use crate::ivm::{
    CollectByField, GraphBuilder, IvmRuntimeError, MultisinkDeltas, MultisinkSubscription,
    PredicateExpr, PreparedShapeId, ProjectField, PublicationUpdate, RoutedMultisinkTerminal,
    Subscription, SubscriptionError, SubscriptionEvent, SubscriptionId,
};

/// Schema-aware database facade over storage and IVM subscriptions.
pub struct Database {
    storage: Rc<LayoutStorage>,
    /// Owns query/index maintenance over the storage-backed base tables.
    ivm_runtime: IvmRuntime,
    last_commit_metrics: Option<CommitMetrics>,
    last_tick_metrics: Option<TickMetrics>,
    storage_read_metrics: Rc<RefCell<StorageReadMetrics>>,
    next_publication_id: u64,
    durable_publication_frontier: Option<PublicationId>,
    resident_publications: BTreeMap<PublicationId, Vec<OwnedWriteOperation>>,
    persisted_publications: BTreeSet<PublicationId>,
    resident_writes: Rc<RefCell<StagedWriteState>>,
    publication_persistence: Rc<RefCell<PersistenceOrder>>,
    abandoned_application: Rc<Cell<bool>>,
    poisoned: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AppliedBatchLifecycle {
    Applied,
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
    lifecycle: Rc<Cell<AppliedBatchLifecycle>>,
    abandoned_application: Rc<Cell<bool>>,
}

impl AppliedBatch {
    pub fn publication(&self) -> PublicationId {
        self.publication
    }

    pub async fn persist(&self) -> PersistedBatch {
        std::future::poll_fn(|cx| {
            let mut order = self.order.borrow_mut();
            if order.next == self.publication.0 {
                return Poll::Ready(());
            }
            order.waiters.insert(self.publication.0, cx.waker().clone());
            Poll::Pending
        })
        .await;
        let storage_start = Instant::now();
        let result = self.storage.write_many(self.operations.clone()).await;
        let storage_write_time = storage_start.elapsed();
        self.lifecycle
            .set(AppliedBatchLifecycle::PersistenceComplete);
        if result.is_ok() {
            let waiters = {
                let mut order = self.order.borrow_mut();
                order.next = order.next.saturating_add(1);
                std::mem::take(&mut order.waiters)
            };
            for (_, waiter) in waiters {
                waiter.wake();
            }
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
