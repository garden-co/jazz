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
use std::sync::{Arc, Mutex};
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
    LayoutStorage, OrderedKvStorage, OwnedStorage, OwnedWriteOperation, RecordStore,
    StagedWriteOverlay, StagedWriteState, StorageLayout,
};
use thiserror::Error;

pub use crate::ivm::{
    CollectByField, GraphBuilder, IvmRuntimeError, MultisinkDeltas, MultisinkSubscription,
    PredicateExpr, PreparedShapeId, ProjectField, PublicationUpdate, RoutedMultisinkTerminal,
    Subscription, SubscriptionId,
};

/// Schema-aware database facade over storage and IVM subscriptions.
pub struct Database<S> {
    storage: Rc<LayoutStorage<S>>,
    /// Owns query/index maintenance over the storage-backed base tables.
    ivm_runtime: IvmRuntime,
    last_commit_metrics: Option<CommitMetrics>,
    last_tick_metrics: Option<TickMetrics>,
    storage_read_metrics: Rc<RefCell<StorageReadMetrics>>,
    /// Host-owned transactions may span several Groove storage commits while
    /// remaining one externally atomic publication. Notifications stay queued
    /// until the outermost host scope completes.
    durable_publication_state: Arc<Mutex<DurablePublicationState>>,
    next_publication_id: u64,
    durable_publication_frontier: Option<PublicationId>,
    resident_publications: BTreeMap<PublicationId, Vec<OwnedWriteOperation>>,
    persisted_publications: BTreeSet<PublicationId>,
    resident_writes: Rc<RefCell<StagedWriteState>>,
    publication_persistence: Rc<RefCell<PublicationPersistenceOrder>>,
    poisoned: bool,
}

/// One resident publication whose ordered storage write can progress without
/// borrowing the database runtime.
#[must_use = "an immediate publication must be persisted and settled"]
pub struct PublishedBatch<S> {
    publication: PublicationId,
    storage: Rc<LayoutStorage<S>>,
    operations: Vec<OwnedWriteOperation>,
    order: Rc<RefCell<PublicationPersistenceOrder>>,
}

impl<S> PublishedBatch<S>
where
    S: OrderedKvStorage,
{
    pub fn publication(&self) -> PublicationId {
        self.publication
    }

    pub async fn persist(&self) -> PublicationPersistence {
        std::future::poll_fn(|cx| {
            let mut order = self.order.borrow_mut();
            if order.next == self.publication.0 {
                return Poll::Ready(());
            }
            order.waiters.insert(self.publication.0, cx.waker().clone());
            Poll::Pending
        })
        .await;
        let result = self.storage.write_many(self.operations.clone()).await;
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
        PublicationPersistence {
            publication: self.publication,
            result,
        }
    }
}

struct PublicationPersistenceOrder {
    next: u64,
    waiters: BTreeMap<u64, Waker>,
}

/// Completion of one owned publication persistence operation.
#[must_use = "persistence completion must be settled on its database"]
pub struct PublicationPersistence {
    publication: PublicationId,
    result: Result<(), crate::storage::Error>,
}

/// Capability token for one host-owned durable publication scope.
///
/// This is an internal cross-crate seam used by Jazz. The token is consumed by
/// exactly one finish or abort operation, preventing double-finalization; the
/// database tracks nesting so an inner abort makes every enclosing completion
/// discard rather than publish.
#[doc(hidden)]
#[must_use = "a durable publication scope must be finished or aborted"]
pub struct DurablePublicationScope {
    state: Arc<Mutex<DurablePublicationState>>,
    resolved: bool,
}

#[derive(Default)]
struct DurablePublicationState {
    depth: usize,
    aborted: bool,
}

impl DurablePublicationScope {
    /// Successfully complete this scope. Publication occurs only when this is
    /// the outermost scope and no nested scope aborted.
    #[doc(hidden)]
    pub fn finish<S: OrderedKvStorage>(mut self, database: &mut Database<S>) {
        assert!(
            Arc::ptr_eq(&self.state, &database.durable_publication_state),
            "durable publication scope belongs to a different database"
        );
        self.resolve(false);
        database.settle_durable_publication_scopes();
    }

    /// Abort this scope and poison its whole nested publication unit.
    #[doc(hidden)]
    pub fn abort<S: OrderedKvStorage>(mut self, database: &mut Database<S>) {
        assert!(
            Arc::ptr_eq(&self.state, &database.durable_publication_state),
            "durable publication scope belongs to a different database"
        );
        self.resolve(true);
        database.settle_durable_publication_scopes();
    }

    fn resolve(&mut self, aborted: bool) {
        let mut state = self
            .state
            .lock()
            .expect("durable publication state mutex poisoned");
        state.depth = state.depth.saturating_sub(1);
        state.aborted |= aborted;
        self.resolved = true;
    }
}

impl Drop for DurablePublicationScope {
    fn drop(&mut self) {
        if !self.resolved {
            self.resolve(true);
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
