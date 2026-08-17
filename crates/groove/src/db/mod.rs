//! Schema-aware database facade over records, storage, and the IVM runtime.
//!
//! This module owns the public [`Database`] API: opening a schema on an
//! [`ResidentStorage`], encoding user rows through [`RecordDescriptor`],
//! maintaining primary/secondary durable storage entries, and synchronously
//! ticking [`IvmRuntime`] after committed batches. Query planning and graph
//! execution live in [`crate::ivm`]; binary row layout lives in
//! [`crate::records`]; storage durability lives below the [`ResidentStorage`]
//! seam. New readers should start here to see how commits become table deltas
//! and how subscriptions are exposed above the engine.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::str;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use web_time::{Duration, Instant};

use crate::ivm::runtime::{durable_index_key_prefix, encode_key_part};
use crate::ivm::{
    IvmRuntime, PlannerError, QueryParameter, RecordDelta, RecordDeltas, RuntimeStats, TableDelta,
    TickMetrics, plan_prepared_shape, plan_query,
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
    LayoutStorage, OwnedWriteOperation, RecordStore, ResidentStorage, StagedWriteOverlay,
    StagedWriteState, StorageLayout, WriteOperation,
};
use thiserror::Error;

pub use crate::ivm::{
    CollectByField, GraphBuilder, IvmRuntimeError, MultisinkDeltas, MultisinkSubscription,
    PredicateExpr, PreparedShapeId, ProjectField, RoutedMultisinkTerminal, Subscription,
    SubscriptionId,
};

/// Schema-aware database facade over storage and IVM subscriptions.
pub struct Database<S> {
    storage: LayoutStorage<S>,
    /// Owns query/index maintenance over the storage-backed base tables.
    ivm_runtime: IvmRuntime,
    last_commit_metrics: Option<CommitMetrics>,
    last_tick_metrics: Option<TickMetrics>,
    storage_read_metrics: RefCell<StorageReadMetrics>,
    /// Host-owned transactions may span several Groove storage commits while
    /// remaining one externally atomic publication. Notifications stay queued
    /// until the outermost host scope completes.
    durable_publication_state: Arc<Mutex<DurablePublicationState>>,
    poisoned: Arc<AtomicBool>,
}

/// Owned durable-storage work produced by a synchronously visible local commit.
///
/// This is an internal migration seam for hosts whose durable
/// [`ResidentStorage`] adapter may suspend. Applying a batch to the resident
/// database still advances the IVM and publishes local subscription deltas in
/// the caller's stack. The host then owns this receipt until the corresponding
/// durable commit succeeds or poisons the database on failure.
#[doc(hidden)]
#[derive(Clone, Debug)]
#[must_use = "the pending persistence batch must be committed or failed"]
pub struct PendingPersistenceBatch {
    operations: Vec<OwnedWriteOperation>,
}

/// Storage-resolved writes ready for one non-suspending resident IVM tick.
///
/// Construction performs no IVM evaluation and publication consumes the value
/// exactly once. Async hosts retain this across the acquire/publish boundary so
/// the raw batch cannot be reinterpreted after acquisition succeeds.
#[doc(hidden)]
#[must_use = "a prepared database batch must be published or discarded"]
pub struct PreparedDatabaseBatch {
    pending_writes: Vec<PendingTableWrite>,
    direct_operations: Vec<OwnedWriteOperation>,
}

impl PendingPersistenceBatch {
    /// Consume the receipt into the exact owned storage operations.
    #[doc(hidden)]
    pub fn into_operations(self) -> Vec<OwnedWriteOperation> {
        self.operations
    }
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
    pub fn finish<S: ResidentStorage>(mut self, database: &mut Database<S>) {
        assert!(
            Arc::ptr_eq(&self.state, &database.durable_publication_state),
            "durable publication scope belongs to a different database"
        );
        self.resolve(false);
        database.settle_durable_publication_scopes();
    }

    /// Abort this scope and poison its whole nested publication unit.
    #[doc(hidden)]
    pub fn abort<S: ResidentStorage>(mut self, database: &mut Database<S>) {
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
mod storage_runtime;

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
#[doc(hidden)]
pub use storage_runtime::{
    DemandDrivenDatabase, PersistenceQueue, PersistenceUnitId, PollableDatabase, StorageAcquisition,
};

#[derive(Debug, Error)]
pub enum Error {
    #[error("database instance is poisoned after a failed atomic commit")]
    DatabasePoisoned,
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
