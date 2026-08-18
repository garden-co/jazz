//! Storage-backed Jazz node core. This module owns the `NodeState` state struct,
//! public node API surface, shared errors, and cross-cutting in-memory indexes;
//! specialized behavior lives in sibling modules such as [`policy`] for policy
//! evaluation, [`global_state`] for read-only settled-global derivations,
//! [`ingest`] for commit/fate ingestion, [`query_eval`] for query execution, and
//! [`views`] for sync view payloads. In the layer map it is the core between the
//! `Db` facade and groove storage/IVM.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(feature = "testing")]
use std::time::Duration;
#[cfg(feature = "testing")]
use web_time::Instant;

use groove::db::{
    CommitMetrics, Database, DatabaseBatch, DirectRecordStoreWrite, Error as GrooveDbError,
    GraphBuilder, PredicateExpr, PrimaryKeyValue, Subscription,
};
use groove::ivm::PreparedShapeId;
use groove::ivm::ProjectField;
#[cfg(test)]
use groove::queries::{Query, Select, SelectItem, TableRef};
use groove::records::{self, BorrowedRecord, OwnedRecord, Value};
use groove::storage::{self, ReopenableStorage, ResidentStorage, StorageLayout};
use thiserror::Error;

use self::query_engine::user_column_field;
use crate::ids::{
    AuthorId, BranchId, MigrationLensId, NodeAlias, NodeUuid, PhysicalColumnId, PhysicalTableId,
    RowUuid, SchemaLineagePublicationId, SchemaVersionAlias, SchemaVersionId,
};
use crate::protocol::{
    BindingViewKey, CurrentWriteSchema, LensOp, MigrationLens, ProgramFactEntry, ReadViewKey,
    RealRowMemberEntry, ResultMemberEntry, ResultRowEntry, RowVersionRef, SchemaLineagePublication,
    SchemaVersion, ShapeAst, Subscribe, SubscriptionKey, SyncMessage, VersionBundle,
    VersionCarrier, VersionRecord, ViewFactEntry, expand_version_carriers,
};
use crate::query::{Binding, BindingId, QueryError, ShapeId, ValidatedQuery};
use crate::schema::{
    JazzSchema, KNOWN_STATE_FACTS_STORE, MergeStrategy, SETTLED_PROGRAM_FACTS_STORE,
    SETTLED_RESULT_MEMBERS_STORE, TableSchema, registered_column_transform,
};
use crate::time::{GlobalSeq, TxTime};
use crate::tools::OpenBatchId;
use crate::tx::{
    AbsentRead, BranchLineage, BranchMergeProvenance, DeletionEvent, DurabilityTier, Fate,
    HistoryEntry, PredicateRead, RejectedTransaction, RejectedVersion, RejectionReason, RowRead,
    Snapshot, Transaction, TransactionRecord, TxId, TxKind,
};

fn hydrate_nested_scalar_enum_cases(
    value_type: &records::ValueType,
    introducing_schema: SchemaVersionId,
    path: &str,
    output: &mut BTreeMap<String, Vec<GlobalScalarEnumCaseId>>,
) -> Result<(), Error> {
    use records::ValueType;
    match value_type {
        ValueType::EnumTag(schema) => {
            output.entry(path.to_owned()).or_insert_with(|| {
                schema
                    .variants
                    .iter()
                    .enumerate()
                    .map(|(ordinal, _)| GlobalScalarEnumCaseId {
                        introducing_schema,
                        introducing_ordinal: ordinal as u8,
                    })
                    .collect()
            });
        }
        ValueType::Nullable(inner) => hydrate_nested_scalar_enum_cases(
            inner,
            introducing_schema,
            &format!("{path}/nullable"),
            output,
        )?,
        ValueType::Array(inner) => hydrate_nested_scalar_enum_cases(
            inner,
            introducing_schema,
            &format!("{path}/array"),
            output,
        )?,
        ValueType::Tuple(values) => {
            for (index, value) in values.iter().enumerate() {
                hydrate_nested_scalar_enum_cases(
                    value,
                    introducing_schema,
                    &format!("{path}/tuple/{index}"),
                    output,
                )?;
            }
        }
        ValueType::Record(record) => {
            for field in record.fields() {
                let name = field.name.as_deref().ok_or(Error::InvalidStoredValue(
                    "nested enum record field unnamed",
                ))?;
                hydrate_nested_scalar_enum_cases(
                    &field.value_type,
                    introducing_schema,
                    &format!("{path}/record/{name}"),
                    output,
                )?;
            }
        }
        ValueType::Enum(schema) => {
            for (ordinal, case) in schema.cases.iter().enumerate() {
                let identity = GlobalScalarEnumCaseId {
                    introducing_schema,
                    introducing_ordinal: ordinal as u8,
                };
                hydrate_nested_scalar_enum_cases(
                    &records::ValueType::Record(Box::new(case.payload.clone())),
                    introducing_schema,
                    &global_case_path(path, &identity),
                    output,
                )?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn global_case_path(path: &str, case: &GlobalScalarEnumCaseId) -> String {
    format!(
        "{path}/case/{}/{}",
        case.introducing_schema.0.simple(),
        case.introducing_ordinal
    )
}

/// Register every nested payload enum by its schema-qualified case identity.
/// Scalar descendants are handled by the existing scalar registry walker;
/// this map owns only the payload tag boundary itself.
fn hydrate_nested_payload_enum_cases(
    value_type: &records::ValueType,
    introducing_schema: SchemaVersionId,
    path: &str,
    output: &mut BTreeMap<String, Vec<GlobalScalarEnumCaseId>>,
) -> Result<(), Error> {
    use records::ValueType;
    match value_type {
        ValueType::Nullable(inner) => hydrate_nested_payload_enum_cases(
            inner,
            introducing_schema,
            &format!("{path}/nullable"),
            output,
        )?,
        ValueType::Array(inner) => hydrate_nested_payload_enum_cases(
            inner,
            introducing_schema,
            &format!("{path}/array"),
            output,
        )?,
        ValueType::Tuple(values) => {
            for (index, value) in values.iter().enumerate() {
                hydrate_nested_payload_enum_cases(
                    value,
                    introducing_schema,
                    &format!("{path}/tuple/{index}"),
                    output,
                )?;
            }
        }
        ValueType::Record(record) => {
            for field in record.fields() {
                let name = field.name.as_deref().ok_or(Error::InvalidStoredValue(
                    "nested enum record field unnamed",
                ))?;
                hydrate_nested_payload_enum_cases(
                    &field.value_type,
                    introducing_schema,
                    &format!("{path}/record/{name}"),
                    output,
                )?;
            }
        }
        ValueType::Enum(schema) => {
            let cases = output.entry(path.to_owned()).or_insert_with(|| {
                schema
                    .cases
                    .iter()
                    .enumerate()
                    .map(|(ordinal, _)| GlobalScalarEnumCaseId {
                        introducing_schema,
                        introducing_ordinal: ordinal as u8,
                    })
                    .collect()
            });
            let cases = cases.clone();
            for (case, layout) in cases.iter().zip(&schema.cases) {
                hydrate_nested_payload_enum_cases(
                    &records::ValueType::Record(Box::new(layout.payload.clone())),
                    introducing_schema,
                    &global_case_path(path, case),
                    output,
                )?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn reconcile_nested_payload_enum_cases(
    value_type: &records::ValueType,
    introducing_schema: SchemaVersionId,
    path: &str,
    output: &mut BTreeMap<String, Vec<GlobalScalarEnumCaseId>>,
) -> Result<(), Error> {
    use records::ValueType;
    match value_type {
        ValueType::Nullable(inner) => reconcile_nested_payload_enum_cases(
            inner,
            introducing_schema,
            &format!("{path}/nullable"),
            output,
        )?,
        ValueType::Array(inner) => reconcile_nested_payload_enum_cases(
            inner,
            introducing_schema,
            &format!("{path}/array"),
            output,
        )?,
        ValueType::Tuple(values) => {
            for (index, value) in values.iter().enumerate() {
                reconcile_nested_payload_enum_cases(
                    value,
                    introducing_schema,
                    &format!("{path}/tuple/{index}"),
                    output,
                )?;
            }
        }
        ValueType::Record(record) => {
            for field in record.fields() {
                let name = field.name.as_deref().ok_or(Error::InvalidStoredValue(
                    "nested enum record field unnamed",
                ))?;
                reconcile_nested_payload_enum_cases(
                    &field.value_type,
                    introducing_schema,
                    &format!("{path}/record/{name}"),
                    output,
                )?;
            }
        }
        ValueType::Enum(schema) => {
            let cases = output.entry(path.to_owned()).or_default();
            if cases.len() > schema.cases.len() {
                return Err(Error::InvalidStoredValue(
                    "nested payload enum registry changed non-additively",
                ));
            }
            for ordinal in cases.len()..schema.cases.len() {
                cases.push(GlobalScalarEnumCaseId {
                    introducing_schema,
                    introducing_ordinal: u8::try_from(ordinal).map_err(|_| {
                        Error::InvalidStoredValue("nested payload enum ordinal exhausted")
                    })?,
                });
            }
            let cases = cases.clone();
            for (case, layout) in cases.iter().zip(&schema.cases) {
                reconcile_nested_payload_enum_cases(
                    &records::ValueType::Record(Box::new(layout.payload.clone())),
                    introducing_schema,
                    &global_case_path(path, case),
                    output,
                )?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn reconcile_nested_scalar_enum_cases(
    value_type: &records::ValueType,
    introducing_schema: SchemaVersionId,
    path: &str,
    output: &mut BTreeMap<String, Vec<GlobalScalarEnumCaseId>>,
) -> Result<(), Error> {
    use records::ValueType;
    match value_type {
        ValueType::EnumTag(schema) => {
            let cases = output.entry(path.to_owned()).or_default();
            if cases.len() > schema.variants.len() {
                return Err(Error::InvalidStoredValue(
                    "nested scalar enum registry changed non-additively",
                ));
            }
            for ordinal in cases.len()..schema.variants.len() {
                cases.push(GlobalScalarEnumCaseId {
                    introducing_schema,
                    introducing_ordinal: u8::try_from(ordinal).map_err(|_| {
                        Error::InvalidStoredValue("nested scalar enum ordinal exhausted")
                    })?,
                });
            }
        }
        ValueType::Nullable(inner) => reconcile_nested_scalar_enum_cases(
            inner,
            introducing_schema,
            &format!("{path}/nullable"),
            output,
        )?,
        ValueType::Array(inner) => reconcile_nested_scalar_enum_cases(
            inner,
            introducing_schema,
            &format!("{path}/array"),
            output,
        )?,
        ValueType::Tuple(values) => {
            for (index, value) in values.iter().enumerate() {
                reconcile_nested_scalar_enum_cases(
                    value,
                    introducing_schema,
                    &format!("{path}/tuple/{index}"),
                    output,
                )?;
            }
        }
        ValueType::Record(record) => {
            for field in record.fields() {
                let name = field.name.as_deref().ok_or(Error::InvalidStoredValue(
                    "nested enum record field unnamed",
                ))?;
                reconcile_nested_scalar_enum_cases(
                    &field.value_type,
                    introducing_schema,
                    &format!("{path}/record/{name}"),
                    output,
                )?;
            }
        }
        ValueType::Enum(schema) => {
            for (ordinal, case) in schema.cases.iter().enumerate() {
                let identity = GlobalScalarEnumCaseId {
                    introducing_schema,
                    introducing_ordinal: ordinal as u8,
                };
                reconcile_nested_scalar_enum_cases(
                    &records::ValueType::Record(Box::new(case.payload.clone())),
                    introducing_schema,
                    &global_case_path(path, &identity),
                    output,
                )?;
            }
        }
        _ => {}
    }
    Ok(())
}

mod branches;
mod catalogue_ingest;
mod codec;
mod currency;
mod database_slot;
mod eviction;
mod global_state;
mod ingest;
pub(crate) mod maintained_subscription_view;
mod open_tx;
mod physical;
mod policy;
pub(crate) mod query_engine;
mod query_eval;
mod recovery;
mod source_resolution;
mod views;
#[cfg(feature = "testing")]
pub(crate) use query_eval::LocalMaintainedViewSubscriptionFootprint;
pub(crate) use query_eval::{
    LocalMaintainedViewSubscription, LocalMaintainedViewSubscriptionUpdate,
};
pub(crate) use views::MaintainedViewBundleInputs;

type ResultRowMembershipKey = crate::tools::OutputOccurrenceId;

use branches::BranchRecord;
use codec::*;
use database_slot::DatabaseSlot;
use open_tx::*;
use physical::*;

pub use eviction::{EdgeCacheBudget, EdgeCacheBudgetReport, EdgeCacheClass, EvictColdReport};

/// Test/bench-only attribution for durable-state work performed while opening a node.
#[cfg(feature = "testing")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NodeOpenReceipt {
    /// Whether startup restored the compact node clock/sequence checkpoint
    /// instead of scanning transaction and physical-history indexes.
    pub clock_checkpoint_loaded: bool,
    /// Catalogue-record decoding and activation preparation.
    pub catalogue_open: Duration,
    /// Groove database construction from recovered physical layouts.
    pub database_open: Duration,
    /// Process-local node allocation and initialization.
    pub state_init: Duration,
    /// Total durable-state recovery time.
    pub recover_storage: Duration,
    /// Alias, branch, clock, and physical-history recovery time.
    pub recover_catalogue_state: Duration,
    /// Retained for receipt compatibility; startup no longer makes this sweep.
    pub validate_current_rows: Duration,
    /// Accepted global-sequence recovery time.
    pub recover_global_sequences: Duration,
    /// Pending-edge and rejected-transaction recovery time.
    pub recover_pending_and_rejected: Duration,
    /// Bounded unclean-close cleanup time.
    pub recover_unclean_close: Duration,
    /// Persisted maintained-query known-state recovery time.
    pub recover_known_state: Duration,
    /// Retained for receipt compatibility; startup no longer rebuilds this index.
    pub rebuild_ahead_current: Duration,
    /// Final catalogue persistence time, when applicable.
    pub finalize_catalogue: Duration,
    /// Current rows decoded by the retired full validation sweep.
    pub validated_current_rows: usize,
    /// Accepted global-sequence entries recovered.
    pub accepted_global_sequences: usize,
    /// Transaction index records inspected for global sequences.
    pub global_sequence_records_scanned: usize,
    /// Physical ahead-current records consumed by the retired startup sweep.
    pub ahead_current_entries: usize,
}

#[cfg(test)]
mod tests;

/// Default client-clock skew tolerance in milliseconds.
pub const SKEW_TOLERANCE_MS: u64 = 30_000;
const TX_VERSION_TABLE_CACHE_MAX_ENTRIES: usize = 4096;

static NEXT_GROOVE_RUNTIME_TOKEN: AtomicU64 = AtomicU64::new(1);

fn next_groove_runtime_token() -> u64 {
    NEXT_GROOVE_RUNTIME_TOKEN.fetch_add(1, Ordering::Relaxed)
}

#[cfg(test)]
std::thread_local! {
    static QUERY_VERSIONS_FOR_TX_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static SUBSCRIPTION_SNAPSHOT_FOR_LINK_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(super) fn reset_query_versions_for_tx_call_count() {
    QUERY_VERSIONS_FOR_TX_CALLS.with(|calls| calls.set(0));
}

#[cfg(test)]
pub(super) fn query_versions_for_tx_call_count() -> usize {
    QUERY_VERSIONS_FOR_TX_CALLS.with(std::cell::Cell::get)
}

#[cfg(test)]
fn record_query_versions_for_tx_call() {
    QUERY_VERSIONS_FOR_TX_CALLS.with(|calls| calls.set(calls.get() + 1));
}

#[cfg(test)]
fn record_subscription_snapshot_for_link_call() {
    SUBSCRIPTION_SNAPSHOT_FOR_LINK_CALLS.with(|calls| calls.set(calls.get() + 1));
}

fn record_maintained_view_stream_b_add_bundle() {}

fn record_maintained_view_removal_stream_bundle() {}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum LensPathDirection {
    Forward,
    Reverse,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct LensPathCacheKey {
    source: SchemaVersionId,
    target: SchemaVersionId,
    direction: LensPathDirection,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct CompiledLensCacheKey {
    source: SchemaVersionId,
    target: SchemaVersionId,
    direction: LensPathDirection,
    table: String,
}

#[derive(Clone, Debug)]
pub(super) struct CompiledLensPath {
    target_table: String,
    ops: Vec<CompiledLensOp>,
}

#[derive(Clone, Debug)]
enum CompiledLensOp {
    Rename { from: String, to: String },
    Copy { from: String, to: String },
    Add { column: String, default: Value },
    Drop { column: String },
}

/// Storage-backed Jazz node: mergeable history, local reads, and commit-unit sync.
pub struct NodeState<S> {
    /// Stable UUID identifying this node across storage reopen.
    node_uuid: NodeUuid,
    /// Compact alias assigned to this node for on-disk transaction keys.
    self_node_alias: Option<NodeAlias>,
    /// Schema catalogue, migration lenses, and logical-to-physical mappings.
    catalogue: SchemaCatalogue,
    /// Whether this runtime has an authoritative catalogue lineage that may
    /// safely describe application data.  A dynamic edge starts
    /// `Uninitialized`: its temporary system-only runtime schema is never a
    /// database genesis and no query or write may use it.  The first trusted
    /// catalogue snapshot installs the authority's exact genesis together
    /// with its mappings and write pointer in one durable batch.
    catalogue_bootstrap_state: CatalogueBootstrapState,
    /// Whether this durable catalogue was installed through the dynamic-edge
    /// bootstrap snapshot boundary and therefore carries a completion record
    /// that must be refreshed with later trusted snapshots.
    catalogue_bootstrap_marker: bool,
    /// In-memory branch records and branch-specific storage partitions.
    branches: Branches,
    /// Local logical time and global-application progress counters.
    clock: Clock,
    /// Commit-unit and shape-registration payloads waiting for missing context.
    parking: Parking,
    /// Query registration, binding, cache, graph, and settled-result state.
    query: QueryServing,
    /// Locally opened transactions and authoring attribution state.
    open_tx: OpenTxState,
    /// Rejected transaction records and pending-cascade parent/child indexes.
    rejections: RejectionTracking,
    /// Groove database slot over this node's storage.
    database: DatabaseSlot<S>,
    /// Exact Groove batches emitted at the node persistence seam. This queue is
    /// enabled only while an outer runtime owns asynchronous durability; the
    /// evaluator and commit path are otherwise identical.
    /// Process-local identity for runtime-local Groove handles such as prepared shape ids.
    groove_runtime_token: u64,
    /// Whether this node has complete settled history for historical reads.
    history_complete: bool,
    /// Durability recorded for commits authored by this process.
    ///
    /// Ordinary storage-backed nodes author at `Local`. A browser main-thread
    /// runtime uses `None` because its in-memory preview is not durable until
    /// the dedicated worker acknowledges persistence.
    authored_commit_durability: DurabilityTier,
    /// Least durability guaranteed by this node's immediate upstream. This is
    /// topology, not a property of local storage: a volatile browser tab has a
    /// Local worker upstream, while a volatile direct client has a Global
    /// server upstream.
    upstream_durability_floor: DurabilityTier,
    /// Mapping from stable node UUIDs to compact on-disk aliases.
    pub(crate) node_aliases: BTreeMap<NodeUuid, NodeAlias>,
    /// Runtime counters for sync parking, draining, and ingestion behavior.
    sync_metrics: SyncMetrics,
    /// Runtime counters for query-engine read authorization paths.
    query_engine_read_metrics: QueryEngineReadMetrics,
    /// Process-local claims attached to authenticated subscriber sessions.
    session_claims: BTreeMap<AuthorId, BTreeMap<String, Value>>,
    /// Monotone revision for each identity's process-local session claims.
    session_claim_revisions: BTreeMap<AuthorId, u64>,
    /// Whether this authority has installed the permissions head that governs
    /// session-scoped reads and writes.
    permissions_ready: bool,
    /// A staged catalogue bundle failed after durable admission. The process
    /// must reopen and deterministically resume activation before serving more
    /// protocol traffic.
    catalogue_activation_failed: bool,
    #[cfg(any(test, feature = "testing"))]
    catalogue_activation_failpoint: Option<CatalogueActivationFailpoint>,
    /// Client-only write cadence selected for the first snapshot hydration.
    initial_sync_flush_cadence: Option<usize>,
    /// Whether the first snapshot is currently using the configured cadence.
    initial_sync_flush_active: bool,
    /// Once the initial snapshot has completed, ordinary writes return to their
    /// existing per-write durability boundaries.
    initial_sync_flush_completed: bool,
}

/// Schema catalogue and schema-version storage layout known by the node.
#[derive(Clone, Debug)]
struct SchemaCatalogue {
    /// Schema version used for the node's base/local API schema.
    current_schema_version_id: SchemaVersionId,
    /// Compact alias for `current_schema_version_id` once recovered or allocated.
    current_schema_version_alias: Option<SchemaVersionAlias>,
    /// Base schema supplied when the node was opened.
    schema: JazzSchema,
    /// Mapping from schema version IDs to compact on-disk aliases.
    schema_version_aliases: BTreeMap<SchemaVersionId, SchemaVersionAlias>,
    /// Catalogue entries for all schema versions known to this node.
    catalogue_schemas: BTreeMap<SchemaVersionId, SchemaVersion>,
    /// Catalogue entries for migration lenses known to this node.
    catalogue_lenses: BTreeMap<MigrationLensId, MigrationLens>,
    /// Resolved logical-to-physical identity mapping for every known schema.
    physical_mappings: BTreeMap<SchemaVersionId, SchemaPhysicalMapping>,
    /// Durable, not-yet-visible schema bundles awaiting ordered activation.
    staged_lineages: BTreeMap<u64, StagedSchemaLineage>,
    /// Ordered bundle payloads waiting for an earlier sequence or active source.
    pending_lineages: BTreeMap<u64, PendingSchemaLineage>,
    /// Canonical bundle that first activated each non-genesis target schema.
    active_lineages_by_target: BTreeMap<SchemaVersionId, StagedSchemaLineage>,
    /// Highest contiguously activated schema catalogue position.
    active_catalogue_seq: u64,
    /// Durable write-pointer updates waiting for their schema to become Active.
    pending_write_pointers: BTreeMap<u64, CurrentWriteSchema>,
    /// Next database-local physical table id.
    next_physical_table_id: u64,
    /// Next database-local physical column id.
    next_physical_column_id: u64,
    /// Shortest migration-lens paths by schema pair and traversal direction.
    lens_path_cache: BTreeMap<LensPathCacheKey, Option<Vec<MigrationLensId>>>,
    /// Table-specific, already-validated lens programs used by hot read/write paths.
    compiled_lens_cache: BTreeMap<CompiledLensCacheKey, Option<CompiledLensPath>>,
    /// Schema version currently used for newly authored writes.
    current_write_schema: CurrentWriteSchema,
}

/// Readiness of a dynamically catalogued node.
///
/// This is deliberately separate from the catalogue's current-write pointer.
/// A pointer is meaningful only after a durable authority lineage exists;
/// treating an empty constructor schema as that lineage manufactures a false
/// genesis on an edge that has not yet heard from its core.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CatalogueBootstrapState {
    /// No authority catalogue has been installed.  Application state must
    /// fail closed until [`NodeState::apply_trusted_catalogue_snapshot`] is
    /// called on the authenticated upstream path.
    Uninitialized,
    /// A durable genesis, mappings, and pointer have been installed.
    Ready,
}

/// Branch metadata and branch-partition layout known by the node.
#[derive(Clone, Debug, Default)]
struct Branches {
    /// In-memory branch records indexed by branch ID.
    branches: BTreeMap<BranchId, BranchRecord>,
    /// Storage partitions materialized for physical-table/branch pairs.
    branch_partitions: BTreeSet<(PhysicalTableId, BranchId)>,
    /// Locally-authored metadata awaiting an upstream acknowledgement.
    pending_metadata_uploads: BTreeSet<BranchId>,
}

/// Local transaction clock and settled-global application progress.
#[derive(Clone, Debug)]
struct Clock {
    /// Highest local transaction timestamp observed or minted by this node.
    tx_time: TxTime,
    /// Next global sequence number to allocate when accepting local work globally.
    next_global_seq: GlobalSeq,
    /// Whether the maximum global sequence has already been allocated or recovered.
    global_seq_exhausted: bool,
    /// Contiguous global sequence watermark already applied to local storage.
    applied_global_watermark: GlobalSeq,
    /// Applied global sequence numbers above the contiguous watermark.
    applied_global_above_watermark: BTreeSet<GlobalSeq>,
}

impl Clock {
    fn allocate_global_seq(&mut self) -> Result<GlobalSeq, Error> {
        if self.global_seq_exhausted {
            return Err(Error::InvalidStoredValue("global sequence exhausted"));
        }
        let global_seq = self.next_global_seq;
        if global_seq == GlobalSeq(u64::MAX) {
            self.global_seq_exhausted = true;
        } else {
            self.next_global_seq = global_seq.next();
        }
        Ok(global_seq)
    }

    /// Record one accepted authority sequence and return every newly
    /// contiguous sequence above the prior watermark.
    ///
    /// Fate preparation applies this to a cloned clock. The real clock is
    /// replaced only after the prepared storage batch publishes, so a cold
    /// read or failed preparation cannot advance observable authority state.
    fn record_applied_global_seq(&mut self, global_seq: GlobalSeq) -> Vec<GlobalSeq> {
        if global_seq == GlobalSeq(u64::MAX) {
            self.next_global_seq = global_seq;
            self.global_seq_exhausted = true;
        } else if !self.global_seq_exhausted {
            self.next_global_seq = self.next_global_seq.max(global_seq.next());
        }
        if global_seq <= self.applied_global_watermark {
            return Vec::new();
        }
        self.applied_global_above_watermark.insert(global_seq);
        let mut advanced = Vec::new();
        while let Some(next) = self
            .applied_global_watermark
            .0
            .checked_add(1)
            .map(GlobalSeq)
            && self.applied_global_above_watermark.remove(&next)
        {
            self.applied_global_watermark = next;
            advanced.push(next);
        }
        advanced
    }
}

/// Payloads parked until missing schema or catalogue context arrives.
#[derive(Clone, Debug, Default)]
struct Parking {
    /// Shape registrations waiting for an unknown schema version.
    parked_shape_registrations: BTreeMap<ShapeId, ShapeAst>,
    /// Subscription attaches waiting for their shape registration to become installable.
    parked_binding_deltas: BTreeMap<ShapeId, Vec<Subscribe>>,
    /// Commit units waiting for parent transactions or schema context.
    parked_commit_units: BTreeMap<TxId, ParkedCommitUnit>,
    /// Catalogue commit units waiting to be applied in dependency order.
    parked_catalogue_commit_units: BTreeSet<TxId>,
}

/// Query registration, cache, current-row graph, and settled-result state.
#[derive(Clone, Debug, Default)]
struct QueryServing {
    /// Prepared query plans keyed by shape, durability tier, and parameter
    /// descriptor signature.
    query_shape_cache:
        BTreeMap<(crate::query::ShapeId, DurabilityTier, String), PreparedQueryPlanHandle>,
    /// Derived read-policy authorization requests keyed by policy context.
    read_policy_authorization_request_cache:
        BTreeMap<ReadPolicyAuthorizationRequestCacheKey, query_engine::QueryProgramRequest>,
    /// Lowered authorization row-id graphs keyed by their full query-engine request.
    policy_authorization_graph_cache: BTreeMap<String, query_eval::PolicyAuthorizationGraph>,
    /// Policy tables currently being compiled as membership proofs. This is
    /// transient recursion state, not a cache.
    policy_proof_stack: Vec<String>,
    /// Logical tables that have history rows for a stored transaction.
    tx_version_tables_cache: BTreeMap<TxId, BTreeSet<String>>,
    /// Recently staged history rows for a stored transaction.
    tx_versions_cache: BTreeMap<TxId, Vec<VersionRow>>,
    /// Approximate insertion order for bounding `tx_version_tables_cache`.
    tx_version_tables_cache_order: VecDeque<TxId>,
    /// Live membership for `tx_version_tables_cache_order`.
    tx_version_tables_cache_order_set: BTreeSet<TxId>,
    /// Physical version-storage sources keyed by logical table and layer.
    version_storage_sources_cache: BTreeMap<(String, VersionLayer), Vec<String>>,
    /// Registered validated query shapes keyed by stable shape ID.
    registered_shapes: BTreeMap<ShapeId, ValidatedQuery>,
    /// Registered query binding values keyed by shape and usage-site binding ID.
    registered_bindings: BTreeMap<ShapeId, BTreeMap<BindingId, RegisteredBinding>>,
    /// Monotonically increasing receiver receipts for applied authoritative
    /// updates. Attachments capture the current receipt and require a later
    /// one; this remains logical binding-view state, never a wire nonce.
    applied_view_update_generations: BTreeMap<BindingViewKey, u64>,
    /// Subscriber-side settled result-member/completeness state by canonical query binding/view.
    settled_result_sets: BTreeMap<BindingViewKey, BTreeSet<ResultMemberEntry>>,
    /// Point index for settled real-row output occurrences.
    ///
    /// This mirrors the row-shaped subset of `settled_result_sets` so applying a
    /// new current winner can remove the previous winner without scanning the
    /// full result set.
    settled_result_row_index:
        BTreeMap<BindingViewKey, BTreeMap<ResultRowMembershipKey, ResultMemberEntry>>,
    /// Subscriber-side settled non-row facts by canonical query binding/view.
    settled_program_facts: BTreeMap<BindingViewKey, BTreeSet<ViewFactEntry>>,
    /// Durable binding views admitted into the process-local settled caches.
    known_state_loaded_binding_views: BTreeSet<BindingViewKey>,
    /// Server-stamped settled-through cursor for each canonical binding view.
    settled_through_by_binding_view: BTreeMap<BindingViewKey, GlobalSeq>,
    /// Server-stamped authorization generation paired with settled fast state.
    authorization_progress_by_binding_view: BTreeMap<BindingViewKey, u64>,
    /// Binding views whose current subscription declared known-state repair.
    known_state_declared_binding_views: BTreeSet<BindingViewKey>,
    /// Binding views that have begun receiving an initial snapshot. Some
    /// snapshot payloads arrive after an empty reset stamp, and every payload
    /// in that phase is eligible for complete-bundle bulk ingest.
    initial_hydration_binding_views: BTreeSet<BindingViewKey>,
    /// Binding views that are currently receiving a chunked update sequence.
    ///
    /// Intermediate chunks apply storage and settled-result state, but they do
    /// not define an observation boundary for local maintained subscribers.
    /// Publication runs when the final chunk clears this marker.
    deferred_publication_binding_views: BTreeSet<BindingViewKey>,
    /// Binding views whose settled state was replaced by an authoritative
    /// server-provided reset since the last facade refresh.
    pending_authoritative_reset_binding_views: BTreeSet<BindingViewKey>,
    pending_opening_binding_views: BTreeSet<BindingViewKey>,
    /// FIFO terminal edits received from the serving peer and not yet
    /// published by the local subscription facade.
    pending_terminal_operations_by_binding_view:
        BTreeMap<BindingViewKey, Vec<groove::ivm::TerminalOperation>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum ParamBindingModeCacheKey {
    InlineAllReachableSeeds,
    RetainAllParams,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct ReadPolicyAuthorizationRequestCacheKey {
    policy_schema_version: SchemaVersionId,
    table_name: String,
    identity: AuthorId,
    param_binding_mode: ParamBindingModeCacheKey,
    tier: DurabilityTier,
    binding_source_shape: Option<String>,
    binding_user_params: String,
    binding_claim_params: String,
    include_deleted_root: bool,
}

/// One usage-site query binding registration.
#[derive(Clone, Debug, PartialEq)]
struct RegisteredBinding {
    values: Vec<Value>,
    read_view: ReadViewKey,
    binding_view_key: BindingViewKey,
}

/// Locally open transactions and local-only permission attribution.
struct OpenTxState {
    /// Open transaction handles keyed by caller-generated identity.
    open_transactions: BTreeMap<OpenBatchId, OpenTransaction>,
    /// Identities consumed by commit or rollback; never reusable in this runtime.
    closed_batches: BTreeSet<OpenBatchId>,
    /// Local-only permission subjects for transactions whose `made_by` keeps provenance.
    local_permission_subjects: BTreeMap<TxId, AuthorId>,
}

/// Fully read-preflighted mergeable publication. Constructing this value may
/// demand-load storage; applying it is a no-suspension resident mutation.
pub(crate) struct PreparedMergeableCommit {
    tx_id: TxId,
    batch: groove::db::PreparedDatabaseBatch,
    stored_versions: Vec<VersionRow>,
    made_by: AuthorId,
    permission_subject: AuthorId,
}

/// One branch transaction whose lazy physical partitions and row batch have
/// both been validated without changing the live runtime.
pub(crate) struct PreparedBranchMergeableCommit {
    tx_id: TxId,
    registrations: Vec<groove::db::PreparedTableRegistration>,
    partitions: BTreeSet<(PhysicalTableId, BranchId)>,
    batch: groove::db::PreparedDatabaseBatch,
    transaction_tables: BTreeSet<String>,
}

pub(crate) struct PreparedOpenMergeableCommit {
    open_batch_id: OpenBatchId,
    commit: PreparedMergeableCommit,
}

pub(crate) struct PreparedExclusiveCommit {
    open_batch_id: OpenBatchId,
    tx: Transaction,
    versions: Vec<VersionRecord>,
}

/// Durable settled-view state decoded without changing the live query runtime.
///
/// Async ingress prepares these values before publishing any canonical rows or
/// maintained-view state, so a cold prefix read cannot strand a half-applied
/// receiver frame.
pub(crate) struct PreparedKnownStateFact {
    binding_view_key: BindingViewKey,
    settled_through: Option<GlobalSeq>,
    authorization_progress: Option<u64>,
    members: Vec<ResultMemberEntry>,
    facts: Vec<ViewFactEntry>,
}

/// Sparse branch tables and durable partition markers required by a receiver
/// batch, validated against a prospective schema but not yet made live.
pub(crate) struct PreparedViewUpdateBranchPartitions {
    registrations: Vec<groove::db::PreparedTableRegistration>,
    partitions: BTreeSet<(PhysicalTableId, BranchId)>,
    batch: Option<groove::db::PreparedDatabaseBatch>,
}

/// One receiver batch whose persisted settled state and prospective sparse
/// schema have been acquired before any live receiver mutation.
pub(crate) struct PreparedViewUpdateBatch {
    updates: Vec<ViewUpdateParts>,
    known_states: Vec<PreparedKnownStateFact>,
    branch_partitions: PreparedViewUpdateBranchPartitions,
}

pub(crate) struct PreparedRepairPayloadIngress {
    bundles: Vec<VersionBundle>,
}

/// Rejection records and derived indexes used for pending-cascade handling.
#[derive(Clone, Debug, Default)]
struct RejectionTracking {
    /// Transactions rejected by local policy or conflict checks.
    rejected_transactions: BTreeMap<TxId, RejectedTransaction>,
    /// Pending child transactions grouped by pending parent transaction.
    child_txs_by_parent: BTreeMap<TxId, BTreeSet<TxId>>,
    /// Parents whose durable pending-edge prefix has been admitted into the
    /// in-memory cascade index. Absence means "not loaded", not "no children".
    loaded_child_edges: BTreeSet<TxId>,
}

/// Authenticated identity attached to an inbound commit-unit upload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommitUnitIngestContext {
    /// Identity authenticated by the connection carrying the upload.
    pub identity: AuthorId,
    /// Whether the connection may attribute writes to a different `made_by`.
    pub trust: CommitUnitTrust,
    /// Whether this subscriber link is hosted by an edge authority.
    pub edge_authority: bool,
}

/// Trust mode for an inbound commit-unit upload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitUnitTrust {
    /// Session/client links must honestly set `made_by` to the link identity.
    Session,
    /// Trusted backends may preserve user provenance in `made_by`.
    TrustedBackend,
}

include!("state/async_runtime.rs");
include!("state/lifecycle.rs");
include!("state/commit.rs");
include!("state/durable.rs");
include!("state/catalogue.rs");
include!("state/read_payload.rs");

pub(super) fn apply_compiled_lens_path(
    path: &CompiledLensPath,
    cells: &mut BTreeMap<String, Value>,
) -> String {
    for op in &path.ops {
        match op {
            CompiledLensOp::Rename { from, to } => {
                if let Some(value) = cells.remove(from) {
                    cells.insert(to.clone(), value);
                }
            }
            CompiledLensOp::Copy { from, to } => {
                if let Some(value) = cells.get(from).cloned() {
                    cells.insert(to.clone(), value);
                }
            }
            CompiledLensOp::Add { column, default } => {
                cells
                    .entry(column.clone())
                    .or_insert_with(|| default.clone());
            }
            CompiledLensOp::Drop { column } => {
                cells.remove(column);
            }
        }
    }
    path.target_table.clone()
}

fn push_compiled_forward_lens_op(
    op: &LensOp,
    compiled: &mut Vec<CompiledLensOp>,
) -> Result<(), Error> {
    match op {
        LensOp::RenameTable { .. } => {}
        LensOp::RenameColumn { from, to } => {
            compiled.push(CompiledLensOp::Rename {
                from: from.clone(),
                to: to.clone(),
            });
        }
        LensOp::CopyColumn { from, to } => {
            compiled.push(CompiledLensOp::Copy {
                from: from.clone(),
                to: to.clone(),
            });
        }
        LensOp::AddColumn { column, default } => {
            compiled.push(CompiledLensOp::Add {
                column: column.clone(),
                default: default.clone(),
            });
        }
        LensOp::DropColumn { column, .. } => {
            compiled.push(CompiledLensOp::Drop {
                column: column.clone(),
            });
        }
        LensOp::TransformColumn { transform, .. } => {
            validate_registered_transform(transform)?;
        }
        LensOp::RejectSourceDelta { .. } => {
            return Err(Error::InvalidCatalogueUpdate(
                "lens op is not naturally mappable",
            ));
        }
    }
    Ok(())
}

fn push_compiled_reverse_lens_op(
    op: &LensOp,
    compiled: &mut Vec<CompiledLensOp>,
) -> Result<(), Error> {
    match op {
        LensOp::RenameTable { .. } => {}
        LensOp::RenameColumn { from, to } => {
            compiled.push(CompiledLensOp::Rename {
                from: to.clone(),
                to: from.clone(),
            });
        }
        LensOp::CopyColumn { to, .. } => {
            compiled.push(CompiledLensOp::Drop { column: to.clone() });
        }
        LensOp::AddColumn { column, .. } => {
            compiled.push(CompiledLensOp::Drop {
                column: column.clone(),
            });
        }
        LensOp::DropColumn {
            column,
            backwards_default,
        } => {
            compiled.push(CompiledLensOp::Add {
                column: column.clone(),
                default: backwards_default.clone(),
            });
        }
        LensOp::TransformColumn { transform, .. } => {
            validate_registered_transform(transform)?;
        }
        LensOp::RejectSourceDelta { .. } => {
            return Err(Error::InvalidCatalogueUpdate(
                "lens op is not naturally mappable",
            ));
        }
    }
    Ok(())
}

pub(super) fn validate_registered_transform(transform: &str) -> Result<(), Error> {
    let Some(semantics) = registered_column_transform(transform) else {
        return Err(Error::InvalidCatalogueUpdate(
            "transform column is not registered",
        ));
    };
    if !semantics.bijective || !semantics.canonical_equality_preserving {
        return Err(Error::InvalidCatalogueUpdate(
            "transform column is not bijective and canonical-preserving",
        ));
    }
    Ok(())
}

/// Current-row result backed by an encoded projected record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CurrentRow {
    table: groove::Intern<String>,
    record: std::sync::Arc<OwnedRecord>,
    deleted: bool,
}

/// Work performed by the durable local-write replay lookup.
///
/// Kept separate from storage metrics so scale tests can assert candidate
/// records independently of storage-window and cache details.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct PendingTransactionScan {
    tx_ids: Vec<TxId>,
    records_visited: usize,
    full_transactions_decoded: usize,
}

/// User-visible row provenance resolved from commit authorship.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RowProvenance {
    /// Principal that created the row.
    pub created_by: AuthorId,
    /// Commit time of the row's first retained content version.
    pub created_at: TxTime,
    /// Principal that authored the visible row version.
    pub updated_by: AuthorId,
    /// Commit time of the visible row version.
    pub updated_at: TxTime,
}

/// Directed relation edge emitted for an array-subquery payload.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RelationEdge {
    /// Source row table.
    pub source_table: String,
    /// Source row id.
    pub source_row: RowUuid,
    /// Relation/output column name.
    pub relation: String,
    /// Target row table.
    pub target_table: String,
    /// Target row id.
    pub target_row: RowUuid,
}

/// One-shot relation read payload: row material plus array-subquery edges.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RelationSnapshot {
    /// Number of leading `rows` entries that are query roots.
    pub root_count: usize,
    /// Root and related rows referenced by `edges`.
    pub rows: Vec<CurrentRow>,
    /// Relation edges between rows.
    pub edges: Vec<RelationEdge>,
}

impl CurrentRow {
    /// Construct a current row from an encoded projection record.
    pub(crate) fn new(table: impl Into<String>, record: OwnedRecord) -> Self {
        Self {
            table: groove::Intern::new(table.into()),
            record: std::sync::Arc::new(record),
            deleted: false,
        }
    }

    pub(crate) fn into_deleted(mut self) -> Self {
        self.deleted = true;
        self
    }

    /// Whether this row was returned as a current deleted row by an opt-in read.
    pub fn is_deleted(&self) -> bool {
        self.deleted
    }

    /// Logical table name.
    pub fn table(&self) -> &str {
        self.table.as_str()
    }

    /// Row id.
    pub fn row_uuid(&self) -> RowUuid {
        RowUuid(
            self.record
                .borrowed()
                .get_uuid(CurrentRowRecord::FIELD_ROW_UUID_IDX)
                .expect("valid current row_uuid"),
        )
    }

    /// Cell value by application-schema column position.
    pub fn cell_at(&self, column_position: usize) -> Option<Value> {
        match self
            .record
            .borrowed()
            .get_idx(CurrentRowRecord::USER_CELLS + column_position)
            .expect("valid current user cell")
        {
            Value::Nullable(None) => None,
            Value::Nullable(Some(value)) => Some(*value),
            value => Some(value),
        }
    }

    /// Cell value by application column name using the table schema to resolve position.
    pub fn cell(&self, table: &TableSchema, column: &str) -> Option<Value> {
        let _ = table
            .columns
            .iter()
            .find(|candidate| candidate.name == column)?;
        let user_name = user_column_field(column);
        let idx = self.record.descriptor().fields().iter().position(|field| {
            field.name.as_deref() == Some(user_name.as_str())
                || field.name.as_deref() == Some(column)
        })?;
        match self.record.borrowed().get_idx(idx).ok()? {
            Value::Nullable(None) => None,
            Value::Nullable(Some(value)) => Some(*value),
            value => Some(value),
        }
    }

    /// Encoded groove record backing this projected current row.
    pub fn encoded_record(&self) -> (&records::RecordDescriptor, &[u8]) {
        (self.record.descriptor(), self.record.raw())
    }

    pub(crate) fn raw_field(&self, field: &str) -> Option<Value> {
        let idx = self.record.descriptor().field_index(field)?;
        self.record.borrowed().get_idx(idx).ok()
    }

    pub(crate) fn provenance(&self) -> Result<Option<RowProvenance>, Error> {
        let descriptor = self.record.descriptor();
        let borrowed = self.record.borrowed();
        let Some(created_by_idx) = descriptor.field_index("$createdBy") else {
            return Ok(None);
        };
        let Some(created_at_idx) = descriptor.field_index("$createdAt") else {
            return Ok(None);
        };
        let Some(updated_by_idx) = descriptor.field_index("$updatedBy") else {
            return Ok(None);
        };
        let Some(updated_at_idx) = descriptor.field_index("$updatedAt") else {
            return Ok(None);
        };
        Ok(Some(RowProvenance {
            created_by: AuthorId(borrowed.get_uuid(created_by_idx)?),
            created_at: TxTime(borrowed.get_u64(created_at_idx)?),
            updated_by: AuthorId(borrowed.get_uuid(updated_by_idx)?),
            updated_at: TxTime(borrowed.get_u64(updated_at_idx)?),
        }))
    }

    pub(crate) fn project(&self, table: &TableSchema, columns: &[String]) -> Result<Self, Error> {
        let selected = columns.iter().map(String::as_str).collect::<BTreeSet<_>>();
        let projected_columns = table
            .columns
            .iter()
            .filter(|column| selected.contains(column.name.as_str()))
            .collect::<Vec<_>>();
        let descriptor = records::RecordDescriptor::new(
            std::iter::once(("row_uuid".to_owned(), records::ValueType::Uuid))
                .chain(projected_columns.iter().map(|column| {
                    (
                        user_column_field(&column.name),
                        records::ValueType::Nullable(Box::new(column.column_type.clone())),
                    )
                }))
                .chain([
                    ("$createdBy".to_owned(), records::ValueType::Uuid),
                    ("$createdAt".to_owned(), records::ValueType::U64),
                    ("$updatedBy".to_owned(), records::ValueType::Uuid),
                    ("$updatedAt".to_owned(), records::ValueType::U64),
                    ("tx_time".to_owned(), records::ValueType::U64),
                    ("tx_node_id".to_owned(), records::ValueType::U64),
                ]),
        );
        let mut values = vec![Value::Uuid(self.row_uuid().0)];
        for column in projected_columns {
            let cell = self.cell(table, &column.name);
            let projected = if matches!(column.column_type, records::ValueType::Nullable(_)) {
                match cell {
                    Some(value @ Value::Nullable(_)) => Value::Nullable(Some(Box::new(value))),
                    Some(value) => {
                        Value::Nullable(Some(Box::new(Value::Nullable(Some(Box::new(value))))))
                    }
                    None => Value::Nullable(None),
                }
            } else {
                Value::Nullable(cell.map(Box::new))
            };
            values.push(projected);
        }
        if let Some(provenance) = self.provenance()? {
            values.push(Value::Uuid(provenance.created_by.0));
            values.push(Value::U64(provenance.created_at.0));
            values.push(Value::Uuid(provenance.updated_by.0));
            values.push(Value::U64(provenance.updated_at.0));
        } else {
            values.push(Value::Uuid(AuthorId::SYSTEM.0));
            values.push(Value::U64(0));
            values.push(Value::Uuid(AuthorId::SYSTEM.0));
            values.push(Value::U64(0));
        }
        if let Some((time, node)) = self.projected_tx_alias() {
            values.push(Value::U64(time.0));
            values.push(Value::U64(node.0));
        } else {
            values.push(Value::U64(0));
            values.push(Value::U64(0));
        }
        let raw = descriptor.create(&values)?;
        Ok(Self::new(
            table.name.clone(),
            OwnedRecord::new(raw, descriptor),
        ))
    }

    pub(crate) fn projected_tx_alias(&self) -> Option<(TxTime, NodeAlias)> {
        // Located by name: graph outputs may project additional fields (e.g.
        // binding params) after the tx columns, so position is not stable.
        let fields = self.record.descriptor().fields();
        let stamp_idx = fields
            .iter()
            .position(|field| field.name.as_deref() == Some("tx_time"))?;
        let alias_idx = stamp_idx + 1;
        if fields.get(alias_idx)?.name.as_deref() != Some("tx_node_id") {
            return None;
        }
        let borrowed = self.record.borrowed();
        let time = borrowed.get_u64(stamp_idx).ok()?;
        let alias = borrowed.get_u64(alias_idx).ok()?;
        if time == 0 && alias == 0 {
            return None;
        }
        Some((TxTime(time), NodeAlias(alias)))
    }

    #[cfg(test)]
    pub(crate) fn test_cells_by_descriptor(&self) -> BTreeMap<String, Value> {
        self.record
            .descriptor()
            .fields()
            .iter()
            .enumerate()
            .skip(CurrentRowRecord::USER_CELLS)
            .filter_map(|(idx, field)| {
                let name = field.name.as_ref()?.as_str();
                let name = if name.starts_with("user_") {
                    let name = self::query_engine::logical_user_column(name);
                    self::query_engine::aggregate_output_logical_name(name)
                        .unwrap_or(name)
                        .to_owned()
                } else if let Some(name) = self::query_engine::aggregate_output_logical_name(name) {
                    name.to_owned()
                } else if matches!(field.value_type, records::ValueType::Nullable(_))
                    && !matches!(name, "authored_columns" | "settle_position")
                {
                    name.to_owned()
                } else {
                    return None;
                };
                let value = match self.record.borrowed().get_idx(idx).ok()? {
                    Value::Nullable(value) => value.map(|value| *value)?,
                    value => value,
                };
                Some((name, value))
            })
            .collect()
    }
}

#[cfg(test)]
impl PartialEq<(RowUuid, BTreeMap<String, Value>)> for CurrentRow {
    fn eq(&self, other: &(RowUuid, BTreeMap<String, Value>)) -> bool {
        self.row_uuid() == other.0 && self.test_cells_by_descriptor() == other.1
    }
}

/// Cheap read-only handle for historical settled-state reads.
pub struct HistoricalRead<'node, S>
where
    S: ResidentStorage,
{
    node: &'node mut NodeState<S>,
    position: GlobalSeq,
}

impl<S> HistoricalRead<'_, S>
where
    S: ResidentStorage,
{
    /// Global settle position this handle reads at.
    pub fn position(&self) -> GlobalSeq {
        self.position
    }
}

/// Deterministic counters for storage-backed sync ingestion.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SyncMetrics {
    /// Commit units parked because parents were missing.
    pub parked_orphans: u64,
    /// Parked commit units later resolved.
    pub parked_orphans_resolved: u64,
    /// Commit units parked because row schema versions were missing from the catalogue.
    pub parked_catalogue_orphans: u64,
    /// Catalogue-orphan commit units later resolved.
    pub parked_catalogue_orphans_resolved: u64,
    /// Relay-only commit units discarded after an unknown authored schema became
    /// known and proved their row record incomplete for that schema. A relay
    /// cannot assign a fate, so these are deliberately dropped rather than
    /// stored as a synthetic rejected transaction.
    pub dropped_malformed_relay_commit_units: u64,
    /// Shape registrations parked because their schema version was missing.
    pub parked_catalogue_shapes: u64,
    /// Parked shape registrations later resolved by catalogue arrival.
    pub parked_catalogue_shapes_resolved: u64,
    /// Per-subscription messages dropped because the subscription is no longer registered locally.
    pub dropped_detached_subscription_messages: u64,
    /// Remote peer requests dropped at the sync-driver boundary without killing the local driver.
    pub dropped_peer_request_messages: u64,
    /// Transport sends retried after local backpressure instead of killing the sync driver.
    pub transport_backpressure_retries: u64,
    /// View-update bundles ingested through a receiver-level shared storage batch.
    pub receiver_bulk_bundle_ingests: u64,
    /// View-update bundles that still required the per-bundle ingest path.
    pub receiver_per_bundle_ingests: u64,
    /// Receiver-level shared ingest batches committed.
    pub receiver_bulk_ingest_commits: u64,
    /// Authoritative reset callback materialization fell back because the
    /// reset referenced a version not yet available in this receiver.
    pub authoritative_reset_missing_payload_fallbacks: u64,
    /// Receiver ignored a peer complete-transaction inventory claim because the
    /// transaction was not yet available on this link.
    pub peer_payload_inventory_missing_fallbacks: u64,
}

/// Deterministic counters for query-engine read authorization.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QueryEngineReadMetrics {
    /// Query-engine authorization terminal graphs constructed for read visibility.
    pub policy_authorization_graphs: u64,
    /// Source graphs filtered by query-engine authorization terminals.
    pub policy_authorized_source_joins: u64,
    /// Visible-current source resolutions that used a static primary-key scan.
    pub source_primary_key_scans: u64,
    /// Visible-current source resolutions that used a declared secondary index.
    pub source_index_probes: u64,
    /// Historical/branch-base source resolutions that used a bounded global-sequence range.
    pub source_global_seq_range_scans: u64,
    /// Visible-current source resolutions that fell back to a full source scan.
    pub source_full_scans: u64,
}

/// Wall-clock attribution for one synchronous query materialization.
///
/// This is intended for benchmark diagnostics. The phases cover work inside
/// the node query path; facade borrowing and error conversion remain outside
/// `total`.
#[derive(Clone, Copy, Debug, Default)]
pub struct QueryReadProfile {
    /// Resolve the settled view and choose whether a supplied prepared plan is usable.
    pub resolve_view: std::time::Duration,
    /// Lower the current query into a one-shot query program when no prepared plan applies.
    pub compile_program: std::time::Duration,
    /// Select or construct the executable plan and resolve policy/output schema context.
    pub select_plan: std::time::Duration,
    /// Execute the selected Groove graph or prepared shape and collect output deltas.
    pub execute_plan: std::time::Duration,
    /// Decode positive output records and materialize Jazz current rows.
    pub decode_materialize: std::time::Duration,
    /// Apply query-engine post-processing such as includes, ordering, offset, and limit.
    pub finish_rows: std::time::Duration,
    /// Apply the requested output projection.
    pub apply_projection: std::time::Duration,
    /// Total time spent in the profiled node query path.
    pub total: std::time::Duration,
}

/// Builder for a local mergeable commit.
#[derive(Clone)]
pub struct MergeableCommit {
    /// Target table.
    pub table: String,
    /// Target row.
    pub row_uuid: RowUuid,
    /// Author making the commit.
    pub made_by: AuthorId,
    /// Identity used for write-policy evaluation.
    pub permission_subject: Option<AuthorId>,
    /// Abstract wall clock at the committing node.
    pub now_ms: u64,
    /// User cells for content versions.
    pub cells: BTreeMap<String, Value>,
    /// Explicitly authored content columns. `None` means every supplied cell.
    pub authored_columns: Option<BTreeSet<String>>,
    /// Deletion-register event, if any.
    pub deletion: Option<DeletionEvent>,
    /// Parent content versions.
    pub parents: Vec<TxId>,
    /// Optional application metadata.
    pub user_metadata_json: Option<String>,
}

impl MergeableCommit {
    /// Construct an empty mergeable commit builder.
    pub fn new(table: impl Into<String>, row_uuid: RowUuid, now_ms: u64) -> Self {
        Self {
            table: table.into(),
            row_uuid,
            made_by: AuthorId::SYSTEM,
            permission_subject: None,
            now_ms,
            cells: BTreeMap::new(),
            authored_columns: None,
            deletion: None,
            parents: Vec::new(),
            user_metadata_json: None,
        }
    }

    /// Set the commit author.
    pub fn made_by(mut self, made_by: AuthorId) -> Self {
        self.made_by = made_by;
        self
    }

    /// Set the authenticated identity used for write policy.
    pub fn permission_subject(mut self, permission_subject: AuthorId) -> Self {
        self.permission_subject = Some(permission_subject);
        self
    }

    pub(crate) fn effective_permission_subject(&self) -> AuthorId {
        self.permission_subject.unwrap_or(self.made_by)
    }

    /// Set user cells for a content version.
    pub fn cells<V: Into<Value>>(mut self, cells: BTreeMap<String, V>) -> Self {
        self.cells = cells
            .into_iter()
            .map(|(column, value)| (column, value.into()))
            .collect();
        self
    }

    /// Set one user cell for a content version.
    pub fn cell(mut self, column: impl Into<String>, value: Value) -> Self {
        self.cells.insert(column.into(), value);
        self
    }

    /// Preserve which cells were explicitly authored when `cells` is a
    /// materialized snapshot assembled for a partial update.
    pub fn authored_columns(mut self, columns: BTreeSet<String>) -> Self {
        self.authored_columns = Some(columns);
        self
    }

    /// Set a deletion-register event.
    pub fn deletion(mut self, deletion: DeletionEvent) -> Self {
        self.deletion = Some(deletion);
        self
    }

    /// Set parent content versions.
    pub fn parents(mut self, parents: Vec<TxId>) -> Self {
        self.parents = parents;
        self
    }

    /// Attach application metadata.
    pub fn user_metadata(mut self, json: String) -> Self {
        self.user_metadata_json = Some(json);
        self
    }

    fn validate(&self) -> Result<(), Error> {
        validate_mergeable_write_shape(self.cells.is_empty(), self.deletion.is_some())
    }
}

#[derive(Clone)]
pub(crate) struct ViewUpdateParts {
    pub(crate) subscription: SubscriptionKey,
    pub(crate) settled_through: GlobalSeq,
    pub(crate) defer_settlement: bool,
    pub(crate) reset_result_set: bool,
    pub(crate) version_carriers: Vec<VersionCarrier>,
    pub(crate) version_bundles: Vec<VersionBundle>,
    pub(crate) peer_complete_tx_payload_refs: Vec<TxId>,
    pub(crate) authorization_progress: Option<u64>,
    pub(crate) opening_pending: bool,
    pub(crate) result_member_adds: Vec<ResultMemberEntry>,
    pub(crate) result_member_removes: Vec<ResultMemberEntry>,
    pub(crate) terminal_operations: Vec<groove::ivm::TerminalOperation>,
    pub(crate) program_fact_adds: Vec<ViewFactEntry>,
    pub(crate) program_fact_removes: Vec<ViewFactEntry>,
}

#[derive(Default)]
struct IngestMemo {
    tx_exists: BTreeMap<TxId, bool>,
    tx_made_at: BTreeMap<TxId, Option<TxTime>>,
}

struct CatalogueOpenState<S> {
    storage: S,
    schemas: BTreeMap<SchemaVersionId, SchemaVersion>,
    lenses: BTreeMap<MigrationLensId, MigrationLens>,
    schema_version_aliases: BTreeMap<SchemaVersionId, SchemaVersionAlias>,
    physical_mappings: BTreeMap<SchemaVersionId, SchemaPhysicalMapping>,
    staged_lineages: BTreeMap<u64, StagedSchemaLineage>,
    pending_lineages: BTreeMap<u64, PendingSchemaLineage>,
    active_lineages_by_target: BTreeMap<SchemaVersionId, StagedSchemaLineage>,
    active_catalogue_seq: u64,
    pending_write_pointers: BTreeMap<u64, CurrentWriteSchema>,
    next_physical_table_id: u64,
    next_physical_column_id: u64,
    current_write_schema: CurrentWriteSchema,
    branch_partitions: BTreeSet<(PhysicalTableId, BranchId)>,
    catalogue_bootstrap_marker: bool,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct StagedSchemaLineage {
    catalogue_seq: u64,
    publication: SchemaLineagePublication,
    alias: SchemaVersionAlias,
    mapping: SchemaPhysicalMapping,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
struct PendingSchemaLineage {
    catalogue_seq: u64,
    publication: SchemaLineagePublication,
}

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
struct SchemaLineageActivation {
    id: SchemaLineagePublicationId,
    catalogue_seq: u64,
}

/// Durable completion receipt for an authority snapshot installed by an
/// initially unconfigured dynamic edge.  Its record is the atomic boundary:
/// discovery never repairs a prefix that lacks this exact join.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
struct CatalogueBootstrapReady {
    genesis: SchemaVersionId,
    current_write_schema: CurrentWriteSchema,
    active_catalogue_seq: u64,
}

#[cfg(any(test, feature = "testing"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub enum CatalogueActivationFailpoint {
    AfterStaged,
    AfterRegistration,
    BeforeSnapshotActivationCommit,
}

#[derive(Clone, Debug)]
pub(crate) enum PreparedQueryPlan {
    Graph(GraphBuilder),
    Prepared {
        shape: PreparedShapeId,
        params: Vec<PreparedQueryParam>,
    },
    PeerMaintainedMarker,
}

pub(crate) type PreparedQueryPlanHandle = Arc<PreparedQueryPlan>;

#[derive(Clone, Debug)]
pub(crate) struct PreparedQueryParam {
    pub(crate) name: String,
    pub(crate) ty: groove::schema::ColumnType,
    pub(crate) source: PreparedQueryParamSource,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PreparedQueryParamSource {
    User,
    Claim(query_engine::ClaimPath),
}

fn validate_mergeable_write_shape(cells_empty: bool, deletion_present: bool) -> Result<(), Error> {
    match (cells_empty, deletion_present) {
        (false, false) | (true, true) => Ok(()),
        (false, true) => Err(Error::InvalidMergeableCommit(
            "content versions cannot also carry deletion-register events",
        )),
        (true, false) => Err(Error::InvalidMergeableCommit(
            "mergeable commits must carry content cells or a deletion-register event",
        )),
    }
}

#[cfg(test)]
fn select_all(table: &str) -> Query {
    Query::Select(Box::new(
        Select::new([SelectItem::Wildcard]).from([TableRef::named(table)]),
    ))
}

fn known_state_fact_key(binding_view_key: BindingViewKey) -> [Value; 3] {
    [
        Value::Uuid(binding_view_key.shape_id.0),
        Value::Uuid(binding_view_key.binding_id.0),
        Value::Uuid(binding_view_key.read_view.id),
    ]
}

fn binding_view_store_prefix(binding_view_key: BindingViewKey) -> Vec<Value> {
    known_state_fact_key(binding_view_key).to_vec()
}

fn settled_result_member_key(
    binding_view_key: BindingViewKey,
    member: &ResultMemberEntry,
) -> Result<Vec<Value>, Error> {
    let mut key = binding_view_store_prefix(binding_view_key);
    key.push(Value::Bytes(postcard::to_allocvec(member).map_err(
        |_| Error::InvalidStoredValue("settled result member must encode"),
    )?));
    Ok(key)
}

fn settled_program_fact_key(
    binding_view_key: BindingViewKey,
    fact: &ViewFactEntry,
) -> Result<Vec<Value>, Error> {
    let mut key = binding_view_store_prefix(binding_view_key);
    key.push(Value::Bytes(postcard::to_allocvec(fact).map_err(|_| {
        Error::InvalidStoredValue("settled program fact must encode")
    })?));
    Ok(key)
}

/// Details of a persisted current row that could not be decoded at the point of use.
#[derive(Debug, thiserror::Error)]
#[error("malformed current row in table {table} for {row_uuid:?}: {source}")]
pub struct MalformedCurrentRow {
    /// Logical table containing the row.
    pub table: String,
    /// Primary-key row identity of the malformed record.
    pub row_uuid: RowUuid,
    /// The record decoding failure.
    #[source]
    pub source: records::Error,
}

/// Error type returned by the storage-backed node API.
#[derive(Debug, Error)]
pub enum Error {
    /// Error returned by groove.
    #[error(transparent)]
    Groove(#[from] GrooveDbError),
    /// Error returned by groove records.
    #[error(transparent)]
    Record(#[from] records::Error),
    /// A persisted current row could not be decoded at the point of use.
    #[error(transparent)]
    MalformedCurrentRow(#[from] Box<MalformedCurrentRow>),
    /// Error returned by storage.
    #[error(transparent)]
    Storage(#[from] storage::Error),
    /// Error returned by query validation or binding.
    #[error("{0}")]
    Query(#[source] Box<QueryError>),
    /// Query could not be represented by the unified query engine.
    #[error("query lowering failed: {0}")]
    QueryLowering(String),
    /// Query-engine capability report for a currently unsupported program.
    #[error("query capability unsupported: {0}")]
    QueryCapability(String),
    /// A terminal authorization-support subscription depends on a session
    /// claim the permission subject did not provide. This is a denied proof,
    /// not malformed persisted state.
    #[error("authorization support policy claim is not bound: {0}")]
    AuthorizationSupportMissingClaim(String),
    /// A membership-policy proof revisited a table already on its compilation
    /// stack. The named error prevents stack exhaustion while diagnosing a
    /// policy cycle.
    #[error("PolicyProofCycle: table '{table}' re-entered at depth {depth}")]
    PolicyProofCycle {
        /// Revisited policy table.
        table: String,
        /// Compilation-stack depth at attempted re-entry.
        depth: usize,
    },
    /// Table was not found in the schema.
    #[error("table not found: {0}")]
    TableNotFound(String),
    /// Column type is not supported by Jazz v0.
    #[error("M1 only supports string user columns, got unsupported column: {0}")]
    UnsupportedColumnType(String),
    /// Mergeable commit shape is invalid.
    #[error("invalid mergeable commit: {0}")]
    InvalidMergeableCommit(&'static str),
    /// An exclusive transaction no longer matches its fixed local snapshot.
    #[error("row visible parent changed since transaction write was staged")]
    TransactionConflict,
    /// Stored value failed validation.
    #[error("invalid stored value: {0}")]
    InvalidStoredValue(&'static str),
    /// Transaction was not known locally.
    #[error("missing transaction: {0:?}")]
    MissingTransaction(TxId),
    /// View update payload was internally inconsistent.
    #[error("malformed view update: {0}")]
    MalformedViewUpdate(&'static str),
    /// Maintained subscription view lacked the version witness needed to emit
    /// a self-contained incremental bundle.
    #[error("maintained subscription view missing bundle witness: {0}")]
    MaintainedViewMissingBundleWitness(&'static str),
    /// Open transaction handle was not known.
    #[error("missing open transaction: {0:?}")]
    MissingOpenBatch(OpenBatchId),
    /// A caller attempted to reuse an identity that still names live mutable work.
    #[error("duplicate open batch id: {0}")]
    DuplicateOpenBatch(OpenBatchId),
    /// Fate or global-current update was non-monotone.
    #[error("non-monotone state update: {0}")]
    NonMonotoneState(&'static str),
    /// Commit unit conflicted with an existing transaction.
    #[error("conflicting commit unit for transaction: {0:?}")]
    ConflictingCommitUnit(TxId),
    /// Fate transition conflicted with an existing fate.
    #[error("conflicting fate transition")]
    ConflictingFate,
    /// Commit unit kind is unsupported.
    #[error("unsupported commit unit: {0}")]
    UnsupportedCommitUnit(&'static str),
    /// Sync message kind is unsupported.
    #[error("unsupported sync message: {0}")]
    UnsupportedSyncMessage(&'static str),
    /// Catalogue lane message was not authorized.
    #[error("unauthorized catalogue update")]
    UnauthorizedCatalogueUpdate,
    /// Catalogue payload failed validation.
    #[error("invalid catalogue update: {0}")]
    InvalidCatalogueUpdate(&'static str),
    /// Durable staged catalogue activation failed and requires node reopen.
    #[error("catalogue activation failed; reopen required")]
    CatalogueActivationFailed,
    /// A dynamically catalogued runtime has not yet received the trusted
    /// authority snapshot that establishes its genesis, mappings, and pointer.
    #[error("catalogue bootstrap is not ready")]
    CatalogueUninitialized,
    /// Durable catalogue payload could not be encoded or decoded.
    #[error(transparent)]
    CatalogueCodec(#[from] serde_json::Error),
    /// Historical read must be evaluated by a history-complete server.
    #[error("historical read requires server evaluation")]
    HistoricalReadRequiresServer,
    /// Branch id was not known locally.
    #[error("branch not found: {0:?}")]
    BranchNotFound(BranchId),
    /// Branch is no longer open for writes.
    #[error("branch is not open: {0:?}")]
    BranchClosed(BranchId),
    /// Branch-scoped exclusive transactions are not implemented in v1.
    #[error("exclusive transactions on branches are unsupported in v1")]
    UnsupportedBranchExclusive,
    /// Local branch-merge calculation could not prove or encode the requested
    /// ordinary target write.
    #[error("branch merge calculation failed: {0}")]
    BranchMergeCalculation(&'static str),
    /// The authenticated identity is not authorized for this operation.
    #[error("authorization denied")]
    AuthorizationDenied,
    /// A prepared point-read subscription closed before its initial snapshot.
    #[error("prepared point-read subscription closed")]
    SubscriptionClosed,
}

/// Whether encoding a projected current row failed solely because the read
/// schema does not know the selected enum case.
///
/// Schema projection is deliberately fail-closed at this boundary: callers
/// omit that row, while every other lens, materialization, and record error
/// remains visible to the query caller.
pub(super) fn is_unrepresentable_enum_projection(error: &Error) -> bool {
    matches!(
        error,
        Error::Record(
            records::Error::InvalidEnumDiscriminant { .. } | records::Error::UnknownEnumTag { .. }
        )
    )
}

impl From<QueryError> for Error {
    fn from(error: QueryError) -> Self {
        Self::Query(Box::new(error))
    }
}

#[cfg(test)]
#[test]
fn query_errors_keep_display_source_and_matching_after_node_conversion() {
    let error = Error::from(QueryError::UnknownTable("missing".to_owned()));

    assert_eq!(error.to_string(), "unknown table missing");
    assert_eq!(
        std::error::Error::source(&error).unwrap().to_string(),
        "unknown table missing"
    );
    assert!(matches!(
        error,
        Error::Query(source) if matches!(*source, QueryError::UnknownTable(ref table) if table == "missing")
    ));
}
