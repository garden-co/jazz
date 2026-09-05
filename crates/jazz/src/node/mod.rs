//! Storage-backed Jazz node core. This module owns the `NodeState` state struct,
//! public node API surface, shared errors, and cross-cutting in-memory indexes;
//! specialized behavior lives in sibling modules such as [`policy`] for policy
//! evaluation, [`global_state`] for read-only settled-global derivations,
//! [`ingest`] for commit/fate ingestion, [`query_eval`] for query execution, and
//! [`views`] for sync view payloads. In the layer map it is the core between the
//! `Db` facade and groove storage/IVM.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(feature = "testing")]
use std::time::Duration;
#[cfg(feature = "testing")]
use web_time::Instant;

use groove::db::{
    AppliedBatch, CommitMetrics, Database, DatabaseBatch, DirectRecordStoreWrite,
    Error as GrooveDbError, GraphBuilder, PersistedBatch, PredicateExpr, PrimaryKeyValue,
    Subscription,
};
use groove::ivm::PreparedShapeId;
use groove::ivm::ProjectField;
#[cfg(test)]
use groove::queries::{Query, Select, SelectItem, TableRef};
use groove::records::{self, BorrowedRecord, OwnedRecord, Value};
use groove::storage::{self, BoxedStorage, OrderedKvStorage, ReopenableStorage, StorageLayout};
use rustc_hash::FxHashSet;
use thiserror::Error;

use self::query_engine::{QueryAuthorizationMode, user_column_field};
use crate::ids::{
    AuthorSubject, MigrationLensId, NodeAlias, NodeUuid, PhysicalColumnId, PhysicalTableId,
    RowUuid, SchemaFamilyId, SchemaLineagePublicationId, SchemaVersionAlias, SchemaVersionId,
};
use crate::protocol::{
    AuthorityResultKey, BindingViewKey, BranchKey, BranchSelector, CoveredInputEntry,
    CurrentWriteSchema, LensOp, MigrationLens, PhysicalColumnIdentity, PhysicalIdentityManifest,
    PhysicalTableIdentity, PolicyBindingKey, ProgramFactEntry, ProgramSourceId, ProgramSourceRole,
    ReadViewKey, RealRowMemberEntry, ResultMemberEntry, ResultRowEntry, RowVersionRef,
    SchemaLineagePublication, SchemaVersion, ShapeAst, Subscribe, SubscriptionKey, SyncMessage,
    VersionBundle, VersionCarrier, VersionRecord, ViewFactEntry, expand_version_carriers,
};
use crate::query::{
    Binding, BindingId, OrderBy, Query as JazzQuery, QueryError, ShapeId, ValidatedQuery,
};
use crate::schema::{
    AUTHORITY_POLICY_BINDINGS_STORE, JazzSchema, KNOWN_STATE_FACTS_STORE, MergeStrategy,
    SCOPE_RELAY_REPAIR_LEDGER_STORE, SETTLED_PROGRAM_FACTS_STORE, SETTLED_RESULT_MEMBERS_STORE,
    TableSchema, registered_column_transform,
};
use crate::time::{GlobalTime, TxTime};
use crate::tools::OpenTransactionId;
use crate::tx::{
    AbsentRead, BranchWriteIntent, BranchWriteOperation, ContributionComponent,
    ContributionCoordinate, ContributionDot, ContributionMergeProvenance, ContributionSubstitution,
    ContributionSubstitutionIndex, DeletionEvent, DurabilityTier, Fate, HistoryEntry, MergeAspect,
    PredicateRead, RejectedTransaction, RejectedVersion, RejectionReason, RowRead, Snapshot,
    Transaction, TransactionRecord, TxId, TxKind,
};

fn install_enum_case_ids(
    slot: &mut Vec<GlobalScalarEnumCaseId>,
    ids: &[crate::ids::GlobalPhysicalEnumVariantId],
    introducing_schema: SchemaVersionId,
) -> Result<(), Error> {
    let expected = ids
        .iter()
        .copied()
        .enumerate()
        .map(|(ordinal, id)| {
            Ok(GlobalScalarEnumCaseId {
                id,
                introducing_schema,
                introducing_ordinal: u8::try_from(ordinal)
                    .map_err(|_| Error::InvalidStoredValue("scalar enum tag exhausted"))?,
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;
    let shared = slot.len().min(expected.len());
    if slot[..shared] != expected[..shared] || slot.len() > expected.len() {
        return Err(Error::InvalidStoredValue(
            "enum registry conflicts with authority identities",
        ));
    }
    slot.extend(expected.into_iter().skip(slot.len()));
    Ok(())
}

fn install_payload_enum_case_ids(
    slot: &mut Vec<GlobalEnumCaseId>,
    ids: &[crate::ids::GlobalPhysicalEnumVariantId],
    introducing_schema: SchemaVersionId,
) -> Result<(), Error> {
    let expected = ids
        .iter()
        .copied()
        .enumerate()
        .map(|(ordinal, id)| {
            Ok(GlobalEnumCaseId {
                id,
                introducing_schema,
                introducing_ordinal: u32::try_from(ordinal)
                    .map_err(|_| Error::InvalidStoredValue("payload enum tag exhausted"))?,
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;
    let shared = slot.len().min(expected.len());
    if slot[..shared] != expected[..shared] || slot.len() > expected.len() {
        return Err(Error::InvalidStoredValue(
            "enum registry conflicts with authority identities",
        ));
    }
    slot.extend(expected.into_iter().skip(slot.len()));
    Ok(())
}

fn hydrate_nested_scalar_enum_cases(
    value_type: &records::ValueType,
    identities: &BTreeMap<String, Vec<crate::ids::GlobalPhysicalEnumVariantId>>,
    introducing_schema: SchemaVersionId,
    authored_path: &str,
    path: &str,
    output: &mut BTreeMap<String, Vec<GlobalScalarEnumCaseId>>,
) -> Result<(), Error> {
    use records::ValueType;
    match value_type {
        ValueType::EnumTag(schema) => {
            let ids = identities
                .get(authored_path)
                .ok_or(Error::InvalidStoredValue(
                    "nested scalar enum authority identities missing",
                ))?;
            if ids.len() != schema.variants.len() {
                return Err(Error::InvalidStoredValue(
                    "nested scalar enum authority identity width mismatch",
                ));
            }
            install_enum_case_ids(
                output.entry(path.to_owned()).or_default(),
                ids,
                introducing_schema,
            )?;
        }
        ValueType::Nullable(inner) => hydrate_nested_scalar_enum_cases(
            inner,
            identities,
            introducing_schema,
            &format!("{authored_path}/nullable"),
            &format!("{path}/nullable"),
            output,
        )?,
        ValueType::Array(inner) => hydrate_nested_scalar_enum_cases(
            inner,
            identities,
            introducing_schema,
            &format!("{authored_path}/array"),
            &format!("{path}/array"),
            output,
        )?,
        ValueType::Tuple(values) => {
            for (index, value) in values.iter().enumerate() {
                hydrate_nested_scalar_enum_cases(
                    value,
                    identities,
                    introducing_schema,
                    &format!("{authored_path}/tuple/{index}"),
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
                    identities,
                    introducing_schema,
                    &format!("{authored_path}/record/{name}"),
                    &format!("{path}/record/{name}"),
                    output,
                )?;
            }
        }
        ValueType::Enum(schema) => {
            for (ordinal, case) in schema.cases.iter().enumerate() {
                let identity = GlobalEnumCaseId {
                    id: *identities
                        .get(authored_path)
                        .and_then(|ids| ids.get(ordinal))
                        .ok_or(Error::InvalidStoredValue(
                            "nested payload enum authority identity missing",
                        ))?,
                    introducing_schema,
                    introducing_ordinal: u32::try_from(ordinal).map_err(|_| {
                        Error::InvalidStoredValue("nested payload enum tag exhausted")
                    })?,
                };
                hydrate_nested_scalar_enum_cases(
                    &records::ValueType::Record(Box::new(case.payload.clone())),
                    identities,
                    introducing_schema,
                    &format!("{authored_path}/case/{ordinal}"),
                    &global_case_path(path, &identity),
                    output,
                )?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn global_case_path(path: &str, case: &GlobalEnumCaseId) -> String {
    format!("{path}/case/{}", case.id.0.simple())
}

/// Register every nested payload enum by its permanent global variant UUID.
/// Scalar descendants are handled by the existing scalar registry walker;
/// this map owns only the payload tag boundary itself.
fn hydrate_nested_payload_enum_cases(
    value_type: &records::ValueType,
    identities: &BTreeMap<String, Vec<crate::ids::GlobalPhysicalEnumVariantId>>,
    introducing_schema: SchemaVersionId,
    authored_path: &str,
    path: &str,
    output: &mut BTreeMap<String, Vec<GlobalEnumCaseId>>,
) -> Result<(), Error> {
    use records::ValueType;
    match value_type {
        ValueType::Nullable(inner) => hydrate_nested_payload_enum_cases(
            inner,
            identities,
            introducing_schema,
            &format!("{authored_path}/nullable"),
            &format!("{path}/nullable"),
            output,
        )?,
        ValueType::Array(inner) => hydrate_nested_payload_enum_cases(
            inner,
            identities,
            introducing_schema,
            &format!("{authored_path}/array"),
            &format!("{path}/array"),
            output,
        )?,
        ValueType::Tuple(values) => {
            for (index, value) in values.iter().enumerate() {
                hydrate_nested_payload_enum_cases(
                    value,
                    identities,
                    introducing_schema,
                    &format!("{authored_path}/tuple/{index}"),
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
                    identities,
                    introducing_schema,
                    &format!("{authored_path}/record/{name}"),
                    &format!("{path}/record/{name}"),
                    output,
                )?;
            }
        }
        ValueType::Enum(schema) => {
            let ids = identities
                .get(authored_path)
                .ok_or(Error::InvalidStoredValue(
                    "nested payload enum authority identities missing",
                ))?;
            if ids.len() != schema.cases.len() {
                return Err(Error::InvalidStoredValue(
                    "nested payload enum authority identity width mismatch",
                ));
            }
            install_payload_enum_case_ids(
                output.entry(path.to_owned()).or_default(),
                ids,
                introducing_schema,
            )?;
            let cases = output[path].clone();
            for (ordinal, (case, layout)) in cases.iter().zip(&schema.cases).enumerate() {
                hydrate_nested_payload_enum_cases(
                    &records::ValueType::Record(Box::new(layout.payload.clone())),
                    identities,
                    introducing_schema,
                    &format!("{authored_path}/case/{ordinal}"),
                    &global_case_path(path, case),
                    output,
                )?;
            }
        }
        _ => {}
    }
    Ok(())
}

mod catalogue_ingest;
mod codec;
mod currency;
mod database_slot;
mod descriptor_roles;
mod eviction;
mod global_state;
mod ingest;
pub(crate) mod maintained_subscription_view;
mod open_tx;
pub(crate) mod physical;
mod policy;
pub(crate) mod query_engine;
mod query_eval;
mod recovery;
mod source_resolution;
mod views;
pub(crate) use open_tx::TransactionBranchRowState;
#[cfg(feature = "testing")]
pub(crate) use query_eval::LocalMaintainedViewSubscriptionFootprint;
#[cfg(test)]
pub(crate) use query_eval::take_client_physical_row_query_calls_for_test;
pub(crate) use query_eval::{
    CoveredInputReceiver, LocalAuthorityReconciliation, LocalMaintainedViewSubscription,
    LocalMaintainedViewSubscriptionUpdate,
};
pub(crate) use views::MaintainedViewBundleInputs;

type ResultRowMembershipKey = crate::tools::OutputOccurrenceId;

use codec::*;
use database_slot::DatabaseSlot;
use open_tx::*;
use physical::*;

pub use eviction::{EdgeCacheBudget, EdgeCacheBudgetReport, EdgeCacheClass, EvictColdReport};

/// Test/bench-only attribution for durable-state work performed while opening a node.
#[cfg(feature = "testing")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NodeOpenReceipt {
    /// Catalogue-record decoding and activation preparation.
    pub catalogue_open: Duration,
    /// Groove database construction from recovered physical layouts.
    pub database_open: Duration,
    /// Process-local node allocation and initialization.
    pub state_init: Duration,
    /// Total durable-state recovery time.
    pub recover_storage: Duration,
    /// Alias, schema-family, clock, and physical-history recovery time.
    pub recover_catalogue_state: Duration,
    /// Retained for receipt compatibility; startup no longer makes this sweep.
    pub validate_current_rows: Duration,
    /// Accepted global-time recovery time.
    pub recover_global_times: Duration,
    /// Pending-edge and rejected-transaction recovery time.
    pub recover_pending_and_rejected: Duration,
    /// Bounded unclean-close cleanup time.
    pub recover_unclean_close: Duration,
    /// Persisted maintained-query known-state recovery time.
    pub recover_known_state: Duration,
    /// In-memory ahead-current index reconstruction time.
    pub rebuild_ahead_current: Duration,
    /// Final catalogue persistence time, when applicable.
    pub finalize_catalogue: Duration,
    /// Current rows decoded by the retired full validation sweep.
    pub validated_current_rows: usize,
    /// Accepted global-time entries recovered.
    pub accepted_global_times: usize,
    /// Transaction index records inspected for global timestamps.
    pub global_time_records_scanned: usize,
    /// Physical ahead-current records consumed while rebuilding indexes.
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
    static PARENT_VERSION_LOOKUP_MATERIALIZED_ROWS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
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
pub(super) fn reset_parent_version_lookup_materialized_row_count() {
    PARENT_VERSION_LOOKUP_MATERIALIZED_ROWS.with(|rows| rows.set(0));
}

#[cfg(test)]
pub(super) fn parent_version_lookup_materialized_row_count() -> usize {
    PARENT_VERSION_LOOKUP_MATERIALIZED_ROWS.with(std::cell::Cell::get)
}

#[cfg(test)]
fn record_query_versions_for_tx_call() {
    QUERY_VERSIONS_FOR_TX_CALLS.with(|calls| calls.set(calls.get() + 1));
}

#[cfg(test)]
fn record_parent_version_lookup_materialized_rows(rows: usize) {
    PARENT_VERSION_LOOKUP_MATERIALIZED_ROWS.with(|count| count.set(count.get() + rows));
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
    database: DatabaseSlot,
    local_chunk_reader: groove::chunks::LocalChunkReader,
    chunk_resolver: Rc<dyn groove::chunks::MissingChunkResolver>,
    large_value_staging_policy: LargeValueStagingPolicy,
    large_value_ingress: RefCell<LargeValueIngressState>,
    /// Groove-owned verified cache retained across internal database rebuilds.
    content_runtime_provider: groove::chunks::OwnedChunkProvider,
    storage_type: std::marker::PhantomData<fn() -> S>,
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
    /// This process is the durable browser relay that owns upstream Edge
    /// authority sessions for a non-durable client. This is process-local
    /// topology, never schema policy or persisted state.
    relay_authority_session_owner: Option<crate::db::ClientRelayScope>,
    /// Resident transactions whose Groove persistence receipt has not settled.
    pending_persistence: BTreeSet<TxId>,
    /// Mapping from stable node UUIDs to compact on-disk aliases.
    pub(crate) node_aliases: BTreeMap<NodeUuid, NodeAlias>,
    /// Exact ahead-current keys used to make peer replay idempotent. No caller
    /// needs ordering, so use the low-overhead deterministic hasher here.
    ahead_current_keys: FxHashSet<(PhysicalTableId, VersionLayer, Vec<u8>)>,
    /// Runtime counters for sync parking, draining, and ingestion behavior.
    sync_metrics: SyncMetrics,
    /// Runtime counters for query-engine read authorization paths.
    query_engine_read_metrics: QueryEngineReadMetrics,
    /// Test-only observer for one node's merge-head graph walks. This must be
    /// node-scoped so unrelated parallel test nodes cannot contaminate it.
    #[cfg(any(test, feature = "testing"))]
    merge_head_reachability_walks: usize,
    /// Process-local claims attached to authenticated subscriber sessions.
    session_claims: BTreeMap<AuthorSubject, BTreeMap<String, Value>>,
    /// Monotone revision for each identity's process-local session claims.
    session_claim_revisions: BTreeMap<AuthorSubject, u64>,
    /// Claims scoped to the subscriber whose query is currently compiling.
    /// This never escapes the node lock, so equal authenticated identities on
    /// concurrent transports cannot change each other's policy inputs.
    active_session_claims: Option<(AuthorSubject, BTreeMap<String, Value>)>,
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

// A descriptor-only start performs durable work despite carrying no chunk
// bytes. Charge one MiB of the existing ingress budget to bound that work rate.
pub(crate) const LARGE_VALUE_UPLOAD_START_INGRESS_CHARGE_BYTES: u64 = 1 << 20;

/// Jazz-owned limits for unpublished Groove staging roots.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LargeValueStagingPolicy {
    /// Incoming upload-work budget. Chunk batches charge encoded bytes;
    /// descriptor-only starts charge a fixed nonzero amount.
    pub incoming_bytes_per_window: u64,
    /// Fixed rate-limit window duration.
    pub window_ms: u64,
    /// Maximum staging age used by explicit maintenance eviction.
    pub max_age_ms: u64,
}

impl Default for LargeValueStagingPolicy {
    fn default() -> Self {
        Self {
            // Admit one maximum-size logical wire message per second by
            // default. Deployments can tighten this without changing Groove's
            // policy-blind storage contract.
            incoming_bytes_per_window: 256 * 1024 * 1024,
            window_ms: 1_000,
            // Completed uploads are deliberately short-lived claims. Ten
            // minutes tolerates slow authority synchronization while bounding
            // abandoned staging on an otherwise unconfigured host.
            max_age_ms: 10 * 60 * 1_000,
        }
    }
}

#[derive(Default)]
struct LargeValueIngressState {
    window_started_ms: u64,
    admitted_bytes: u64,
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
    /// Immutable lowering plans reused by authored-to-physical row writes.
    physical_write_plan_cache: BTreeMap<
        SchemaVersionId,
        BTreeMap<
            String,
            BTreeMap<physical::PhysicalWriteTarget, Arc<physical::PreparedPhysicalWritePlan>>,
        >,
    >,
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

/// Local transaction clock and settled-global application progress.
#[derive(Clone, Debug)]
struct Clock {
    /// Highest local transaction timestamp observed or minted by this node.
    tx_time: TxTime,
    /// Synchronously reservable high-water mirror shared with the host-facing
    /// node wrapper. Every mint/merge path advances both clocks.
    reservation_high_water: Rc<Cell<TxTime>>,
    /// Highest authority settlement timestamp observed or minted by this node.
    global_time_register: GlobalTime,
    /// Authority timestamps allocated here and awaiting accepted application.
    locally_minted_global_times: BTreeSet<GlobalTime>,
    /// Highest globally accepted timestamp durably committed by this core.
    committed_global_time: GlobalTime,
    /// Global transactions held by a partial node outside its core frontier.
    applied_global_times_after_frontier: BTreeSet<GlobalTime>,
}

impl Clock {
    fn allocate_global_time(&mut self, now_ms: u64) -> Result<GlobalTime, Error> {
        let global_time = GlobalTime::tick(self.global_time_register, now_ms)?;
        self.global_time_register = global_time;
        self.locally_minted_global_times.insert(global_time);
        Ok(global_time)
    }
}

#[cfg(test)]
impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    pub(super) fn reset_merge_head_reachability_walks_for_test(&mut self) {
        self.merge_head_reachability_walks = 0;
    }

    pub(super) fn merge_head_reachability_walks_for_test(&self) -> usize {
        self.merge_head_reachability_walks
    }

    fn allocate_global_time_for_test(&mut self) -> GlobalTime {
        self.clock
            .allocate_global_time(self.tx_time_high_water().physical_ms())
            .expect("test global HLC must have capacity")
    }

    fn accept_global_for_test(&mut self, tx_id: TxId) -> Result<(), Error> {
        let global_time = self.allocate_global_time_for_test();
        crate::db::block_on(self.apply_fate_update(
            tx_id,
            Fate::Accepted,
            Some(global_time),
            Some(DurabilityTier::Global),
        ))
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

/// Recently stored transaction versions with a row-addressable cache view.
///
/// The complete vector remains available to callers that need the whole
/// transaction. Parent validation, however, is constrained by the physical
/// `(table, row)` coordinate, so its cache hit must not revisit unrelated
/// siblings from a wide transaction.
#[derive(Clone, Debug, Default)]
struct CachedTransactionVersions {
    versions: Vec<VersionRow>,
    by_schema_table_row: BTreeMap<(SchemaVersionAlias, String, RowUuid), Vec<usize>>,
}

impl CachedTransactionVersions {
    fn new(versions: Vec<VersionRow>) -> Self {
        let mut by_schema_table_row = BTreeMap::new();
        for (index, version) in versions.iter().enumerate() {
            by_schema_table_row
                .entry((
                    version.schema_version_alias(),
                    version.table().to_owned(),
                    version.row_uuid(),
                ))
                .or_insert_with(Vec::new)
                .push(index);
        }
        Self {
            versions,
            by_schema_table_row,
        }
    }

    fn versions_for_schema_table_row(
        &self,
        schema_alias: SchemaVersionAlias,
        table: &str,
        row_uuid: RowUuid,
    ) -> Vec<VersionRow> {
        let key = (schema_alias, table.to_owned(), row_uuid);
        let Some(indexes) = self.by_schema_table_row.get(&key) else {
            return Vec::new();
        };
        #[cfg(test)]
        record_parent_version_lookup_materialized_rows(indexes.len());
        indexes
            .iter()
            .map(|index| self.versions[*index].clone())
            .collect()
    }
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
    /// Recently staged history rows for a stored transaction, indexed by
    /// authored schema/table/row so parent validation does not rescan wide
    /// transactions on a cache hit.
    tx_versions_cache: BTreeMap<TxId, CachedTransactionVersions>,
    /// Approximate insertion order for bounding `tx_version_tables_cache`.
    tx_version_tables_cache_order: VecDeque<TxId>,
    /// Live membership for `tx_version_tables_cache_order`.
    tx_version_tables_cache_order_set: BTreeSet<TxId>,
    /// Physical version-storage sources keyed by logical table and layer.
    version_storage_sources_cache: BTreeMap<(String, VersionLayer), Vec<String>>,
    /// Registered validated query shapes keyed by stable shape ID.
    registered_shapes: BTreeMap<ShapeId, ValidatedQuery>,
    /// Exact semantic registration options keyed by the read-view identity
    /// carried by a subscription.  A shape AST alone cannot reconstruct the
    /// compiler sources for a non-default read view.
    registered_shape_options:
        BTreeMap<(ShapeId, ReadViewKey), crate::protocol::RegisterShapeOptions>,
    /// Connection epochs retaining each peer-installed or peer-parked shape.
    ///
    /// One owner is recorded per peer regardless of repeated registration or
    /// the number of read-view registrations sharing the shape.
    peer_shape_owners: BTreeMap<ShapeId, BTreeSet<u64>>,
    /// Shapes registered by the local runtime rather than a served peer.
    ///
    /// Local ownership pins a shared shape after the last remote owner leaves.
    locally_registered_shapes: BTreeSet<ShapeId>,
    /// Served publications retaining each shape, keyed by the peer-state
    /// instance and concrete usage subscription that opened the publication.
    ///
    /// These are deliberately distinct from `peer_shape_owners`: the latter
    /// tracks an inbound peer's `RegisterShape` lifetime, while this records
    /// this node serving a downstream peer. Both kinds of owner may retain the
    /// same shape concurrently.
    outbound_shape_owners: BTreeMap<ShapeId, BTreeSet<(u64, SubscriptionKey)>>,
    /// Served publications retaining each concrete binding view.  This keeps
    /// one peer's retirement from leaving its binding receipts resident, while
    /// still permitting multiple peer states to share an identical binding.
    outbound_binding_owners: BTreeMap<(SubscriptionKey, PolicyBindingKey), BTreeSet<u64>>,
    /// Registered query binding values keyed by their complete usage-site
    /// admission identity. The same wire binding/read-view pair may be served
    /// to two readers with distinct immutable policy snapshots; neither may
    /// overwrite nor retire the other.
    registered_bindings: BTreeMap<ShapeId, BTreeMap<RegisteredBindingUsageKey, RegisteredBinding>>,
    /// Every settled result received from an authority.  This is deliberately
    /// keyed by Jazz's full policy-scoped identity, rather than the ordinary
    /// canonical binding view used by local maintained views.  Two sessions
    /// can run the same query with the same binding and still receive
    /// different authorized membership.
    authority_results: BTreeMap<AuthorityResultKey, AuthorityResultState>,
    // Transitional local facade materialization state.  It stays keyed by an
    // ordinary binding view and must never be used as the receiving-side
    // authority receipt.  The following implementation moves inbound updates
    // into `authority_results` first, then selects the exact aggregate when a
    // relay needs an authority source.
    applied_view_update_generations: BTreeMap<BindingViewKey, u64>,
    settled_result_sets: BTreeMap<BindingViewKey, BTreeSet<ResultMemberEntry>>,
    /// Bounded, receiver-local source pages retained by a non-durable client
    /// after the matching authority usage site detached. This is not an
    /// authority receipt: only an exact compatible Local lowering may use it;
    /// Edge/Global must open fresh coverage.
    retained_root_window_sources: BTreeMap<AuthorityResultKey, RetainedRootWindowSource>,
    settled_result_row_index:
        BTreeMap<BindingViewKey, BTreeMap<ResultRowMembershipKey, ResultMemberEntry>>,
    settled_program_facts: BTreeMap<BindingViewKey, BTreeSet<ViewFactEntry>>,
    settled_through_by_binding_view: BTreeMap<BindingViewKey, GlobalTime>,
    authorization_progress_by_binding_view: BTreeMap<BindingViewKey, u64>,
    known_state_declared_binding_views: BTreeSet<BindingViewKey>,
    initial_hydration_binding_views: BTreeSet<BindingViewKey>,
    deferred_publication_binding_views: BTreeSet<BindingViewKey>,
    pending_authoritative_reset_binding_views: BTreeSet<BindingViewKey>,
    pending_opening_binding_views: BTreeSet<BindingViewKey>,
}

/// Compiler-owned description of the root window stage represented by a
/// retained receiver input page. The page is *already the output* of this
/// stage; a compatible local lowering applies only its requested subwindow
/// relative to this descriptor, never the absolute source offset again.
///
/// Root windows have one global partition. Ordering is explicit and the
/// compiler's deterministic root-occurrence tie break is part of the
/// capability, so a differently ordered (or unbounded) request cannot reuse
/// this page accidentally.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RetainedRootWindowSource {
    /// The exact frozen root source occurrence whose rows this page contains.
    pub(crate) source: ProgramSourceId,
    /// The complete validated source shape, including its canonical identity.
    pub(crate) source_shape: ValidatedQuery,
    /// Root windows are unpartitioned; keeping this typed makes a future
    /// parent-partitioned descriptor a different, non-interchangeable shape.
    pub(crate) partition: RootWindowPartition,
    /// Ordering keys and directions applied before the window.
    pub(crate) order_by: Vec<OrderBy>,
    /// Compiler-owned deterministic tie break after `order_by`.
    pub(crate) tie_break: RootWindowTieBreak,
    /// Source window bounds in the ordered root stream.
    pub(crate) offset: usize,
    pub(crate) limit: Option<usize>,
    /// The source query with only the root window stripped. This is the exact
    /// semantic shape a target must share before its window can be rebased.
    pub(crate) query_without_window: JazzQuery,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RootWindowPartition {
    Global,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RootWindowTieBreak {
    RootOccurrence,
}

impl RetainedRootWindowSource {
    pub(crate) fn for_shape(shape: &ValidatedQuery) -> Self {
        let query = shape.query();
        let mut query_without_window = query.clone();
        query_without_window.offset = 0;
        query_without_window.limit = None;
        Self {
            source: ProgramSourceId {
                table: groove::Intern::new(query.table.clone()),
                path: vec![ProgramSourceRole::Root],
            },
            source_shape: shape.clone(),
            partition: RootWindowPartition::Global,
            order_by: query.order_by.clone(),
            tie_break: RootWindowTieBreak::RootOccurrence,
            offset: query.offset,
            limit: query.limit,
            query_without_window,
        }
    }

    pub(crate) fn is_bounded(&self) -> bool {
        self.offset != 0 || self.limit.is_some()
    }

    pub(crate) fn contains_target(&self, target: &ValidatedQuery) -> bool {
        let target_window = Self::for_shape(target);
        if self.source != target_window.source
            || self.partition != target_window.partition
            || self.order_by != target_window.order_by
            || self.tie_break != target_window.tie_break
            || self.source_shape.schema_version() != target.schema_version()
            || self.source_shape.params() != target.params()
            || self.query_without_window != target_window.query_without_window
            || self.offset > target_window.offset
        {
            return false;
        }
        let source_end = self.limit.map(|limit| self.offset.saturating_add(limit));
        let target_end = target_window
            .limit
            .map(|limit| target_window.offset.saturating_add(limit));
        !matches!((source_end, target_end), (Some(source_end), Some(target_end)) if target_end > source_end)
            && !matches!((source_end, target_end), (Some(_), None))
    }

    pub(crate) fn relative_window_for(&self, target: &ValidatedQuery) -> (usize, Option<usize>) {
        debug_assert!(self.contains_target(target));
        (target.query().offset - self.offset, target.query().limit)
    }
}

/// One authority-owned result stream, including every receipt that makes its
/// membership meaningful after a reconnect or durable reopen.  Nothing in
/// this aggregate is an ordinary local maintained-view cache.
#[derive(Clone, Debug, Default)]
pub(crate) struct AuthorityResultState {
    /// Monotonically increasing receipt for received ViewUpdates.
    applied_view_update_generation: u64,
    /// Whether this usage-site receipt has actually claimed a complete
    /// successor input closure for a receiver-local maintained graph.
    ///
    /// Merely opening a subscription creates an authority result state, but
    /// that is not evidence that an empty source closure was selected.  The
    /// first non-deferred reset claims a particular immutable closure
    /// generation.  A receiver may wait while this remains pending; once it
    /// is claimed it must reject any missing, duplicate, or otherwise
    /// non-exact [`ProgramSourceCoverage`](crate::protocol::ProgramSourceCoverageEntry)
    /// set atomically rather than accidentally interpreting it as empty.
    source_closure: AuthoritySourceClosure,
    /// Exact predecessor-preserving source changes after the claimed reset
    /// closure. This is process-local delivery state, not durable authority
    /// truth. A receiver coalesces the contiguous suffix beginning at its
    /// own installed closure receipt; it must never use an arbitrary current
    /// source set as a synthetic incremental predecessor.
    source_incrementals: Vec<AuthoritySourceIncremental>,
    /// This process has received a complete, exact authority settlement for
    /// this result since it was opened. It is deliberately live-only: durable
    /// membership recovered after a restart is useful cache material, but it
    /// is not evidence that a newly opened usage may skip fresh authority
    /// coverage.
    live_settled: bool,
    /// Exact authoritative membership and an occurrence index for replacement.
    settled_result_set: BTreeSet<ResultMemberEntry>,
    settled_result_row_index: BTreeMap<ResultRowMembershipKey, ResultMemberEntry>,
    /// Non-row facts paired with the membership.
    settled_program_facts: BTreeSet<ViewFactEntry>,
    /// O(changed) admission indexes for the exact source closure. These are
    /// receiver-local indexes over `settled_program_facts`, rebuilt on reopen;
    /// they never replace the durable closure itself.
    covered_input_sources: BTreeSet<ProgramSourceId>,
    covered_input_versions: BTreeMap<(ProgramSourceId, RowUuid), CoveredInputEntry>,
    compiled_covered_input_sources: Option<BTreeSet<ProgramSourceId>>,
    /// Optional fast cursor and authorization receipt. The cursor is durable
    /// cache metadata; only `live_settled` permits a new known-state claim.
    settled_through: Option<GlobalTime>,
    authorization_progress: Option<u64>,
    /// Per-subscription lifecycle state.  It remains policy-scoped because a
    /// reset or opening transition for Alice must never wake Bob.
    known_state_declared: bool,
    initial_hydration: bool,
    deferred_publication: bool,
    pending_authoritative_reset: bool,
    pending_opening: bool,
}

/// Receipt state for the authority-selected source closure of one usage site.
///
/// This is intentionally distinct from membership settlement.  A query may
/// have an authority-result container before any ViewUpdate selects input
/// sources, and treating that absence as an empty closure would let a strict
/// receiver settle before its policy-scoped input arrived.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum AuthoritySourceClosure {
    #[default]
    Pending,
    Claimed {
        generation: u64,
    },
}

/// One receiver input delta with an exact prior receipt generation.
#[derive(Clone, Debug)]
pub(crate) struct AuthoritySourceIncremental {
    pub(crate) predecessor_generation: u64,
    pub(crate) generation: u64,
    pub(crate) adds: BTreeSet<ProgramFactEntry>,
    pub(crate) removes: BTreeSet<ProgramFactEntry>,
}

impl AuthoritySourceIncremental {
    /// Coalesce contiguous wire frames into the one exact net transition from
    /// the retained source receipt to the newest source receipt. This is
    /// deliberately source-closure-local: unrelated ViewUpdate frames neither
    /// advance nor invalidate this predecessor chain.
    pub(crate) fn coalesce(
        previous: Option<Self>,
        predecessor_generation: u64,
        generation: u64,
        adds: BTreeSet<ProgramFactEntry>,
        removes: BTreeSet<ProgramFactEntry>,
    ) -> Result<Self, Error> {
        if !adds.is_disjoint(&removes) {
            return Err(Error::InvalidStoredValue(
                "covered input incremental delta overlaps additions and removals",
            ));
        }
        let mut result = match previous {
            Some(previous) => {
                if previous.generation != predecessor_generation {
                    return Err(Error::InvalidStoredValue(
                        "covered input incremental delta does not continue its retained predecessor",
                    ));
                }
                previous
            }
            None => Self {
                predecessor_generation,
                generation: predecessor_generation,
                adds: BTreeSet::new(),
                removes: BTreeSet::new(),
            },
        };
        for fact in adds {
            if !result.removes.remove(&fact) {
                result.adds.insert(fact);
            }
        }
        for fact in removes {
            if !result.adds.remove(&fact) {
                result.removes.insert(fact);
            }
        }
        debug_assert!(result.adds.is_disjoint(&result.removes));
        result.generation = generation;
        Ok(result)
    }
}

/// Coalesce the one contiguous source-delta chain beginning at a receiver's
/// installed receipt and ending at `successor_generation`. Missing, forked,
/// replayed, or noncontiguous links are protocol errors: only a reset may
/// repair a receiver that cannot prove its predecessor chain.
pub(crate) fn coalesce_authority_source_incrementals(
    frames: &[AuthoritySourceIncremental],
    predecessor_generation: u64,
    successor_generation: u64,
) -> Result<AuthoritySourceIncremental, Error> {
    let mut cursor = predecessor_generation;
    let mut combined = None;
    while cursor != successor_generation {
        let mut candidates = frames
            .iter()
            .filter(|frame| frame.predecessor_generation == cursor);
        let Some(frame) = candidates.next() else {
            return Err(Error::InvalidStoredValue(
                "covered input receiver missed an incremental predecessor; authority reset required",
            ));
        };
        if candidates.next().is_some() || frame.generation <= cursor {
            return Err(Error::InvalidStoredValue(
                "covered input incremental chain is forked or nonmonotonic",
            ));
        }
        combined = Some(AuthoritySourceIncremental::coalesce(
            combined,
            frame.predecessor_generation,
            frame.generation,
            frame.adds.clone(),
            frame.removes.clone(),
        )?);
        cursor = frame.generation;
    }
    combined.ok_or(Error::InvalidStoredValue(
        "covered input receiver has no incremental successor",
    ))
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
    identity: AuthorSubject,
    param_binding_mode: ParamBindingModeCacheKey,
    tier: DurabilityTier,
    binding_source_shape: Option<String>,
    binding_user_params: String,
    binding_claim_params: String,
    active_session_claims: String,
    include_deleted_root: bool,
}

/// One usage-site query binding registration.
#[derive(Clone, Debug, PartialEq)]
struct RegisteredBinding {
    values: Vec<Value>,
    read_view: ReadViewKey,
    binding_view_key: BindingViewKey,
    /// Exact, Jazz-owned namespace for authority-selected membership.  This
    /// stays separate from the canonical local binding key.
    authority_result_key: AuthorityResultKey,
    /// Immutable compiler context selected at RegisterShape ingress.
    options: crate::protocol::RegisterShapeOptions,
    /// The admitted identity associated with this usage, when a trusted relay
    /// supplied one.  Receiver source discovery preserves it even though the
    /// client-local compiler does not evaluate policy branches.
    compiler_identity: AuthorSubject,
}

/// Wire binding handles are only unique within one admitted policy scope.
/// Keep the scope in the in-memory ownership key even though the wire
/// `Unsubscribe` stays compact; authenticated ingress supplies it at teardown.
type RegisteredBindingUsageKey = (BindingId, ReadViewKey, Option<PolicyBindingKey>);

/// Locally open transactions and local-only permission attribution.
struct OpenTxState {
    /// Open transaction handles keyed by caller-generated identity.
    open_transactions: BTreeMap<OpenTransactionId, OpenTransaction>,
    /// Identities consumed by commit or rollback; never reusable in this runtime.
    closed_batches: BTreeSet<OpenTransactionId>,
    /// Local-only permission subjects for transactions whose `made_by` keeps provenance.
    local_permission_subjects: BTreeMap<TxId, AuthorSubject>,
}

/// Rejection records and derived indexes used for pending-cascade handling.
#[derive(Clone, Debug, Default)]
struct RejectionTracking {
    /// Transactions rejected by local policy or conflict checks.
    rejected_transactions: BTreeMap<TxId, RejectedTransaction>,
    /// Pending child transactions grouped by pending parent transaction.
    child_txs_by_parent: BTreeMap<TxId, BTreeSet<TxId>>,
}

/// Authenticated identity attached to an inbound commit-unit upload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommitUnitIngestContext {
    /// Identity authenticated by the connection carrying the upload.
    pub identity: AuthorSubject,
    /// Whether the connection may attribute writes to a different `made_by`.
    pub trust: CommitUnitTrust,
    /// Whether this subscriber link is hosted by an edge authority.
    pub edge_authority: bool,
    /// The authenticated connection admission path has already proved every
    /// terminal write clause against its immutable delegated session binding.
    /// This may only be set by the peer-connection authority path immediately
    /// after that proof; wire messages cannot carry it.
    pub(crate) admitted_write_authorization: bool,
}

/// Trust mode for an inbound commit-unit upload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitUnitTrust {
    /// Session/client links must honestly set `made_by` to the link identity.
    Session,
    /// An authenticated relay transport. It has no permission subject and
    /// may carry a topology-assigned delegated session only for the exact
    /// request that needs authority-side policy composition.
    Relay,
    /// Trusted backends may preserve user provenance in `made_by`.
    TrustedBackend,
    /// Authenticated authority control-plane link. Ordinary writes retain
    /// their permission subject; only complete authority publications carry
    /// a prior edge-admission proof. Never inferred from a wire identity.
    TrustedAuthority,
    /// Administrators may preserve provenance and bypass application write policies.
    TrustedAdmin,
}

impl CommitUnitTrust {
    pub(crate) fn is_trusted(self) -> bool {
        matches!(
            self,
            Self::TrustedBackend | Self::TrustedAuthority | Self::TrustedAdmin
        )
    }
}

include!("state/lifecycle.rs");
include!("state/commit.rs");
include!("state/contribution_merge.rs");
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
    publication_fields: std::sync::Arc<Vec<CurrentRowPublicationField>>,
}

/// Constructor-time source or logical role before publication is finalized.
///
/// This role is never serialized. The single publication metadata owner below
/// carries authoritative catalogue IDs, names and application-cell visibility.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CurrentRowBindingRole {
    /// A persisted CurrentRow field using Jazz's private physical name.
    PhysicalColumn,
    /// A query, relation, or collector field using its public logical name.
    LogicalField,
}

/// Application cells, public provenance, and private engine metadata have
/// different publication roles even when their names happen to be identical.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CurrentRowResultVisibility {
    /// A query-visible application cell, included in subscription cell comparison.
    ApplicationCell,
    /// Public magic provenance, available to explicit projections and row metadata.
    PublicProvenance,
    /// Engine bookkeeping carried only for decoding/internal identity.
    HiddenMetadata,
}

impl CurrentRowResultVisibility {
    /// Classify metadata constructed by the CurrentRow producer, not a wire
    /// field guessed by a consumer. Explicit application outputs bypass this.
    pub(crate) fn current_row_metadata(name: &str) -> Self {
        match name {
            "$createdBy" | "$createdAt" | "$updatedBy" | "$updatedAt" => Self::PublicProvenance,
            _ => Self::HiddenMetadata,
        }
    }
}

/// One producer-owned publication binding. Runtime query slots are separate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CurrentRowPublicationField {
    /// A source application cell with its authoritative catalogue identity.
    StoredColumn {
        /// Exact catalogue column identity.
        id: PhysicalColumnId,
        /// Application output name, including explicit aliases.
        output_name: String,
    },
    /// A derived or metadata field with an explicit name and visibility.
    ResultField {
        /// Exact name sent to the host.
        name: String,
        /// Explicit application/provenance/internal role assigned by the producer.
        visibility: CurrentRowResultVisibility,
    },
    /// Construction-only source cell, resolved before publication.
    UnresolvedSourceCell {
        /// Source application name in the selected read schema.
        output_name: String,
    },
}

impl CurrentRowPublicationField {
    pub(crate) fn public_name(&self) -> Option<&str> {
        match self {
            Self::StoredColumn { output_name, .. } | Self::UnresolvedSourceCell { output_name } => {
                Some(output_name)
            }
            Self::ResultField {
                name,
                visibility:
                    CurrentRowResultVisibility::ApplicationCell
                    | CurrentRowResultVisibility::PublicProvenance,
            } => Some(name),
            Self::ResultField {
                visibility: CurrentRowResultVisibility::HiddenMetadata,
                ..
            } => None,
        }
    }

    pub(crate) fn application_name(&self) -> Option<&str> {
        match self {
            Self::StoredColumn { output_name, .. } | Self::UnresolvedSourceCell { output_name } => {
                Some(output_name)
            }
            Self::ResultField {
                name,
                visibility: crate::node::CurrentRowResultVisibility::ApplicationCell,
            } => Some(name),
            Self::ResultField { .. } => None,
        }
    }

    fn role(&self) -> CurrentRowBindingRole {
        match self {
            Self::StoredColumn { .. } | Self::UnresolvedSourceCell { .. } => {
                CurrentRowBindingRole::PhysicalColumn
            }
            Self::ResultField { .. } => CurrentRowBindingRole::LogicalField,
        }
    }
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
    pub created_by: AuthorSubject,
    /// Unix milliseconds of the row's first retained content version.
    pub created_at: u64,
    /// Principal that authored the visible row version.
    pub updated_by: AuthorSubject,
    /// Unix milliseconds of the visible row version.
    pub updated_at: u64,
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
        Self::new_with_binding_fields(table, record, CurrentRowBindingRole::PhysicalColumn)
    }

    pub(crate) fn new_with_binding_fields(
        table: impl Into<String>,
        record: OwnedRecord,
        default_field: CurrentRowBindingRole,
    ) -> Self {
        let binding_fields = vec![default_field; record.descriptor().fields().len()];
        Self::new_with_explicit_binding_fields(table, record, binding_fields)
    }

    pub(crate) fn new_with_explicit_binding_fields(
        table: impl Into<String>,
        record: OwnedRecord,
        binding_fields: Vec<CurrentRowBindingRole>,
    ) -> Self {
        let binding_field_names = record
            .descriptor()
            .fields()
            .iter()
            .zip(&binding_fields)
            .map(|(field, binding)| match binding {
                CurrentRowBindingRole::PhysicalColumn => None,
                CurrentRowBindingRole::LogicalField => {
                    self::query_engine::descriptor_public_name(field).map(str::to_owned)
                }
            })
            .collect();
        Self::new_with_explicit_binding_fields_and_names(
            table,
            record,
            binding_fields,
            binding_field_names,
        )
    }

    /// Construct a row with explicit binding provenance and any logical
    /// descriptor-name overrides supplied by its producer.
    pub(crate) fn new_with_explicit_binding_fields_and_names(
        table: impl Into<String>,
        record: OwnedRecord,
        binding_fields: Vec<CurrentRowBindingRole>,
        binding_field_names: Vec<Option<String>>,
    ) -> Self {
        assert_eq!(
            binding_fields.len(),
            record.descriptor().fields().len(),
            "native binding fields must align with their record descriptor"
        );
        assert_eq!(
            binding_field_names.len(),
            binding_fields.len(),
            "native binding field names must align with their record descriptor"
        );
        let publication_fields = record
            .descriptor()
            .fields()
            .iter()
            .zip(binding_fields)
            .zip(binding_field_names)
            .map(|((field, role), public_name)| {
                let name = field.name.as_deref().expect("current row fields are named");
                match role {
                    CurrentRowBindingRole::PhysicalColumn => {
                        match public_name
                            .or_else(|| Self::physical_application_name(field).map(str::to_owned))
                        {
                            Some(output_name) => {
                                CurrentRowPublicationField::UnresolvedSourceCell { output_name }
                            }
                            None => CurrentRowPublicationField::ResultField {
                                name: name.to_owned(),
                                visibility: CurrentRowResultVisibility::current_row_metadata(name),
                            },
                        }
                    }
                    CurrentRowBindingRole::LogicalField => {
                        CurrentRowPublicationField::ResultField {
                            visibility: if public_name.is_some() {
                                CurrentRowResultVisibility::ApplicationCell
                            } else {
                                CurrentRowResultVisibility::current_row_metadata(name)
                            },
                            name: public_name.unwrap_or_else(|| name.to_owned()),
                        }
                    }
                }
            })
            .collect();
        Self::new_with_publication_fields(table, record, publication_fields)
    }

    pub(crate) fn new_with_publication_fields(
        table: impl Into<String>,
        record: OwnedRecord,
        publication_fields: Vec<CurrentRowPublicationField>,
    ) -> Self {
        assert_eq!(
            publication_fields.len(),
            record.descriptor().fields().len(),
            "publication fields must align with record slots"
        );
        Self {
            table: groove::Intern::new(table.into()),
            record: std::sync::Arc::new(record),
            deleted: false,
            publication_fields: std::sync::Arc::new(publication_fields),
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

    /// The explicit native-binding provenance of every encoded descriptor field.
    #[doc(hidden)]
    pub fn binding_fields(&self) -> Vec<CurrentRowBindingRole> {
        self.publication_fields
            .iter()
            .map(CurrentRowPublicationField::role)
            .collect()
    }

    /// Producer-owned publication metadata, aligned with descriptor slots.
    #[doc(hidden)]
    pub fn publication_fields(&self) -> &[CurrentRowPublicationField] {
        &self.publication_fields
    }

    /// Logical public names derived from the single publication metadata owner.
    #[doc(hidden)]
    pub fn binding_field_names(&self) -> Vec<Option<&str>> {
        self.publication_fields
            .iter()
            .map(|field| match field {
                CurrentRowPublicationField::ResultField {
                    name,
                    visibility:
                        CurrentRowResultVisibility::ApplicationCell
                        | CurrentRowResultVisibility::PublicProvenance,
                } => Some(name.as_str()),
                _ => None,
            })
            .collect()
    }

    /// Logical table name.
    pub fn table(&self) -> &str {
        self.table.as_str()
    }

    /// Row id.
    pub fn row_uuid(&self) -> RowUuid {
        let row_idx = self
            .record
            .descriptor()
            .field_index("row_uuid")
            .unwrap_or(CurrentRowRecord::FIELD_ROW_UUID_IDX);
        RowUuid(
            self.record
                .borrowed()
                .get_uuid(row_idx)
                .expect("valid current row_uuid"),
        )
    }

    /// Cell value by application-schema column position.
    pub fn cell_at(&self, column_position: usize) -> Option<Value> {
        let user_cells = self
            .record
            .descriptor()
            .field_index("row_uuid")
            .map_or(0, |idx| idx + 1);
        match self
            .record
            .borrowed()
            .get_idx(user_cells + column_position)
            .expect("valid current user cell")
        {
            Value::Nullable(None) => None,
            Value::Nullable(Some(value)) => Some(*value),
            value => Some(value),
        }
    }

    /// Cell value by application column name using the table schema to resolve position.
    pub fn cell(&self, table: &TableSchema, column: &str) -> Option<Value> {
        let idx = self.application_column_index(table, column)?;
        match self.record.borrowed().get_idx(idx).ok()? {
            Value::Nullable(None) => None,
            Value::Nullable(Some(value)) => Some(*value),
            value => Some(value),
        }
    }

    /// Resolve an application-schema column through explicit descriptor-field
    /// provenance. A logical field may legitimately be named `user_{column}`;
    /// it must never shadow the physical current-row column of that name.
    fn application_column_index(&self, table: &TableSchema, column: &str) -> Option<usize> {
        let _ = table
            .columns
            .iter()
            .find(|candidate| candidate.name == column)?;
        self.application_column_index_by_name(column)
    }

    fn application_column_index_by_name(&self, column: &str) -> Option<usize> {
        for role in [
            CurrentRowBindingRole::PhysicalColumn,
            CurrentRowBindingRole::LogicalField,
        ] {
            if let Some(index) = self
                .publication_fields
                .iter()
                .position(|field| field.role() == role && field.application_name() == Some(column))
            {
                return Some(index);
            }
        }
        None
    }

    fn physical_application_name(field: &records::DescriptorField) -> Option<&str> {
        let name = self::query_engine::descriptor_public_name(field)?;
        if field.name.as_deref() == Some(name) {
            // This conversion is confined to the explicitly tagged physical
            // CurrentRow boundary, never used to resolve a graph field.
            name.strip_prefix(self::query_engine::USER_COLUMN_PREFIX)
        } else {
            Some(name)
        }
    }

    /// Encoded groove record backing this projected current row.
    pub fn encoded_record(&self) -> (&records::RecordDescriptor, &[u8]) {
        (self.record.descriptor(), self.record.raw())
    }

    /// Read one application field through its explicit publication binding.
    /// Unlike a descriptor/carrier lookup this remains exact when a literal
    /// application name equals another field's generated storage carrier.
    #[cfg(feature = "runtime")]
    pub(crate) fn application_field(&self, name: &str) -> Option<Value> {
        let index = self.application_column_index_by_name(name)?;
        self.record.borrowed().get_idx(index).ok()
    }

    pub(crate) fn raw_field(&self, field: &str) -> Option<Value> {
        let idx = self.record.descriptor().field_index(field)?;
        self.record.borrowed().get_idx(idx).ok()
    }

    /// Locate one provenance field without requiring unrelated projected fields.
    /// Storage records use physical names; query records use magic-column names.
    fn provenance_field_index(&self, column: &str) -> Option<usize> {
        let stored_column = match column {
            "$createdBy" => "created_by",
            "$createdAt" => "created_at",
            "$updatedBy" => "updated_by",
            "$updatedAt" => "updated_at",
            _ => return None,
        };
        let descriptor = self.record.descriptor();
        descriptor.field_index(column).or_else(|| {
            (descriptor.field_index("schema_version").is_some()
                && descriptor.field_index("branch_key").is_some())
            .then(|| descriptor.field_index(stored_column))
            .flatten()
        })
    }

    #[cfg(feature = "runtime")]
    pub(crate) fn provenance_value(&self, column: &str) -> Result<Option<Value>, Error> {
        let Some(index) = self.provenance_field_index(column) else {
            return Ok(None);
        };
        let record = self.record.borrowed();
        Ok(Some(match column {
            "$createdBy" | "$updatedBy" => {
                let author = AuthorSubject::from_canonical(record.get_str(index)?)
                    .map_err(|_| groove::records::Error::NonCanonicalRecord)?;
                Value::String(author.canonical().to_owned())
            }
            "$createdAt" | "$updatedAt" => Value::U64(record.get_u64(index)?),
            _ => unreachable!("provenance_field_index accepts only provenance columns"),
        }))
    }

    pub(crate) fn provenance(&self) -> Result<Option<RowProvenance>, Error> {
        let borrowed = self.record.borrowed();
        let indices = match (
            self.provenance_field_index("$createdBy"),
            self.provenance_field_index("$createdAt"),
            self.provenance_field_index("$updatedBy"),
            self.provenance_field_index("$updatedAt"),
        ) {
            (Some(created_by), Some(created_at), Some(updated_by), Some(updated_at)) => {
                Some((created_by, created_at, updated_by, updated_at))
            }
            _ => None,
        };
        let Some((created_by_idx, created_at_idx, updated_by_idx, updated_at_idx)) = indices else {
            return Ok(None);
        };
        Ok(Some(RowProvenance {
            created_by: AuthorSubject::from_canonical(borrowed.get_str(created_by_idx)?)
                .map_err(|_| groove::records::Error::NonCanonicalRecord)?,
            created_at: borrowed.get_u64(created_at_idx)?,
            updated_by: AuthorSubject::from_canonical(borrowed.get_str(updated_by_idx)?)
                .map_err(|_| groove::records::Error::NonCanonicalRecord)?,
            updated_at: borrowed.get_u64(updated_at_idx)?,
        }))
    }

    pub(crate) fn project(&self, table: &TableSchema, columns: &[String]) -> Result<Self, Error> {
        let projected_public_name = |column_name: &str| -> String {
            if self.application_column_index_by_name(column_name).is_some() {
                column_name.to_owned()
            } else {
                self::query_engine::aggregate_output_logical_name(column_name)
                    .filter(|logical| self.application_column_index_by_name(logical).is_some())
                    .unwrap_or(column_name)
                    .to_owned()
            }
        };
        let selected = columns.iter().map(String::as_str).collect::<BTreeSet<_>>();
        let projected_columns = table
            .columns
            .iter()
            .filter(|column| selected.contains(column.name.as_str()))
            .collect::<Vec<_>>();
        let mut descriptor_fields = vec![records::DescriptorField::new(
            "row_uuid",
            records::ValueType::Uuid,
        )];
        descriptor_fields.extend(projected_columns.iter().map(|column| {
            let public_name = projected_public_name(&column.name);
            records::DescriptorField::new(
                user_column_field(&column.name),
                records::ValueType::Nullable(Box::new(column.column_type.clone())),
            )
            .with_identity(records::FieldIdentity::Name(public_name))
        }));
        descriptor_fields.extend([
            records::DescriptorField::new("$createdBy", records::ValueType::String),
            records::DescriptorField::new("$createdAt", records::ValueType::U64),
            records::DescriptorField::new("$updatedBy", records::ValueType::String),
            records::DescriptorField::new("$updatedAt", records::ValueType::U64),
            records::DescriptorField::new("tx_time", records::ValueType::U64),
            records::DescriptorField::new("tx_node_id", records::ValueType::U64),
        ]);
        let descriptor = records::RecordDescriptor::new_with_fields(descriptor_fields);
        let mut values = vec![Value::Uuid(self.row_uuid().0)];
        let row_uuid_field = self
            .record
            .descriptor()
            .field_index("row_uuid")
            .and_then(|index| {
                self.publication_fields
                    .get(index)
                    .map(CurrentRowPublicationField::role)
            })
            .unwrap_or(CurrentRowBindingRole::LogicalField);
        let mut binding_fields = vec![row_uuid_field];
        let mut binding_field_names = vec![None];
        for column in projected_columns {
            let public_name = projected_public_name(&column.name);
            let cell = self
                .application_column_index_by_name(&public_name)
                .and_then(|idx| self.record.borrowed().get_idx(idx).ok())
                .and_then(|value| match value {
                    Value::Nullable(None) => None,
                    Value::Nullable(Some(value)) => Some(*value),
                    value => Some(value),
                });
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
            let binding = self
                .application_column_index_by_name(&public_name)
                .and_then(|index| {
                    self.publication_fields
                        .get(index)
                        .map(CurrentRowPublicationField::role)
                })
                .unwrap_or(CurrentRowBindingRole::LogicalField);
            binding_fields.push(binding);
            binding_field_names.push(Some(public_name));
        }
        if let Some(provenance) = self.provenance()? {
            values.push(Value::String(provenance.created_by.canonical().to_owned()));
            values.push(Value::U64(provenance.created_at));
            values.push(Value::String(provenance.updated_by.canonical().to_owned()));
            values.push(Value::U64(provenance.updated_at));
            binding_fields.extend(
                ["$createdBy", "$createdAt", "$updatedBy", "$updatedAt"]
                    .into_iter()
                    .map(|column| {
                        self.provenance_field_index(column)
                            .and_then(|index| {
                                self.publication_fields
                                    .get(index)
                                    .map(CurrentRowPublicationField::role)
                            })
                            .unwrap_or(CurrentRowBindingRole::LogicalField)
                    }),
            );
            binding_field_names.extend(std::iter::repeat_n(None, 4));
        } else {
            values.push(Value::String(AuthorSubject::SYSTEM.canonical().to_owned()));
            values.push(Value::U64(0));
            values.push(Value::String(AuthorSubject::SYSTEM.canonical().to_owned()));
            values.push(Value::U64(0));
            binding_fields.extend([CurrentRowBindingRole::LogicalField; 4]);
            binding_field_names.extend(std::iter::repeat_n(None, 4));
        }
        if let Some((time, node)) = self.projected_tx_alias() {
            values.push(Value::U64(time.0));
            values.push(Value::U64(node.0));
        } else {
            values.push(Value::U64(0));
            values.push(Value::U64(0));
        }
        binding_fields.extend([CurrentRowBindingRole::LogicalField; 2]);
        binding_field_names.extend(std::iter::repeat_n(None, 2));
        let raw = descriptor.create(&values)?;
        let mut projected = Self::new_with_explicit_binding_fields_and_names(
            table.name.clone(),
            OwnedRecord::new(raw, descriptor),
            binding_fields,
            binding_field_names,
        );
        let fields = std::sync::Arc::make_mut(&mut projected.publication_fields);
        for field in fields {
            let Some(name) = field.application_name() else {
                continue;
            };
            if let Some(index) = self.application_column_index_by_name(name) {
                *field = self.publication_fields[index].clone();
            }
        }
        Ok(projected)
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

    /// Compare the row data visible to a subscription, independent of the
    /// physical descriptor that happened to materialize it.
    ///
    /// A maintained view may carry an unchanged row first from its physical
    /// current relation and then from a public policy/query projection. The
    /// storage-only fields differ between those descriptors, but emitting an
    /// update for that representation change would turn a policy no-op (such
    /// as reordering an array used only as a reverse-inheritance grant) into a
    /// spurious application-visible row update.
    pub(crate) fn subscription_equivalent(&self, other: &Self) -> bool {
        let provenance_matches = match (self.provenance(), other.provenance()) {
            (Ok(left), Ok(right)) => left == right,
            _ => false,
        };
        self.table == other.table
            && self.row_uuid() == other.row_uuid()
            && self.deleted == other.deleted
            && self.subscription_cells_equivalent(other)
            && provenance_matches
    }

    fn subscription_cells_equivalent(&self, other: &Self) -> bool {
        // Decode each cell exactly once. Descriptor order differs between a
        // physical current row and its public projection, so canonicalize the
        // borrowed logical names instead of repeatedly rescanning either row.
        match (
            self.canonical_subscription_cells(),
            other.canonical_subscription_cells(),
        ) {
            (Some(left), Some(right)) => left == right,
            _ => false,
        }
    }

    fn canonical_subscription_cells(&self) -> Option<Vec<(&str, Vec<u8>)>> {
        let mut cells = self
            .subscription_cells()
            .map(|(name, value)| Some((name, postcard::to_allocvec(&value).ok()?)))
            .collect::<Option<Vec<_>>>()?;
        // Logical names may legally collide (for example a group column and
        // an aggregate alias). The canonical Value bytes preserve multiset
        // semantics without making equality depend on descriptor order.
        cells.sort_unstable();
        Some(cells)
    }

    fn subscription_cells(&self) -> impl Iterator<Item = (&str, Option<Value>)> + '_ {
        let borrowed = self.record.borrowed();
        self.publication_fields
            .iter()
            .enumerate()
            .filter_map(move |(idx, field)| {
                let name = field.application_name()?;
                let value = match borrowed.get_idx(idx).ok()? {
                    Value::Nullable(value) => value.map(|value| *value),
                    value => Some(value),
                };
                Some((name, value))
            })
    }

    #[cfg(test)]
    pub(crate) fn test_cells_by_descriptor(&self) -> BTreeMap<String, Value> {
        // Expected test maps describe application cells. Compiler carrier names
        // and metadata visibility are not an alternate publication contract.
        self.subscription_cells()
            .filter_map(|(name, value)| value.map(|value| (name.to_owned(), value)))
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
    S: OrderedKvStorage,
{
    node: &'node mut NodeState<S>,
    position: GlobalTime,
}

impl<S> HistoricalRead<'_, S>
where
    S: OrderedKvStorage,
{
    /// Global settle position this handle reads at.
    pub fn position(&self) -> GlobalTime {
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
    /// Historical/branch-base source resolutions that used a bounded global-time range.
    pub source_global_time_range_scans: u64,
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

/// One row selected for an atomic cross-branch contribution merge.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContributionMergeRow {
    /// Logical table containing the row.
    pub table: String,
    /// Global object identity shared by its branch-local branch-local rows.
    pub row_uuid: RowUuid,
}

/// Explicit source and target views for a local contribution calculation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContributionMergeRequest {
    /// Full named source branch selector.
    pub source: BranchSelector,
    /// Full named target branch selector.
    pub target: BranchSelector,
    /// Rows calculated and committed atomically.
    pub rows: Vec<ContributionMergeRow>,
    /// Author of the ordinary output transaction.
    pub made_by: AuthorSubject,
    /// Identity used by ordinary target write policy.
    pub permission_subject: Option<AuthorSubject>,
    /// Abstract wall clock at the calculating node.
    pub now_ms: u64,
}

/// Builder for a local mergeable commit.
#[derive(Clone)]
pub struct MergeableCommit {
    /// Target table.
    pub table: String,
    /// Target row.
    pub row_uuid: RowUuid,
    /// Exact named branch coordinate for this row branch-local row.
    pub branch: BranchSelector,
    /// Author making the commit.
    pub made_by: AuthorSubject,
    /// Identity used for write-policy evaluation.
    pub permission_subject: Option<AuthorSubject>,
    /// Abstract wall clock at the committing node.
    pub now_ms: u64,
    /// User cells for content versions.
    pub cells: BTreeMap<String, Value>,
    /// Explicitly authored content columns. `None` means every supplied cell.
    pub authored_columns: Option<BTreeSet<String>>,
    /// Deletion-register event, if any.
    pub deletion: Option<DeletionEvent>,
    /// Exact prior versions of this same physical row and layer.
    ///
    /// Version parents describe only row-history ancestry: they are neither a
    /// general transaction-dependency graph nor a way to express an observed
    /// state precondition. In particular, content and deletion registers have
    /// independent parent chains. A read/CAS precondition belongs to an
    /// exclusive transaction's read set instead.
    pub parents: Vec<TxId>,
    /// Optional application metadata.
    pub user_metadata_json: Option<String>,
    /// Columns carrying Groove preparations staged through this node. Private
    /// provenance prevents callers from handcrafting physical descriptors.
    prepared_large_columns: BTreeSet<String>,
    staged_large_values: Vec<groove::large_values::StagedLargeValueId>,
    /// Construction-time proof that the production UUID source generated this
    /// insert's row id.
    /// Kept private so direct commits and replicated writes cannot assert it.
    known_fresh_row: bool,
}

impl MergeableCommit {
    /// Construct an empty mergeable commit builder.
    pub fn new(table: impl Into<String>, row_uuid: RowUuid, now_ms: u64) -> Self {
        Self {
            table: table.into(),
            row_uuid,
            branch: BranchSelector::default(),
            made_by: AuthorSubject::SYSTEM,
            permission_subject: None,
            now_ms,
            cells: BTreeMap::new(),
            authored_columns: None,
            deletion: None,
            parents: Vec::new(),
            user_metadata_json: None,
            prepared_large_columns: BTreeSet::new(),
            staged_large_values: Vec::new(),
            known_fresh_row: false,
        }
    }

    pub(crate) fn known_fresh_row(mut self) -> Self {
        self.known_fresh_row = true;
        self
    }

    /// Target an exact branch-keyed row branch-local row.
    pub fn branch(mut self, branch: BranchSelector) -> Self {
        self.branch = branch;
        self
    }

    /// Set the commit author.
    pub fn made_by(mut self, made_by: AuthorSubject) -> Self {
        self.made_by = made_by;
        self
    }

    /// Set the authenticated identity used for write policy.
    pub fn permission_subject(mut self, permission_subject: AuthorSubject) -> Self {
        self.permission_subject = Some(permission_subject);
        self
    }

    pub(crate) fn effective_permission_subject(&self) -> AuthorSubject {
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

    /// Attach Jazz-private provenance for a Groove-staged large scalar. This
    /// remains crate-private so public callers cannot bless handcrafted
    /// descriptors.
    pub(crate) fn staged_large_cell(
        mut self,
        column: impl Into<String>,
        staged: groove::large_values::StagedLargeValue,
        nullable: bool,
    ) -> Self {
        let column = column.into();
        let value = Value::Large(staged.value_ref);
        self.cells.insert(
            column.clone(),
            if nullable {
                Value::Nullable(Some(Box::new(value)))
            } else {
                value
            },
        );
        self.prepared_large_columns.insert(column);
        self.staged_large_values.push(staged.id);
        self
    }

    /// Carry an engine-proven physical preimage into a branch-view write.
    ///
    /// A branch overlay starts with cells read from its base. An unchanged
    /// indirect descriptor in that snapshot is not caller-authored: it was
    /// already admitted by the local physical read that produced `inherited`.
    /// Mark only an exact still-present cell, so a later patch cannot use this
    /// provenance to smuggle in a different descriptor.
    pub(crate) fn verified_inherited_large_cells(
        mut self,
        inherited: &BTreeMap<String, Value>,
    ) -> Self {
        for (column, value) in inherited {
            if value_contains_indirect_descriptor(value) && self.cells.get(column) == Some(value) {
                self.prepared_large_columns.insert(column.clone());
            }
        }
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

    /// Set exact same-row/layer history parents.
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
        crate::time::TxTime::from_physical_ms(self.now_ms).map_err(|_| {
            Error::InvalidMergeableCommit(
                "commit now_ms exceeds packed HLC physical-millisecond range",
            )
        })?;
        validate_mergeable_write_shape(self.cells.is_empty(), self.deletion.is_some())?;
        codec::validate_parent_tx_ids(&self.parents)?;
        if self.cells.iter().any(|(column, value)| {
            value_contains_indirect_descriptor(value)
                && !self.prepared_large_columns.contains(column)
        }) {
            return Err(Error::InvalidMergeableCommit(
                "callers must author logical scalar values, not physical large descriptors",
            ));
        }
        Ok(())
    }
}

fn value_contains_indirect_descriptor(value: &Value) -> bool {
    match value {
        Value::Large(_) => true,
        Value::Tuple(values) | Value::Array(values) => {
            values.iter().any(value_contains_indirect_descriptor)
        }
        Value::Nullable(Some(value)) => value_contains_indirect_descriptor(value),
        Value::Record(record) => record
            .to_values()
            .is_ok_and(|values| values.iter().any(value_contains_indirect_descriptor)),
        Value::Enum(value) => value
            .record()
            .to_values()
            .is_ok_and(|values| values.iter().any(value_contains_indirect_descriptor)),
        _ => false,
    }
}

fn collect_indirect_descriptors(
    value: &Value,
    descriptors: &mut Vec<groove::large_values::LargeValueRef>,
) {
    match value {
        Value::Large(value_ref) => {
            if !descriptors.contains(value_ref) {
                descriptors.push(value_ref.clone());
            }
        }
        Value::Tuple(values) | Value::Array(values) => {
            for value in values {
                collect_indirect_descriptors(value, descriptors);
            }
        }
        Value::Nullable(Some(value)) => collect_indirect_descriptors(value, descriptors),
        Value::Record(record) => {
            if let Ok(values) = record.to_values() {
                for value in values {
                    collect_indirect_descriptors(&value, descriptors);
                }
            }
        }
        Value::Enum(value) => {
            if let Ok(values) = value.record().to_values() {
                for value in values {
                    collect_indirect_descriptors(&value, descriptors);
                }
            }
        }
        _ => {}
    }
}

fn version_indirect_descriptors(
    versions: &[VersionRecord],
) -> Vec<groove::large_values::LargeValueRef> {
    let mut descriptors = Vec::new();
    for version in versions {
        for position in 0..version.application_cell_count() {
            if let Some(value) = version.cell_at(position) {
                collect_indirect_descriptors(&value, &mut descriptors);
            }
        }
    }
    descriptors
}

pub(crate) struct ViewUpdateParts {
    pub(crate) subscription: SubscriptionKey,
    pub(crate) settled_through: GlobalTime,
    pub(crate) defer_settlement: bool,
    pub(crate) reset_result_set: bool,
    pub(crate) version_carriers: Vec<VersionCarrier>,
    pub(crate) peer_complete_tx_payload_refs: Vec<TxId>,
    pub(crate) authorization_progress: Option<u64>,
    pub(crate) opening_pending: bool,
    pub(crate) result_member_adds: Vec<ResultMemberEntry>,
    pub(crate) result_member_removes: Vec<ResultMemberEntry>,
    pub(crate) program_fact_adds: Vec<ViewFactEntry>,
    pub(crate) program_fact_removes: Vec<ViewFactEntry>,
}

#[derive(Default)]
struct IngestMemo {
    tx_exists: BTreeMap<TxId, bool>,
    tx_made_at: BTreeMap<TxId, Option<TxTime>>,
}

/// A Jazz transaction whose resident Groove publication is visible while its
/// owned durable write is still pending.
/// A transaction that is resident and locally visible, with persistence still
/// owned by the caller.
#[must_use = "a published transaction must be persisted and settled"]
pub struct PublishedTransaction {
    pub(crate) tx_id: TxId,
    persistence: AppliedBatch,
}

impl PublishedTransaction {
    /// The transaction made resident by this publication.
    pub fn tx_id(&self) -> TxId {
        self.tx_id
    }

    /// Persist the resident publication in storage order.
    pub async fn persist(&self) -> PersistedBatch {
        self.persistence.persist().await
    }
}

/// The logical result of an operation together with every resident write that
/// must be observed locally before it is persisted and released externally.
/// A logical operation result and the resident publications it created.
#[must_use = "publication outcomes must be persisted and settled"]
pub struct PublicationOutcome<T> {
    pub(crate) value: T,
    pub(crate) publications: Vec<PublishedTransaction>,
    /// Same-node messages that may enter normal ingest only after every
    /// publication ahead of them has settled successfully.
    pub(crate) post_settlement_work: VecDeque<SyncMessage>,
}

impl<T> PublicationOutcome<T> {
    /// Borrow the logical operation result without consuming its receipts.
    pub fn value(&self) -> &T {
        &self.value
    }

    /// Split the logical result from the publications awaiting persistence.
    pub fn into_parts(self) -> (T, Vec<PublishedTransaction>, VecDeque<SyncMessage>) {
        (self.value, self.publications, self.post_settlement_work)
    }

    pub(crate) fn settled(value: T) -> Self {
        Self {
            value,
            publications: Vec::new(),
            post_settlement_work: VecDeque::new(),
        }
    }

    pub(crate) fn published(value: T, publication: PublishedTransaction) -> Self {
        Self {
            value,
            publications: vec![publication],
            post_settlement_work: VecDeque::new(),
        }
    }

    pub(crate) fn published_then(
        value: T,
        publication: PublishedTransaction,
        work: SyncMessage,
    ) -> Self {
        Self {
            value,
            publications: vec![publication],
            post_settlement_work: VecDeque::from([work]),
        }
    }
}

impl<T> PublicationOutcome<Vec<T>> {
    pub(crate) fn extend(&mut self, other: Self) {
        self.append_outcome(other);
    }

    pub(crate) fn append_outcome(&mut self, mut other: Self) {
        self.value.append(&mut other.value);
        self.publications.append(&mut other.publications);
        self.post_settlement_work
            .append(&mut other.post_settlement_work);
    }
}

struct CatalogueOpenState {
    storage: BoxedStorage,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
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
    /// Fail after physical table/variant registration but before the coupled
    /// projection registry is complete.  This proves registry synchronization
    /// rolls its live in-memory mutations back as one unit.
    AfterPhysicalRegistryRegistration,
    AfterRegistration,
    BeforeSnapshotActivationCommit,
}

#[derive(Clone, Debug)]
pub(crate) enum PreparedQueryPlan {
    Graph {
        graph: GraphBuilder,
        output: query_engine::AppRowSchema,
    },
    Prepared {
        shape: PreparedShapeId,
        params: Vec<PreparedQueryParam>,
        output: query_engine::AppRowSchema,
    },
    PeerMaintainedMarker,
}

impl PreparedQueryPlan {
    fn app_row_schema(&self) -> Option<&query_engine::AppRowSchema> {
        match self {
            Self::Graph { output, .. } | Self::Prepared { output, .. } => Some(output),
            Self::PeerMaintainedMarker => None,
        }
    }
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

/// Durable v1 identity for authority-selected results.
///
/// This intentionally uses normal Groove values, not postcard or a variable
/// claim payload: `[shape, binding, read-view, policy-absent-or-present,
/// 32-byte policy handle]`. The handle names a separate typed directory entry
/// that preserves the exact subject and claims and rejects a collision during
/// both write and recovery.
fn authority_result_store_prefix(authority_result_key: &AuthorityResultKey) -> Vec<Value> {
    let binding_view = authority_result_key.binding_view;
    let mut key = vec![
        Value::Uuid(binding_view.shape_id.0),
        Value::Uuid(binding_view.binding_id.0),
        Value::Uuid(binding_view.read_view.id),
    ];
    match &authority_result_key.policy_binding {
        None => {
            key.push(Value::U8(0));
            key.push(Value::Bytes(vec![0; 32]));
        }
        Some(policy) => {
            key.push(Value::U8(1));
            key.push(Value::Bytes(policy.directory_digest().to_vec()));
        }
    }
    key
}

fn known_state_fact_key(authority_result_key: &AuthorityResultKey) -> Vec<Value> {
    authority_result_store_prefix(authority_result_key)
}

fn settled_result_member_key(
    authority_result_key: &AuthorityResultKey,
    member: &ResultMemberEntry,
) -> Result<Vec<Value>, Error> {
    let member_bytes = codec::result_member_storage_bytes(member)?;
    let mut key = authority_result_store_prefix(authority_result_key);
    key.push(Value::Bytes(
        settled_result_member_digest(&member_bytes).to_vec(),
    ));
    Ok(key)
}

/// Domain-separated identity for one settled result member.  A member may
/// contain an application-controlled synthetic payload, so its canonical bytes
/// belong in the direct-store value rather than an ordered storage key.  The
/// fixed-size digest retains idempotent add/remove semantics without allowing
/// a large member to make an IDB/B-tree key exceed its page bound.
const SETTLED_RESULT_MEMBER_DIGEST_DOMAIN: &str = "jazz.settled-result-member-key.v1";

fn settled_result_member_digest(member_bytes: &[u8]) -> [u8; 32] {
    blake3::derive_key(SETTLED_RESULT_MEMBER_DIGEST_DOMAIN, member_bytes)
}

fn settled_result_member_storage_write(
    authority_result_key: &AuthorityResultKey,
    member: &ResultMemberEntry,
) -> Result<groove::db::DirectRecordStoreWrite, Error> {
    let member_bytes = codec::result_member_storage_bytes(member)?;
    let mut key = authority_result_store_prefix(authority_result_key);
    key.push(Value::Bytes(
        settled_result_member_digest(&member_bytes).to_vec(),
    ));
    Ok(groove::db::DirectRecordStoreWrite::Set {
        key,
        value: vec![Value::Bytes(member_bytes)],
    })
}

fn settled_program_fact_key(
    authority_result_key: &AuthorityResultKey,
    fact: &ViewFactEntry,
) -> Result<Vec<Value>, Error> {
    let mut key = authority_result_store_prefix(authority_result_key);
    key.push(Value::Bytes(
        settled_program_fact_digest(&codec::program_fact_storage_bytes(fact)?).to_vec(),
    ));
    Ok(key)
}

/// Domain-separated identity for a settled program fact. Facts can include a
/// hydrated application payload, so the full canonical encoding belongs in a
/// value cell. The key keeps only this fixed-size identity, preserving ordered
/// B-tree page bounds while retaining exact payload validation on recovery.
const SETTLED_PROGRAM_FACT_DIGEST_DOMAIN: &str = "jazz.settled-program-fact-key.v1";

fn settled_program_fact_digest(fact_bytes: &[u8]) -> [u8; 32] {
    blake3::derive_key(SETTLED_PROGRAM_FACT_DIGEST_DOMAIN, fact_bytes)
}

fn settled_program_fact_storage_write(
    authority_result_key: &AuthorityResultKey,
    fact: &ViewFactEntry,
) -> Result<groove::db::DirectRecordStoreWrite, Error> {
    let fact_bytes = codec::program_fact_storage_bytes(fact)?;
    let mut key = authority_result_store_prefix(authority_result_key);
    key.push(Value::Bytes(
        settled_program_fact_digest(&fact_bytes).to_vec(),
    ));
    Ok(groove::db::DirectRecordStoreWrite::Set {
        key,
        value: vec![Value::Bytes(fact_bytes)],
    })
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct StoredAuthorityResultKey {
    binding_view: BindingViewKey,
    policy_digest: Option<[u8; 32]>,
}

fn authority_result_key_from_store_prefix(
    key: &[Value],
    context: &'static str,
) -> Result<StoredAuthorityResultKey, Error> {
    if key.len() != 5 {
        return Err(Error::InvalidStoredValue(context));
    }
    let (Value::Uuid(shape_id), Value::Uuid(binding_id), Value::Uuid(read_view)) =
        (&key[0], &key[1], &key[2])
    else {
        return Err(Error::InvalidStoredValue(context));
    };
    let binding_view = BindingViewKey::new(
        ShapeId(*shape_id),
        BindingId(*binding_id),
        ReadViewKey { id: *read_view },
    );
    let Value::U8(scope) = key[3] else {
        return Err(Error::InvalidStoredValue(context));
    };
    let Value::Bytes(digest) = &key[4] else {
        return Err(Error::InvalidStoredValue(context));
    };
    let digest: [u8; 32] = digest
        .as_slice()
        .try_into()
        .map_err(|_| Error::InvalidStoredValue(context))?;
    match scope {
        0 if digest == [0; 32] => Ok(StoredAuthorityResultKey {
            binding_view,
            policy_digest: None,
        }),
        1 => Ok(StoredAuthorityResultKey {
            binding_view,
            policy_digest: Some(digest),
        }),
        _ => Err(Error::InvalidStoredValue(context)),
    }
}

fn resolve_stored_authority_result_key(
    stored: StoredAuthorityResultKey,
    policies: &BTreeMap<[u8; 32], crate::protocol::PolicyBindingKey>,
    context: &'static str,
) -> Result<AuthorityResultKey, Error> {
    match stored.policy_digest {
        None => Ok(AuthorityResultKey::unscoped(stored.binding_view)),
        Some(digest) => policies
            .get(&digest)
            .cloned()
            .map(|policy| AuthorityResultKey::policy_scoped(stored.binding_view, policy))
            .ok_or(Error::InvalidStoredValue(context)),
    }
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
    /// Error returned by Groove-owned chunk storage.
    #[error(transparent)]
    ChunkStorage(#[from] groove::chunks::ChunkStorageError),
    /// An upstream upload referenced a locally owned immutable chunk that is
    /// no longer readable. Retrieval locators are fingerprinted rather than
    /// printed because they authorize exact reads.
    #[error("large-value upstream upload cannot read local chunk ({context}): {source}")]
    LargeValueUploadChunkUnavailable {
        /// Redacted source role, transaction, and immutable-object identities.
        context: String,
        /// The local chunk-store failure.
        #[source]
        source: groove::chunks::ChunkStorageError,
    },
    /// Groove rejected a malformed logical value or indirect descriptor.
    #[error(transparent)]
    LargeValue(#[from] groove::large_values::Error),
    /// Groove could not authenticate/export a locally referenced tree.
    #[error(transparent)]
    LargeValueReachability(#[from] groove::large_values::ReachabilityError),
    /// Jazz staging policy rejected an otherwise valid Groove preparation.
    #[error("large-value upload rate limit exceeded")]
    LargeValueIngressRateLimited,
    /// Required staging state was removed by TTL maintenance before use.
    #[error("large-value staging root expired; upload again")]
    LargeValueStageExpired,
    /// Error returned by groove records.
    #[error(transparent)]
    Record(#[from] records::Error),
    /// A persisted current row could not be decoded at the point of use.
    #[error(transparent)]
    MalformedCurrentRow(#[from] Box<MalformedCurrentRow>),
    /// Error returned by storage.
    #[error(transparent)]
    Storage(#[from] storage::Error),
    /// The internal packed HLC exhausted its final physical/logical position.
    #[error(transparent)]
    ClockOverflow(#[from] crate::time::HlcOverflow),
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
    /// Exact branch selector is missing, malformed, or inconsistent with row cells.
    #[error("invalid branch key: {0}")]
    InvalidBranchKey(String),
    /// An exclusive transaction no longer matches its fixed local snapshot.
    #[error("row visible parent changed since transaction write was staged")]
    TransactionConflict,
    /// Stored value failed validation.
    #[error("invalid stored value: {0}")]
    InvalidStoredValue(&'static str),
    /// A live authority source-closure delta could not transition from the
    /// receiver's installed predecessor.  The usage handle is safe to expose
    /// to the subscription owner; row bodies and claims are deliberately not.
    #[error("invalid authority source closure for subscription {subscription:?}: {transition}")]
    InvalidAuthoritySourceClosure {
        /// Usage-site subscription whose ordered live transition was rejected.
        subscription: SubscriptionKey,
        /// Safe transition diagnosis, never containing row bodies or claims.
        transition: String,
    },
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
    MissingOpenBatch(OpenTransactionId),
    /// A caller attempted to reuse an identity that still names live mutable work.
    #[error("duplicate open batch id: {0}")]
    DuplicateOpenBatch(OpenTransactionId),
    /// A caller tried to use an open transaction as a different identity.
    #[error("open transaction identity does not match its bound identity")]
    OpenTransactionIdentityMismatch,
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

#[cfg(test)]
#[test]
fn authority_result_store_prefix_round_trips_complete_policy_identity() {
    let binding_view = BindingViewKey::new(
        ShapeId(uuid::Uuid::from_u128(1)),
        BindingId(uuid::Uuid::from_u128(2)),
        ReadViewKey {
            id: uuid::Uuid::from_u128(3),
        },
    );
    let mut claims = BTreeMap::new();
    claims.insert("role".to_owned(), Value::String("editor".to_owned()));
    claims.insert("team".to_owned(), Value::U64(7));
    let key = AuthorityResultKey::policy_scoped(
        binding_view,
        crate::protocol::PolicyBindingKey::from_canonical_parts(
            AuthorSubject::authenticated("https://issuer.example", "alice").unwrap(),
            claims,
        ),
    );

    let prefix = authority_result_store_prefix(&key);
    assert_eq!(
        prefix.len(),
        5,
        "authority result identity is a fixed 5-field key"
    );
    assert_eq!(prefix[3], Value::U8(1));
    assert!(matches!(&prefix[4], Value::Bytes(digest) if digest.len() == 32));
    let digest = key.policy_binding.as_ref().unwrap().directory_digest();
    assert_eq!(
        resolve_stored_authority_result_key(
            authority_result_key_from_store_prefix(&prefix, "test").unwrap(),
            &BTreeMap::from([(digest, key.policy_binding.clone().unwrap())]),
            "test",
        )
        .unwrap(),
        key
    );
}

#[cfg(test)]
mod authority_source_incremental_tests {
    use super::*;

    fn fact(table: &str) -> ProgramFactEntry {
        ProgramFactEntry::ProgramSourceCoverage(crate::protocol::ProgramSourceCoverageEntry {
            source: ProgramSourceId {
                table: table.to_owned().into(),
                path: vec![ProgramSourceRole::Root],
            },
            complete: true,
        })
    }

    #[test]
    fn coalesces_contiguous_source_frames_into_one_net_delta() {
        let a = fact("a");
        let b = fact("b");

        let first = AuthoritySourceIncremental::coalesce(
            None,
            4,
            5,
            BTreeSet::from([a.clone(), b.clone()]),
            BTreeSet::new(),
        )
        .unwrap();
        // The second frame arrives before the receiver drains the first. Its
        // net effect is remove A and retain B, so the receiver can apply one
        // exact 4 → 6 delta without a reset or a generic frame-generation
        // dependency.
        let combined = AuthoritySourceIncremental::coalesce(
            Some(first),
            5,
            6,
            BTreeSet::new(),
            BTreeSet::from([a]),
        )
        .unwrap();
        assert_eq!(combined.predecessor_generation, 4);
        assert_eq!(combined.generation, 6);
        assert_eq!(combined.adds, BTreeSet::from([b]));
        assert!(combined.removes.is_empty());
    }

    #[test]
    fn coalescing_cancels_transient_add_remove_and_rejects_gaps() {
        let a = fact("a");
        let transient = AuthoritySourceIncremental::coalesce(
            None,
            10,
            11,
            BTreeSet::from([a.clone()]),
            BTreeSet::new(),
        )
        .unwrap();
        let cancelled = AuthoritySourceIncremental::coalesce(
            Some(transient),
            11,
            12,
            BTreeSet::new(),
            BTreeSet::from([a]),
        )
        .unwrap();
        assert!(cancelled.adds.is_empty());
        assert!(cancelled.removes.is_empty());
        assert_eq!(cancelled.predecessor_generation, 10);
        assert_eq!(cancelled.generation, 12);

        let gap = AuthoritySourceIncremental::coalesce(
            Some(cancelled),
            13,
            14,
            BTreeSet::new(),
            BTreeSet::new(),
        );
        assert!(matches!(gap, Err(Error::InvalidStoredValue(_))));
    }
}
