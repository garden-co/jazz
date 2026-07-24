//! Storage-backed Jazz node core. This module owns the `NodeState` state struct,
//! public node API surface, shared errors, and cross-cutting in-memory indexes;
//! specialized behavior lives in sibling modules such as [`policy`] for policy
//! evaluation, [`global_state`] for read-only settled-global derivations,
//! [`ingest`] for commit/fate ingestion, [`query_eval`] for query execution, and
//! [`views`] for sync view payloads. In the layer map it is the core between the
//! `Db` facade and groove storage/IVM.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use groove::db::{
    CommitMetrics, Database, DatabaseBatch, DirectRecordStoreWrite, Error as GrooveDbError,
    GraphBuilder, PredicateExpr, PrimaryKeyValue, Subscription,
};
use groove::ivm::PreparedShapeId;
use groove::ivm::ProjectField;
use groove::queries::{Query, Select, SelectItem, TableRef};
use groove::records::{self, BorrowedRecord, OwnedRecord, Value};
use groove::storage::{
    self, OrderedKvStorage, ReopenableStorage, StorageLayout, WindowConsolidation,
};
use thiserror::Error;

use self::query_engine::user_column_field;
use crate::ids::{
    AuthorId, BranchId, MigrationLensId, NodeAlias, NodeUuid, RowUuid, SchemaVersionAlias,
    SchemaVersionId,
};
use crate::merge_strategy::{CanonicalizeInput, MergeStrategy as TextMergeStrategy};
use crate::protocol::{
    BindingViewKey, CurrentWriteSchema, LensOp, MigrationLens, ProgramFactEntry, ReadViewKey,
    ResultMemberEntry, ResultRowEntry, RowVersionRef, SchemaVersion, ShapeAst, Subscribe,
    SubscriptionKey, SyncMessage, VersionBundle, VersionCarrier, VersionRecord, ViewFactEntry,
    expand_version_carriers,
};
use crate::protocol_limits::MAX_CONTENT_EXTENT_BYTES;
use crate::query::{Binding, BindingId, QueryError, ShapeId, ValidatedQuery};
use crate::schema::{
    CLEAN_CLOSE_MARKERS_STORE, ColumnSchema, JazzSchema, KNOWN_STATE_FACTS_STORE, LargeValueKind,
    MergeStrategy, SETTLED_PROGRAM_FACTS_STORE, SETTLED_RESULT_MEMBERS_STORE,
    STORAGE_CONSISTENCY_MARKERS_STORE, TableSchema, registered_column_transform,
};
use crate::text_merge::{Run as PlainTextRun, TextOp as PlainTextOp};
use crate::time::{GlobalSeq, TxTime};
use crate::tx::{
    AbsentRead, DeletionEvent, DurabilityTier, Fate, HistoryEntry, PredicateRead,
    RecordedMergeStrategy, RejectedTransaction, RejectedVersion, RejectionReason, RowRead,
    Snapshot, Transaction, TransactionRecord, TxId, TxKind,
};

const TEXT_EXTENT_OPS_MAGIC: &[u8] = b"JTXTREF1";
const LARGE_VALUE_HANDLE_MAGIC: &[u8] = b"JLVH1";
const CLEAN_CLOSE_MARKER_NAME: &str = "node-clean-close";
const CLEAN_CLOSE_MARKER_VERSION: u64 = 1;
const STORAGE_CONSISTENCY_MARKER_NAME: &str = "settled-ahead-current-clean-through";
const STORAGE_CONSISTENCY_MARKER_VERSION: u64 = 1;

mod branches;
mod codec;
pub mod content_store;
mod currency;
mod eviction;
mod global_state;
mod ingest;
pub(crate) mod maintained_subscription_view;
mod open_tx;
mod policy;
mod query_engine;
mod query_eval;
mod recovery;
mod source_resolution;
pub mod text_oplog;
mod views;
#[cfg(feature = "testing")]
pub(crate) use query_eval::LocalMaintainedViewSubscriptionFootprint;
pub(crate) use query_eval::{
    LocalMaintainedViewSubscription, LocalMaintainedViewSubscriptionUpdate,
};
pub(crate) use views::MaintainedViewBundleInputs;

type ResultRowMembershipKey = (groove::Intern<String>, RowUuid);

use branches::BranchRecord;
use codec::*;
use content_store::ContentStore;
use open_tx::*;
use text_oplog::{Content as TextContent, Op as TextOp};

pub use eviction::{EdgeCacheBudget, EdgeCacheBudgetReport, EdgeCacheClass, EvictColdReport};

#[cfg(test)]
mod tests;

/// Default client-clock skew tolerance in milliseconds.
pub const SKEW_TOLERANCE_MS: u64 = 30_000;
/// Default local checkpoint cadence for large text/blob materialization.
///
/// Checkpoints are derived content-store state, never wire state. Every 1024
/// replayed large-value ops, the node stores a materialized snapshot so later
/// reads fold from the nearest checkpoint plus the suffix instead of the full
/// history chain.
pub(crate) const LARGE_VALUE_CHECKPOINT_OP_INTERVAL: usize = 1024;
const LARGE_VALUE_MATERIALIZATION_CACHE_MAX_ENTRIES: usize = 128;
const TX_VERSION_TABLE_CACHE_MAX_ENTRIES: usize = 4096;
type LargeValueCacheKey = (String, RowUuid, String, TxId);

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
    /// Schema catalogue, migration lenses, and schema-partition state.
    catalogue: SchemaCatalogue,
    /// In-memory branch records and branch-specific storage partitions.
    branches: Branches,
    /// Local logical time and global-application progress counters.
    clock: Clock,
    /// Commit-unit and shape-registration payloads waiting for missing context.
    parking: Parking,
    /// Query registration, binding, cache, graph, and settled-result state.
    query: QueryServing,
    /// Locally opened exclusive transactions and authoring attribution state.
    open_tx: OpenTxState,
    /// Rejected transaction records and pending-cascade parent/child indexes.
    rejections: RejectionTracking,
    /// Groove database slot over this node's storage.
    database: DatabaseSlot<S>,
    /// Process-local identity for runtime-local Groove handles such as prepared shape ids.
    groove_runtime_token: u64,
    /// Whether this node has complete settled history for historical reads.
    history_complete: bool,
    /// Mapping from stable node UUIDs to compact on-disk aliases.
    pub(crate) node_aliases: BTreeMap<NodeUuid, NodeAlias>,
    /// Ahead-current overlay keys for rows whose non-global versions can affect local reads.
    ahead_current_keys: BTreeSet<(String, VersionLayer, RowUuid, TxTime, NodeAlias)>,
    /// Rows touched by the ahead-current overlay.
    ahead_current_rows: BTreeSet<(String, RowUuid)>,
    /// Latest ahead-current key per table/layer/row for local overlay reads.
    ahead_current_latest: BTreeMap<(String, VersionLayer, RowUuid), (TxTime, NodeAlias)>,
    /// Minimum number of large-value ops replayed before storing a checkpoint.
    large_value_checkpoint_op_interval: usize,
    /// Runtime counters for large-value materialization and checkpoint behavior.
    large_value_metrics: LargeValueMetrics,
    /// Immutable materialized large-value bytes keyed by exact version.
    large_value_materialization_cache: BTreeMap<LargeValueCacheKey, Vec<u8>>,
    /// Runtime registry for rung-3 text merge strategies.
    text_merge_strategies: BTreeMap<(String, u32), Arc<dyn TextMergeStrategy>>,
    /// Runtime counters for sync parking, draining, and ingestion behavior.
    sync_metrics: SyncMetrics,
    /// Runtime counters for query-engine read authorization paths.
    query_engine_read_metrics: QueryEngineReadMetrics,
    /// Process-local claims attached to authenticated subscriber sessions.
    session_claims: BTreeMap<AuthorId, BTreeMap<String, Value>>,
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
    /// Shortest migration-lens paths by schema pair and traversal direction.
    lens_path_cache: BTreeMap<LensPathCacheKey, Option<Vec<MigrationLensId>>>,
    /// Table-specific, already-validated lens programs used by hot read/write paths.
    compiled_lens_cache: BTreeMap<CompiledLensCacheKey, Option<CompiledLensPath>>,
    /// Schema version currently used for newly authored writes.
    current_write_schema: CurrentWriteSchema,
    /// Storage partitions materialized for table/schema-version pairs.
    partitions: BTreeSet<(String, SchemaVersionId)>,
}

/// Branch metadata and branch-partition layout known by the node.
#[derive(Clone, Debug, Default)]
struct Branches {
    /// In-memory branch records indexed by branch ID.
    branches: BTreeMap<BranchId, BranchRecord>,
    /// Storage partitions materialized for table/schema-version/branch triples.
    branch_partitions: BTreeSet<(String, SchemaVersionId, BranchId)>,
}

/// Local transaction clock and settled-global application progress.
#[derive(Clone, Debug)]
struct Clock {
    /// Highest local transaction timestamp observed or minted by this node.
    tx_time: TxTime,
    /// Next global sequence number to allocate when accepting local work globally.
    next_global_seq: GlobalSeq,
    /// Contiguous global sequence watermark already applied to local storage.
    applied_global_watermark: GlobalSeq,
    /// Applied global sequence numbers above the contiguous watermark.
    applied_global_above_watermark: BTreeSet<GlobalSeq>,
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
    /// Prepared current-row graph per table and durability tier.
    current_row_graphs: BTreeMap<(String, DurabilityTier), GraphBuilder>,
    /// Prepared query plans keyed by shape, durability tier, and parameter
    /// descriptor signature.
    query_shape_cache:
        BTreeMap<(crate::query::ShapeId, DurabilityTier, String), PreparedQueryPlanHandle>,
    /// Derived read-policy authorization requests keyed by policy context.
    read_policy_authorization_request_cache:
        BTreeMap<ReadPolicyAuthorizationRequestCacheKey, query_engine::QueryProgramRequest>,
    /// Lowered authorization row-id graphs keyed by their full query-engine request.
    policy_authorization_graph_cache: BTreeMap<String, query_eval::PolicyAuthorizationGraph>,
    /// Logical tables that have history rows for a stored transaction.
    tx_version_tables_cache: BTreeMap<TxId, BTreeSet<String>>,
    /// Recently staged history rows for a stored transaction.
    tx_versions_cache: BTreeMap<TxId, Vec<VersionRow>>,
    /// Approximate insertion order for bounding `tx_version_tables_cache`.
    tx_version_tables_cache_order: VecDeque<TxId>,
    /// Live membership for `tx_version_tables_cache_order`.
    tx_version_tables_cache_order_set: BTreeSet<TxId>,
    /// Version storage source descriptors keyed by logical table and layer.
    ///
    /// These descriptors are static catalogue metadata. They are invalidated
    /// whenever schema partitions or catalogue schemas change.
    version_storage_sources_cache:
        BTreeMap<(String, VersionLayer), Vec<(String, records::RecordDescriptor)>>,
    /// Interned physical table names for hot ingest/current-row paths.
    ///
    /// Keyed by logical table, physical class, and schema-version context. This
    /// is pure memoization: the mapping key fully determines the name.
    physical_table_name_cache: BTreeMap<PhysicalTableNameKey, groove::Intern<String>>,
    /// Registered validated query shapes keyed by stable shape ID.
    registered_shapes: BTreeMap<ShapeId, ValidatedQuery>,
    /// Registered query binding values keyed by shape and usage-site binding ID.
    registered_bindings: BTreeMap<ShapeId, BTreeMap<BindingId, RegisteredBinding>>,
    /// Subscriber-side settled result-member/completeness state by canonical query binding/view.
    settled_result_sets: BTreeMap<BindingViewKey, BTreeSet<ResultMemberEntry>>,
    /// Point index for ordinary current-row settled result members.
    ///
    /// This mirrors the row-shaped subset of `settled_result_sets` so applying a
    /// new current winner can remove the previous winner without scanning the
    /// full result set.
    settled_result_row_index:
        BTreeMap<BindingViewKey, BTreeMap<ResultRowMembershipKey, ResultMemberEntry>>,
    /// Subscriber-side settled non-row facts by canonical query binding/view.
    settled_program_facts: BTreeMap<BindingViewKey, BTreeSet<ViewFactEntry>>,
    /// Server-stamped settled-through cursor for each canonical binding view.
    settled_through_by_binding_view: BTreeMap<BindingViewKey, GlobalSeq>,
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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum PhysicalTableClass {
    VersionStorage(VersionLayer),
    GlobalCurrent(VersionLayer),
    AheadCurrent(VersionLayer),
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct PhysicalTableNameKey {
    table: String,
    class: PhysicalTableClass,
    schema_version: SchemaVersionId,
    base_schema_version: SchemaVersionId,
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
    include_deleted_root: bool,
}

/// One usage-site query binding registration.
#[derive(Clone, Debug, PartialEq)]
struct RegisteredBinding {
    values: Vec<Value>,
    read_view: ReadViewKey,
    binding_view_key: BindingViewKey,
}

/// Locally open exclusive transactions and local-only permission attribution.
struct OpenTxState {
    /// Open exclusive transaction handles keyed by local handle ID.
    open_exclusive: BTreeMap<OpenTxId, OpenExclusive>,
    /// Next local exclusive transaction handle ID to allocate.
    next_open_tx_id: u64,
    /// Local-only permission subjects for transactions whose `made_by` keeps provenance.
    local_permission_subjects: BTreeMap<TxId, AuthorId>,
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
    pub identity: AuthorId,
    /// Whether the connection may attribute writes to a different `made_by`.
    pub trust: CommitUnitTrust,
}

/// Trust mode for an inbound commit-unit upload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitUnitTrust {
    /// Session/client links must honestly set `made_by` to the link identity.
    Session,
    /// Trusted backends may preserve user provenance in `made_by`.
    TrustedBackend,
}

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// Open or create a node over the supplied storage.
    pub fn new(node_uuid: NodeUuid, schema: JazzSchema, storage: S) -> Result<Self, Error>
    where
        S: ReopenableStorage,
    {
        Self::new_with_history_complete(node_uuid, schema, storage, false)
    }

    /// Open or create a node that is known to hold complete settled history.
    ///
    /// This is the authority/local-complete constructor for historical reads.
    /// Ordinary downstream clients should use [`NodeState::new`], which fails
    /// historical handle reads closed until a complete-history subscription
    /// path marks the queried shape complete in a later slice.
    pub fn new_history_complete(
        node_uuid: NodeUuid,
        schema: JazzSchema,
        storage: S,
    ) -> Result<Self, Error>
    where
        S: ReopenableStorage,
    {
        Self::new_with_history_complete(node_uuid, schema, storage, true)
    }

    /// Register a deterministic rung-3 text merge strategy for this process.
    pub fn register_text_merge_strategy(&mut self, strategy: Arc<dyn TextMergeStrategy>) {
        self.text_merge_strategies
            .insert((strategy.id().to_owned(), strategy.version()), strategy);
    }

    /// Open or create a node with a specific local checkpoint density.
    pub fn new_with_large_value_checkpoint_op_interval(
        node_uuid: NodeUuid,
        schema: JazzSchema,
        storage: S,
        history_complete: bool,
        large_value_checkpoint_op_interval: usize,
    ) -> Result<Self, Error>
    where
        S: ReopenableStorage,
    {
        Self::new_with_options(
            node_uuid,
            schema,
            storage,
            history_complete,
            large_value_checkpoint_op_interval,
        )
    }

    /// Rebuild the groove layer over the same storage using the standard open path.
    pub fn reopen_in_place(self) -> Result<Self, Error>
    where
        S: ReopenableStorage,
    {
        let NodeState {
            node_uuid,
            catalogue,
            database,
            history_complete,
            text_merge_strategies,
            ..
        } = self;
        let storage = database.into_inner().into_storage();
        let mut reopened = Self::new_with_history_complete(
            node_uuid,
            catalogue.schema,
            storage,
            history_complete,
        )?;
        reopened.text_merge_strategies = text_merge_strategies;
        Ok(reopened)
    }

    fn new_with_history_complete(
        node_uuid: NodeUuid,
        schema: JazzSchema,
        storage: S,
        history_complete: bool,
    ) -> Result<Self, Error> {
        Self::new_with_options(
            node_uuid,
            schema,
            storage,
            history_complete,
            LARGE_VALUE_CHECKPOINT_OP_INTERVAL,
        )
    }

    fn new_with_options(
        node_uuid: NodeUuid,
        schema: JazzSchema,
        storage: S,
        history_complete: bool,
        large_value_checkpoint_op_interval: usize,
    ) -> Result<Self, Error> {
        let current_schema_version_id = schema.version_id();
        let CatalogueOpenState {
            storage,
            mut schemas,
            lenses,
            current_write_schema,
            partitions,
            branch_partitions,
        } = Self::open_catalogue_stage(schema.clone(), storage)?;
        let had_base_schema = schemas.contains_key(&current_schema_version_id);
        if !had_base_schema {
            schemas.insert(
                current_schema_version_id,
                SchemaVersion::new(schema.clone()),
            );
        }
        let database =
            Self::open_full_database(&schema, &schemas, &partitions, &branch_partitions, storage)?;
        let current_row_graphs = current_row_graphs(&schema);
        let mut node = Self {
            node_uuid,
            self_node_alias: None,
            catalogue: SchemaCatalogue {
                current_schema_version_id,
                current_schema_version_alias: None,
                schema: schema.clone(),
                schema_version_aliases: BTreeMap::new(),
                catalogue_schemas: schemas,
                catalogue_lenses: lenses,
                lens_path_cache: BTreeMap::new(),
                compiled_lens_cache: BTreeMap::new(),
                current_write_schema,
                partitions,
            },
            branches: Branches {
                branches: BTreeMap::new(),
                branch_partitions,
            },
            clock: Clock {
                tx_time: TxTime::default(),
                next_global_seq: GlobalSeq(1),
                applied_global_watermark: GlobalSeq(0),
                applied_global_above_watermark: BTreeSet::new(),
            },
            parking: Parking::default(),
            query: QueryServing {
                current_row_graphs,
                query_shape_cache: BTreeMap::new(),
                read_policy_authorization_request_cache: BTreeMap::new(),
                policy_authorization_graph_cache: BTreeMap::new(),
                tx_version_tables_cache: BTreeMap::new(),
                tx_versions_cache: BTreeMap::new(),
                tx_version_tables_cache_order: VecDeque::new(),
                tx_version_tables_cache_order_set: BTreeSet::new(),
                version_storage_sources_cache: BTreeMap::new(),
                physical_table_name_cache: BTreeMap::new(),
                registered_shapes: BTreeMap::new(),
                registered_bindings: BTreeMap::new(),
                settled_result_sets: BTreeMap::new(),
                settled_result_row_index: BTreeMap::new(),
                settled_program_facts: BTreeMap::new(),
                settled_through_by_binding_view: BTreeMap::new(),
                known_state_declared_binding_views: BTreeSet::new(),
                initial_hydration_binding_views: BTreeSet::new(),
                deferred_publication_binding_views: BTreeSet::new(),
                pending_authoritative_reset_binding_views: BTreeSet::new(),
            },
            open_tx: OpenTxState {
                open_exclusive: BTreeMap::new(),
                next_open_tx_id: 1,
                local_permission_subjects: BTreeMap::new(),
            },
            rejections: RejectionTracking::default(),
            database: DatabaseSlot::new(database),
            groove_runtime_token: next_groove_runtime_token(),
            history_complete,
            node_aliases: BTreeMap::new(),
            ahead_current_keys: BTreeSet::new(),
            ahead_current_rows: BTreeSet::new(),
            ahead_current_latest: BTreeMap::new(),
            large_value_checkpoint_op_interval: large_value_checkpoint_op_interval.max(1),
            large_value_metrics: LargeValueMetrics::default(),
            large_value_materialization_cache: BTreeMap::new(),
            text_merge_strategies: BTreeMap::new(),
            sync_metrics: SyncMetrics::default(),
            query_engine_read_metrics: QueryEngineReadMetrics::default(),
            session_claims: BTreeMap::new(),
        };
        node.recover_from_storage()?;
        node.recover_known_state_facts()?;
        node.rebuild_ahead_current_keys()?;
        let self_node_alias = node.ensure_node_alias(node_uuid)?;
        node.self_node_alias = Some(self_node_alias);
        let schema_alias = node.ensure_schema_version_alias(current_schema_version_id)?;
        node.catalogue.current_schema_version_alias = Some(schema_alias);
        if !had_base_schema {
            node.persist_catalogue_schema(&SchemaVersion::new(schema.clone()))?;
        }
        for table in schema.tables.iter().map(|table| table.name.clone()) {
            node.persist_partition(table, current_schema_version_id)?;
        }
        Ok(node)
    }

    fn open_full_database(
        schema: &JazzSchema,
        catalogue_schemas: &BTreeMap<SchemaVersionId, SchemaVersion>,
        partitions: &BTreeSet<(String, SchemaVersionId)>,
        branch_partitions: &BTreeSet<(String, SchemaVersionId, BranchId)>,
        storage: S,
    ) -> Result<Database<S>, Error> {
        debug_assert_lowered_layouts(schema);
        let lowered = schema.lower_to_groove_with_partitions(
            catalogue_schemas,
            partitions,
            branch_partitions,
        );
        let logical_cfs = lowered
            .column_families()
            .into_iter()
            .chain(std::iter::once("indices"))
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let layout = StorageLayout::jazz_class_v1_for(logical_cfs.iter().map(String::as_str));
        Database::new_with_storage_layout(
            schema.lower_to_groove_with_partitions(
                catalogue_schemas,
                partitions,
                branch_partitions,
            ),
            storage,
            layout,
        )
        .map_err(Error::from)
    }

    /// Return the pure-storage large-value content store.
    pub fn content_store(&self) -> ContentStore<'_, S> {
        ContentStore::new(&self.database)
    }

    pub(crate) fn applied_global_watermark(&self) -> GlobalSeq {
        self.clock.applied_global_watermark
    }

    /// Attach process-local auth claims to an accepted subscriber identity.
    pub(crate) fn set_session_claims(
        &mut self,
        identity: AuthorId,
        claims: BTreeMap<String, Value>,
    ) -> bool {
        if self.session_claims.get(&identity) == Some(&claims) {
            return false;
        }
        self.session_claims.insert(identity, claims);
        self.query.read_policy_authorization_request_cache.clear();
        self.query.policy_authorization_graph_cache.clear();
        true
    }

    fn rebuild_database_slot(&mut self) -> Result<(), Error> {
        let old_database = self.database.take();
        let storage = old_database.into_storage();
        let database = Self::open_full_database(
            &self.catalogue.schema,
            &self.catalogue.catalogue_schemas,
            &self.catalogue.partitions,
            &self.branches.branch_partitions,
            storage,
        )?;
        self.database.replace(database);
        self.groove_runtime_token = next_groove_runtime_token();
        self.rederive_restart_state()?;
        Ok(())
    }

    fn rederive_restart_state(&mut self) -> Result<(), Error> {
        self.self_node_alias = None;
        self.catalogue.current_schema_version_alias = None;
        self.clock.tx_time = TxTime::default();
        self.clock.next_global_seq = GlobalSeq(1);
        self.clock.applied_global_watermark = GlobalSeq(0);
        self.clock.applied_global_above_watermark.clear();
        self.node_aliases.clear();
        self.catalogue.schema_version_aliases.clear();
        self.rejections.child_txs_by_parent.clear();
        self.rejections.rejected_transactions.clear();
        self.branches.branches.clear();
        self.query.current_row_graphs = current_row_graphs(&self.catalogue.schema);
        self.query.query_shape_cache.clear();
        self.query.read_policy_authorization_request_cache.clear();
        self.query.policy_authorization_graph_cache.clear();
        self.query.tx_version_tables_cache.clear();
        self.query.tx_versions_cache.clear();
        self.query.tx_version_tables_cache_order.clear();
        self.query.tx_version_tables_cache_order_set.clear();
        self.query.version_storage_sources_cache.clear();
        self.query.settled_result_sets.clear();
        self.query.settled_result_row_index.clear();
        self.query.settled_program_facts.clear();
        self.query.settled_through_by_binding_view.clear();
        self.query.known_state_declared_binding_views.clear();
        self.query.initial_hydration_binding_views.clear();
        self.query.deferred_publication_binding_views.clear();
        self.query.pending_authoritative_reset_binding_views.clear();
        self.parking.parked_shape_registrations.clear();
        self.parking.parked_binding_deltas.clear();
        self.recover_from_storage()?;
        self.recover_known_state_facts()?;
        let self_node_alias = self.ensure_node_alias(self.node_uuid)?;
        self.self_node_alias = Some(self_node_alias);
        let schema_alias =
            self.ensure_schema_version_alias(self.catalogue.current_schema_version_id)?;
        self.catalogue.current_schema_version_alias = Some(schema_alias);
        Ok(())
    }

    fn result_member_row_key(member: &ResultMemberEntry) -> Option<ResultRowMembershipKey> {
        member
            .as_row()
            .map(|(table, row_uuid, _)| (table, row_uuid))
    }

    fn insert_settled_result_member_indexed(
        &mut self,
        binding_view_key: BindingViewKey,
        member: ResultMemberEntry,
    ) {
        if let Some(row_key) = Self::result_member_row_key(&member) {
            self.query
                .settled_result_row_index
                .entry(binding_view_key)
                .or_default()
                .insert(row_key, member.clone());
        }
        self.query
            .settled_result_sets
            .entry(binding_view_key)
            .or_default()
            .insert(member);
    }

    fn remove_settled_result_member_indexed(
        &mut self,
        binding_view_key: BindingViewKey,
        member: &ResultMemberEntry,
    ) -> bool {
        let removed = self
            .query
            .settled_result_sets
            .get_mut(&binding_view_key)
            .is_some_and(|members| members.remove(member));
        if removed
            && let Some(row_key) = Self::result_member_row_key(member)
            && self
                .query
                .settled_result_row_index
                .get(&binding_view_key)
                .and_then(|index| index.get(&row_key))
                == Some(member)
            && let Some(index) = self
                .query
                .settled_result_row_index
                .get_mut(&binding_view_key)
        {
            index.remove(&row_key);
        }
        removed
    }

    fn remove_settled_result_member_for_row_indexed(
        &mut self,
        binding_view_key: BindingViewKey,
        table: groove::Intern<String>,
        row_uuid: RowUuid,
    ) -> Option<ResultMemberEntry> {
        let row_key = (table, row_uuid);
        let previous = self
            .query
            .settled_result_row_index
            .get_mut(&binding_view_key)
            .and_then(|index| index.remove(&row_key))?;
        if let Some(members) = self.query.settled_result_sets.get_mut(&binding_view_key) {
            members.remove(&previous);
        }
        Some(previous)
    }

    fn clear_settled_result_view(&mut self, binding_view_key: BindingViewKey) {
        self.query.settled_result_sets.remove(&binding_view_key);
        self.query
            .settled_result_row_index
            .remove(&binding_view_key);
    }

    fn open_catalogue_stage(
        schema: JazzSchema,
        storage: S,
    ) -> Result<CatalogueOpenState<S>, Error> {
        let current_schema_version_id = schema.version_id();
        let meta_schema = schema.lower_catalogue_meta_to_groove();
        let logical_cfs = meta_schema
            .column_families()
            .into_iter()
            .chain(std::iter::once("indices"))
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let meta_database = Database::new_with_storage_layout(
            meta_schema,
            storage,
            StorageLayout::jazz_class_v1_for(logical_cfs.iter().map(String::as_str)),
        )?;
        let mut catalogue_schemas = BTreeMap::new();
        let mut catalogue_lenses = BTreeMap::new();
        for raw in meta_database.primary_key_scan_raw("jazz_catalogue", &[])? {
            let record = raw.record();
            match record.get_bytes(CatalogueRowRecord::FIELD_KIND_IDX)? {
                b"schema" => {
                    let schema_version: SchemaVersion = serde_json::from_slice(
                        record.get_bytes(CatalogueRowRecord::FIELD_PAYLOAD_IDX)?,
                    )?;
                    if schema_version.id
                        != SchemaVersionId(record.get_uuid(CatalogueRowRecord::FIELD_ID_IDX)?)
                    {
                        return Err(Error::InvalidStoredValue("catalogue schema id mismatch"));
                    }
                    catalogue_schemas.insert(schema_version.id, schema_version);
                }
                b"lens" => {
                    let lens: MigrationLens = serde_json::from_slice(
                        record.get_bytes(CatalogueRowRecord::FIELD_PAYLOAD_IDX)?,
                    )?;
                    if lens.id
                        != MigrationLensId(record.get_uuid(CatalogueRowRecord::FIELD_ID_IDX)?)
                    {
                        return Err(Error::InvalidStoredValue("catalogue lens id mismatch"));
                    }
                    catalogue_lenses.insert(lens.id, lens);
                }
                _ => return Err(Error::InvalidStoredValue("unknown catalogue kind")),
            }
        }
        let mut current_write_schema = CurrentWriteSchema {
            revision: 0,
            schema: current_schema_version_id,
        };
        if let Some(raw) = meta_database.primary_key_last_raw("jazz_catalogue_pointer", &[])? {
            let record = raw.record();
            current_write_schema = CurrentWriteSchema {
                revision: record.get_u64(CataloguePointerRowRecord::FIELD_REVISION_IDX)?,
                schema: SchemaVersionId(
                    record.get_uuid(CataloguePointerRowRecord::FIELD_SCHEMA_IDX)?,
                ),
            };
        }
        let mut partitions = BTreeSet::new();
        for raw in meta_database.primary_key_scan_raw("jazz_partitions", &[])? {
            let record = raw.record();
            let table = String::from_utf8(
                record
                    .get_bytes(PartitionRowRecord::FIELD_TABLE_NAME_IDX)?
                    .to_vec(),
            )
            .map_err(|_| Error::InvalidStoredValue("partition table name must be utf8"))?;
            let schema_version =
                SchemaVersionId(record.get_uuid(PartitionRowRecord::FIELD_SCHEMA_VERSION_IDX)?);
            partitions.insert((table, schema_version));
        }
        let mut branch_partitions = BTreeSet::new();
        for raw in meta_database.primary_key_scan_raw("jazz_branch_partitions", &[])? {
            let record = raw.record();
            let table = String::from_utf8(
                record
                    .get_bytes(BranchPartitionRowRecord::FIELD_TABLE_NAME_IDX)?
                    .to_vec(),
            )
            .map_err(|_| Error::InvalidStoredValue("branch partition table name must be utf8"))?;
            let schema_version = SchemaVersionId(
                record.get_uuid(BranchPartitionRowRecord::FIELD_SCHEMA_VERSION_IDX)?,
            );
            let branch_id =
                BranchId(record.get_uuid(BranchPartitionRowRecord::FIELD_BRANCH_ID_IDX)?);
            branch_partitions.insert((table, schema_version, branch_id));
        }
        Ok(CatalogueOpenState {
            storage: meta_database.into_storage(),
            schemas: catalogue_schemas,
            lenses: catalogue_lenses,
            current_write_schema,
            partitions,
            branch_partitions,
        })
    }

    /// Commit a local mergeable write and leave its fate pending.
    pub fn commit_mergeable(&mut self, commit: MergeableCommit) -> Result<TxId, Error> {
        commit.validate()?;
        self.merge_commit_parent_times(std::slice::from_ref(&commit))?;
        let made_at = self.mint_tx_time(commit.now_ms);
        self.commit_mergeable_at(commit, made_at)
    }

    /// Commit multiple local mergeable writes as one transaction.
    pub fn commit_mergeable_many(&mut self, commits: Vec<MergeableCommit>) -> Result<TxId, Error> {
        if commits.is_empty() {
            return Err(Error::InvalidMergeableCommit(
                "mergeable transaction requires at least one write",
            ));
        }
        for commit in &commits {
            commit.validate()?;
            if commit.effective_permission_subject() != commits[0].effective_permission_subject() {
                return Err(Error::InvalidMergeableCommit(
                    "mergeable transaction permission subjects must match",
                ));
            }
        }
        self.merge_commit_parent_times(&commits)?;
        let made_at = self.mint_tx_time(commits[0].now_ms);
        self.commit_mergeable_many_at(commits, made_at)
    }

    /// Commit explicit text/blob edit operations for one large-value column.
    pub fn commit_large_value_edit(&mut self, edit: LargeValueEditCommit) -> Result<TxId, Error> {
        edit.validate()?;
        if let Some(parent) =
            self.current_layer_parent_tx_id(&edit.table, edit.row_uuid, VersionLayer::Content)?
        {
            self.merge_tx_time(parent.time);
        }
        let made_at = self.mint_tx_time(edit.now_ms);
        self.commit_large_value_edit_at(edit, made_at)
    }

    fn merge_commit_parent_times(&mut self, commits: &[MergeableCommit]) -> Result<(), Error> {
        for commit in commits {
            if commit.parents.is_empty() {
                let table_schema = self
                    .table_in_schema(&commit.table, self.catalogue.current_write_schema.schema)?;
                if table_schema
                    .columns
                    .iter()
                    .any(|column| column.large_value.is_some())
                {
                    let layer = VersionLayer::for_commit(commit);
                    if let Some(parent) =
                        self.current_layer_parent_tx_id(&commit.table, commit.row_uuid, layer)?
                    {
                        self.merge_tx_time(parent.time);
                    }
                }
            } else {
                for parent in &commit.parents {
                    self.merge_tx_time(parent.time);
                }
            }
        }
        Ok(())
    }

    fn current_layer_parent_tx_id(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
        layer: VersionLayer,
    ) -> Result<Option<TxId>, Error> {
        let table_schema =
            self.table_in_schema(table, self.catalogue.current_write_schema.schema)?;
        match self.query_local_layer_winner(&table_schema.name, row_uuid, layer)? {
            Some(previous) => self.version_tx_id(&previous).map(Some),
            None => self
                .query_global_layer_winner(&table_schema.name, row_uuid, layer)?
                .map(|previous| self.version_tx_id(&previous))
                .transpose(),
        }
    }

    fn commit_mergeable_at(
        &mut self,
        commit: MergeableCommit,
        made_at: TxTime,
    ) -> Result<TxId, Error> {
        self.commit_mergeable_many_at(vec![commit], made_at)
    }

    fn commit_mergeable_many_at(
        &mut self,
        commits: Vec<MergeableCommit>,
        made_at: TxTime,
    ) -> Result<TxId, Error> {
        let write_schema_version = self.catalogue.current_write_schema.schema;
        let tx_id = TxId::new(made_at, self.node_uuid);
        let made_by = commits[0].made_by;
        let permission_subject = commits[0].effective_permission_subject();
        let user_metadata_json = commits[0].user_metadata_json.clone();
        let tx = Transaction {
            tx_id,
            kind: TxKind::Mergeable,
            n_total_writes: commits.len().try_into().map_err(|_| {
                Error::InvalidMergeableCommit("transaction write count exceeds u32")
            })?,
            made_by,
            permission_subject: commits[0].permission_subject,
            base_snapshot: None,
            row_read_set: None,
            absent_read_set: None,
            predicate_read_set: None,
            user_metadata_json,
            source_branch: None,
            merge_strategy: commits[0].merge_strategy.clone(),
        };
        let tx_node_alias = self.ensure_node_alias(tx_id.node)?;
        let schema_version_alias = self.ensure_schema_version_alias(write_schema_version)?;
        let mut batch = self.database.open_batch();
        batch.insert(
            "jazz_transactions",
            transaction_values(
                tx_node_alias,
                &tx,
                Fate::Pending,
                None,
                DurabilityTier::Local,
            ),
        );
        let mut stored_versions = Vec::new();
        for commit in commits {
            let table_schema = self.table_in_schema(&commit.table, write_schema_version)?;
            let layer = VersionLayer::for_commit(&commit);
            let previous_current =
                match self.query_local_layer_winner(&table_schema.name, commit.row_uuid, layer)? {
                    Some(previous) => Some(previous),
                    None => {
                        self.query_global_layer_winner(&table_schema.name, commit.row_uuid, layer)?
                    }
                };
            let creator_source = if let Some(previous) = previous_current.as_ref() {
                Some(previous.clone())
            } else if layer == VersionLayer::Deletion {
                match self.query_local_layer_winner(
                    &table_schema.name,
                    commit.row_uuid,
                    VersionLayer::Content,
                )? {
                    Some(previous) => Some(previous),
                    None => self.query_global_layer_winner(
                        &table_schema.name,
                        commit.row_uuid,
                        VersionLayer::Content,
                    )?,
                }
            } else {
                None
            };
            let (created_by, created_at) = creator_source
                .as_ref()
                .map(|version| (version.created_by(), version.created_at()))
                .unwrap_or((commit.made_by, TxTime(commit.now_ms)));

            let implicit_parent = if table_schema
                .columns
                .iter()
                .any(|column| column.large_value.is_some())
            {
                previous_current
                    .as_ref()
                    .map(|previous| self.version_tx_id(previous))
                    .transpose()?
            } else {
                None
            };
            let explicit_parent_count = commit.parents.len();
            let parents = if commit.parents.is_empty() {
                implicit_parent.into_iter().collect()
            } else {
                commit.parents
            };
            let cells = if explicit_parent_count > 1 && commit.merge_strategy.is_some() {
                commit.cells
            } else {
                self.encode_large_value_cells(
                    &table_schema,
                    write_schema_version,
                    commit.row_uuid,
                    commit.made_by,
                    commit.cells,
                    previous_current.as_ref(),
                )?
            };
            let stored = VersionRow::from_parts_with_schema_version(
                &table_schema,
                VersionRowParts {
                    table: commit.table,
                    row_uuid: commit.row_uuid,
                    tx_node_alias,
                    schema_version_alias,
                    tx_time: made_at,
                    parents,
                    created_by,
                    created_at,
                    updated_by: commit.made_by,
                    updated_at: TxTime(commit.now_ms),
                    cells,
                    deletion: commit.deletion,
                },
                (write_schema_version != self.catalogue.current_schema_version_id)
                    .then_some(write_schema_version),
            )?;
            let previous_winner = if let Some(previous) = previous_current.as_ref() {
                Some((
                    previous,
                    self.version_tx_id(previous)?,
                    self.version_made_at(previous)?,
                ))
            } else {
                None
            };
            let new_is_current =
                version_wins_over_open_winner(&stored, tx_id, made_at, previous_winner);
            let _ = (new_is_current, previous_current);
            batch.insert_raw(
                version_storage_table_name_for_schema(
                    &table_schema.name,
                    stored.layer(),
                    write_schema_version,
                    self.catalogue.current_schema_version_id,
                ),
                history_primary_key(&stored),
                stored.record.raw().to_vec(),
            );
            self.update_merge_heads_for_content_version(&mut batch, &stored)?;
            self.write_ahead_current_insert(&mut batch, &stored)?;
            for parent in stored.parents() {
                if let Some(parent_alias) = self.node_aliases.get(&parent.node).copied() {
                    batch.insert(
                        "jazz_pending_edges",
                        pending_edge_values(tx_node_alias, tx_id, parent_alias, parent),
                    );
                }
            }
            stored_versions.push(stored);
        }
        self.database.commit_batch(batch)?;
        self.cache_tx_versions(tx_id, stored_versions.clone());
        if permission_subject != made_by {
            self.open_tx
                .local_permission_subjects
                .insert(tx_id, permission_subject);
        }
        for stored in &stored_versions {
            self.record_child_edges(tx_id, stored.parents());
        }
        Ok(tx_id)
    }

    fn commit_large_value_edit_at(
        &mut self,
        edit: LargeValueEditCommit,
        made_at: TxTime,
    ) -> Result<TxId, Error> {
        let write_schema_version = self.catalogue.current_write_schema.schema;
        let table_schema = self.table_in_schema(&edit.table, write_schema_version)?;
        let column = table_schema
            .columns
            .iter()
            .find(|column| column.name == edit.column)
            .ok_or(Error::InvalidMergeableCommit(
                "large-value edit column not found",
            ))?;
        if column.large_value.is_none() {
            return Err(Error::InvalidMergeableCommit(
                "large-value edit column must be text or blob",
            ));
        }
        // Format-declared text columns currently accept whole-value writes only.
        // Canonicalizing op streams would rewrite client-authored ops, so this
        // remains a named staging limitation until an op-preserving design lands.
        if column.text_merge_spec.is_some() {
            return Err(Error::InvalidMergeableCommit(
                "op edits on format-declared columns not supported yet",
            ));
        }

        let tx_id = TxId::new(made_at, self.node_uuid);
        let tx = Transaction {
            tx_id,
            kind: TxKind::Mergeable,
            n_total_writes: 1,
            made_by: edit.made_by,
            permission_subject: None,
            base_snapshot: None,
            row_read_set: None,
            absent_read_set: None,
            predicate_read_set: None,
            user_metadata_json: edit.user_metadata_json.clone(),
            source_branch: None,
            merge_strategy: None,
        };
        let tx_node_alias = self.ensure_node_alias(tx_id.node)?;
        let previous_current = match self.query_local_layer_winner(
            &table_schema.name,
            edit.row_uuid,
            VersionLayer::Content,
        )? {
            Some(previous) => Some(previous),
            None => self.query_global_layer_winner(
                &table_schema.name,
                edit.row_uuid,
                VersionLayer::Content,
            )?,
        };
        let (created_by, created_at) = previous_current
            .as_ref()
            .map(|version| (version.created_by(), version.created_at()))
            .unwrap_or((edit.made_by, TxTime(edit.now_ms)));
        let parent_len = match previous_current.as_ref() {
            Some(parent) => self.large_value_column_len(&table_schema, parent, &edit.column)?,
            None => 0,
        };
        let table = edit.table.clone();
        let row_uuid = edit.row_uuid;
        let made_by = edit.made_by;
        let updated_at = TxTime(edit.now_ms);
        let column_name = edit.column.clone();
        let inline_ops = edit.ops;
        validate_large_value_edit_ranges(parent_len, &inline_ops)?;
        let cell_payload = match column.large_value {
            Some(LargeValueKind::Text) => {
                let text_ops = large_value_edit_ops_to_legacy_text_ops(inline_ops);
                let ops = self.extent_back_text_ops(made_by, row_uuid, &column_name, text_ops)?;
                encode_extent_text_ops(&ops)
            }
            Some(LargeValueKind::Blob) => {
                let text_ops = large_value_edit_ops_to_legacy_text_ops(inline_ops);
                let ops = self.extent_back_text_ops(made_by, row_uuid, &column_name, text_ops)?;
                text_oplog::encode(&ops)
            }
            None => {
                return Err(Error::InvalidMergeableCommit(
                    "large-value edit column must be text or blob",
                ));
            }
        };
        let cells = BTreeMap::from([(column_name, Value::Bytes(cell_payload))]);
        let parents = previous_current
            .as_ref()
            .map(|previous| self.version_tx_id(previous))
            .transpose()?
            .into_iter()
            .collect();
        let schema_version_alias = self.ensure_schema_version_alias(write_schema_version)?;
        let stored = VersionRow::from_parts_with_schema_version(
            &table_schema,
            VersionRowParts {
                table,
                row_uuid,
                tx_node_alias,
                schema_version_alias,
                tx_time: made_at,
                parents,
                created_by,
                created_at,
                updated_by: made_by,
                updated_at,
                cells,
                deletion: None,
            },
            (write_schema_version != self.catalogue.current_schema_version_id)
                .then_some(write_schema_version),
        )?;
        let mut batch = self.database.open_batch();
        batch.insert(
            "jazz_transactions",
            transaction_values(
                tx_node_alias,
                &tx,
                Fate::Pending,
                None,
                DurabilityTier::Local,
            ),
        );
        batch.insert_raw(
            version_storage_table_name_for_schema(
                &table_schema.name,
                stored.layer(),
                write_schema_version,
                self.catalogue.current_schema_version_id,
            ),
            history_primary_key(&stored),
            stored.record.raw().to_vec(),
        );
        self.update_merge_heads_for_content_version(&mut batch, &stored)?;
        self.write_ahead_current_insert(&mut batch, &stored)?;
        for parent in stored.parents() {
            if let Some(parent_alias) = self.node_aliases.get(&parent.node).copied() {
                batch.insert(
                    "jazz_pending_edges",
                    pending_edge_values(tx_node_alias, tx_id, parent_alias, parent),
                );
            }
        }
        self.database.commit_batch(batch)?;
        self.cache_tx_versions(tx_id, vec![stored.clone()]);
        self.record_child_edges(tx_id, stored.parents());
        Ok(tx_id)
    }

    /// Commit a local mergeable write and return its sync commit unit.
    pub fn commit_mergeable_unit(
        &mut self,
        commit: MergeableCommit,
    ) -> Result<(TxId, SyncMessage), Error> {
        let tx_id = self.commit_mergeable(commit)?;
        Ok((tx_id, self.commit_unit_for(tx_id)?))
    }

    /// Rebuild the sync commit unit for an already-committed local transaction
    /// from its stored versions.
    ///
    /// Used by the `Db` sync surface to upload a client's local writes upstream
    /// on a connection. Unlike [`NodeState::commit_mergeable_unit`] this reads the
    /// stored versions (carrying any large-value extent refs), so the shipped
    /// unit matches what the author actually stored.
    pub fn commit_unit_for(&mut self, tx_id: TxId) -> Result<SyncMessage, Error> {
        let tx = self
            .query_transaction(tx_id)?
            .ok_or(Error::MissingTransaction(tx_id))?
            .tx
            .clone();
        let versions = self
            .query_versions_for_tx(tx_id)?
            .into_iter()
            .map(|row| self.version_record_from_row(&row))
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(SyncMessage::CommitUnit { tx, versions })
    }

    /// Open an exclusive transaction over the current snapshot.
    pub fn visible_current_cells(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<BTreeMap<String, Value>>, Error> {
        Ok(self
            .current_rows(table, DurabilityTier::Local)?
            .into_iter()
            .find(|row| row.row_uuid() == row_uuid)
            .map(|row| {
                let table_schema = self.table(table).expect("table exists");
                table_schema
                    .columns
                    .iter()
                    .filter_map(|column| {
                        row.cell(table_schema, &column.name)
                            .map(|value| (column.name.clone(), value))
                    })
                    .collect()
            }))
    }

    /// Return current rows at the requested durability tier.
    pub fn current_rows(
        &mut self,
        table: &str,
        settled: DurabilityTier,
    ) -> Result<Vec<CurrentRow>, Error> {
        let shape = crate::query::Query::from(table).validate(&self.catalogue.schema)?;
        let binding = shape.bind(BTreeMap::new())?;
        self.query_rows(&shape, &binding, settled)
    }

    fn local_layer_winner_tx_id(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
        layer: VersionLayer,
    ) -> Result<Option<TxId>, Error> {
        self.query_local_layer_winner(table, row_uuid, layer)?
            .as_ref()
            .map(|version| self.version_tx_id(version))
            .transpose()
    }

    pub(crate) fn local_content_winner_tx_id(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<TxId>, Error> {
        self.local_layer_winner_tx_id(table, row_uuid, VersionLayer::Content)
    }

    pub(crate) fn local_deletion_winner_tx_id(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<TxId>, Error> {
        self.local_layer_winner_tx_id(table, row_uuid, VersionLayer::Deletion)
    }

    fn rebuild_ahead_current_keys(&mut self) -> Result<(), Error> {
        self.ahead_current_keys.clear();
        self.ahead_current_rows.clear();
        self.ahead_current_latest.clear();
        for table in self.catalogue.schema.tables.clone() {
            let storage_tables = table.ahead_current_storage_tables();
            let content_rows = self
                .database
                .primary_key_scan_raw(&storage_tables[0].name, &[])?
                .into_iter()
                .map(|raw| raw.raw().to_vec())
                .collect::<Vec<_>>();
            let content_descriptor = storage_tables[0].record_schema();
            for raw in content_rows {
                let record = BorrowedRecord::new(&raw, &content_descriptor);
                self.insert_ahead_current_key(
                    table.name.clone(),
                    VersionLayer::Content,
                    RowUuid(record.get_uuid(GlobalCurrentRowRecord::FIELD_ROW_UUID_IDX)?),
                    TxTime(record.get_u64(GlobalCurrentRowRecord::FIELD_TX_TIME_IDX)?),
                    NodeAlias(record.get_u64(GlobalCurrentRowRecord::FIELD_TX_NODE_ID_IDX)?),
                );
            }
            let deletion_descriptor = storage_tables[1].record_schema();
            let deletion_rows = self
                .database
                .primary_key_scan_raw(&storage_tables[1].name, &[])?
                .into_iter()
                .map(|raw| raw.raw().to_vec())
                .collect::<Vec<_>>();
            for raw in deletion_rows {
                let record = BorrowedRecord::new(&raw, &deletion_descriptor);
                self.insert_ahead_current_key(
                    table.name.clone(),
                    VersionLayer::Deletion,
                    RowUuid(record.get_uuid(RegisterGlobalCurrentRowRecord::FIELD_ROW_UUID_IDX)?),
                    TxTime(record.get_u64(RegisterGlobalCurrentRowRecord::FIELD_TX_TIME_IDX)?),
                    NodeAlias(
                        record.get_u64(RegisterGlobalCurrentRowRecord::FIELD_TX_NODE_ID_IDX)?,
                    ),
                );
            }
        }
        Ok(())
    }

    fn insert_ahead_current_key(
        &mut self,
        table: String,
        layer: VersionLayer,
        row_uuid: RowUuid,
        tx_time: TxTime,
        tx_node_alias: NodeAlias,
    ) {
        self.ahead_current_keys
            .insert((table.clone(), layer, row_uuid, tx_time, tx_node_alias));
        self.ahead_current_rows.insert((table.clone(), row_uuid));
        self.ahead_current_latest
            .entry((table, layer, row_uuid))
            .and_modify(|latest| {
                if (tx_time, tx_node_alias) > *latest {
                    *latest = (tx_time, tx_node_alias);
                }
            })
            .or_insert((tx_time, tx_node_alias));
    }

    fn remove_ahead_current_key(
        &mut self,
        table: &str,
        layer: VersionLayer,
        row_uuid: RowUuid,
        tx_time: TxTime,
        tx_node_alias: NodeAlias,
    ) {
        let table_key = table.to_owned();
        self.ahead_current_keys.remove(&(
            table_key.clone(),
            layer,
            row_uuid,
            tx_time,
            tx_node_alias,
        ));
        let latest_key = (table_key.clone(), layer, row_uuid);
        if self.ahead_current_latest.get(&latest_key) == Some(&(tx_time, tx_node_alias)) {
            let start = (table_key.clone(), layer, row_uuid, TxTime(0), NodeAlias(0));
            let end = (
                table_key.clone(),
                layer,
                row_uuid,
                TxTime(u64::MAX),
                NodeAlias(u64::MAX),
            );
            if let Some((_, _, _, next_time, next_alias)) = self
                .ahead_current_keys
                .range(start..=end)
                .next_back()
                .cloned()
            {
                self.ahead_current_latest
                    .insert(latest_key, (next_time, next_alias));
            } else {
                self.ahead_current_latest.remove(&latest_key);
            }
        }
        if !self.ahead_current_latest.contains_key(&(
            table_key.clone(),
            VersionLayer::Content,
            row_uuid,
        )) && !self.ahead_current_latest.contains_key(&(
            table_key.clone(),
            VersionLayer::Deletion,
            row_uuid,
        )) {
            self.ahead_current_rows.remove(&(table_key, row_uuid));
        }
    }

    fn encode_large_value_cells(
        &mut self,
        table: &TableSchema,
        schema_version: SchemaVersionId,
        row_uuid: RowUuid,
        writer: AuthorId,
        mut cells: BTreeMap<String, Value>,
        parent: Option<&VersionRow>,
    ) -> Result<BTreeMap<String, Value>, Error> {
        for column in table
            .columns
            .iter()
            .filter(|column| column.large_value.is_some())
        {
            let Some(Value::Bytes(new_value)) = cells.get(&column.name).cloned() else {
                continue;
            };
            let parent_value = match parent {
                Some(parent) => self.materialize_large_value_column(table, parent, &column.name)?,
                None => Vec::new(),
            };
            match column.large_value {
                Some(LargeValueKind::Text) => {
                    let new_value =
                        self.canonicalize_format_value(table, schema_version, column, &new_value)?;
                    let ops = text_oplog::diff(&parent_value, &new_value)
                        .into_iter()
                        .collect::<Vec<_>>();
                    let ops = self.extent_back_text_ops(writer, row_uuid, &column.name, ops)?;
                    cells.insert(
                        column.name.clone(),
                        Value::Bytes(encode_extent_text_ops(&ops)),
                    );
                }
                Some(LargeValueKind::Blob) => {
                    let ops = text_oplog::diff(&parent_value, &new_value)
                        .into_iter()
                        .collect::<Vec<_>>();
                    let ops = self.extent_back_text_ops(writer, row_uuid, &column.name, ops)?;
                    cells.insert(column.name.clone(), Value::Bytes(text_oplog::encode(&ops)));
                }
                None => {}
            }
        }
        Ok(cells)
    }

    fn canonicalize_format_value(
        &self,
        table: &TableSchema,
        schema_version: SchemaVersionId,
        column: &ColumnSchema,
        authored: &[u8],
    ) -> Result<Vec<u8>, Error> {
        let Some(spec) = column.text_merge_spec.clone() else {
            return Ok(authored.to_vec());
        };
        let Some(strategy) = self
            .text_merge_strategies
            .get(&(spec.strategy_id.clone(), spec.strategy_version))
        else {
            return Ok(authored.to_vec());
        };
        let input = CanonicalizeInput {
            schema_version,
            table: table.name.clone(),
            column: column.name.clone(),
            spec_hash: spec.spec_hash(),
            spec,
        };
        match strategy.canonicalize(authored, &input) {
            Ok(Some(canonical)) => Ok(canonical),
            Ok(None) => Ok(authored.to_vec()),
            Err(_) => Err(Error::InvalidMergeableCommit(
                "format canonicalization failed",
            )),
        }
    }

    fn extent_back_text_ops(
        &self,
        writer: AuthorId,
        row_uuid: RowUuid,
        column: &str,
        ops: Vec<TextOp>,
    ) -> Result<Vec<TextOp>, Error> {
        ops.into_iter()
            .map(|op| match op {
                TextOp::Insert {
                    pos,
                    content: TextContent::Inline(bytes),
                } => {
                    let mut ops = Vec::new();
                    for chunk in bytes.chunks(MAX_CONTENT_EXTENT_BYTES) {
                        let extent = self
                            .content_store()
                            .append(writer, row_uuid, column, chunk)?;
                        ops.push(TextOp::Insert {
                            pos,
                            content: TextContent::Ref(extent),
                        });
                    }
                    Ok(ops)
                }
                TextOp::Insert {
                    content: TextContent::Ref(_),
                    ..
                } => Err(Error::InvalidStoredValue(
                    "text op input already has ref content",
                )),
                TextOp::Delete { pos, len } => Ok(vec![TextOp::Delete { pos, len }]),
            })
            .collect::<Result<Vec<_>, Error>>()
            .map(|chunks| chunks.into_iter().flatten().collect())
    }

    fn materialize_large_value_column(
        &mut self,
        table: &TableSchema,
        winner: &VersionRow,
        column: &str,
    ) -> Result<Vec<u8>, Error> {
        self.large_value_metrics.materializations =
            self.large_value_metrics.materializations.saturating_add(1);
        let winner_tx_id = self.version_tx_id(winner)?;
        let cache_key = large_value_cache_key(table, winner.row_uuid(), column, winner_tx_id);
        if let Some(value) = self.large_value_materialization_cache.get(&cache_key) {
            self.large_value_metrics.last_replayed_ops = 0;
            self.large_value_metrics.last_replayed_versions = 0;
            return Ok(value.clone());
        }
        let mut suffix = Vec::new();
        let mut current = winner_tx_id;
        let mut checkpoint = None;
        loop {
            let version = self
                .query_versions_for_tx(current)?
                .into_iter()
                .find(|version| {
                    version.table() == table.name
                        && version.row_uuid() == winner.row_uuid()
                        && version.layer() == VersionLayer::Content
                })
                .ok_or(Error::MissingTransaction(current))?;
            if let Some(value) =
                self.large_value_checkpoint(table, version.row_uuid(), column, current)?
            {
                checkpoint = Some(value);
                self.large_value_metrics.checkpoint_hits =
                    self.large_value_metrics.checkpoint_hits.saturating_add(1);
                break;
            }
            let parents = version.parents();
            suffix.push(version);
            match parents.as_slice() {
                [] => break,
                [parent] => current = *parent,
                _ => current = self.large_value_primary_parent(&parents)?,
            }
        }
        suffix.reverse();

        let mut value = checkpoint.unwrap_or_default();
        let mut replayed_ops = 0usize;
        for version in &suffix {
            let Some(Value::Bytes(payload)) = version.cell(table, column)? else {
                continue;
            };
            match column_large_value_kind(table, column)? {
                LargeValueKind::Text => {
                    let op = self.decode_text_storage_op(&payload)?;
                    replayed_ops = replayed_ops.checked_add(op.runs().len()).ok_or(
                        Error::InvalidStoredValue("large value replay op count overflow"),
                    )?;
                    value = op
                        .apply(&value)
                        .map_err(|_| Error::InvalidStoredValue("invalid text op payload"))?;
                }
                LargeValueKind::Blob => {
                    let stored_ops = text_oplog::decode(&payload)?;
                    replayed_ops = replayed_ops.checked_add(stored_ops.len()).ok_or(
                        Error::InvalidStoredValue("large value replay op count overflow"),
                    )?;
                    let ops = self.resolve_text_op_refs(stored_ops)?;
                    value = text_oplog::replay(&value, &ops);
                }
            }
        }
        self.large_value_metrics.last_replayed_ops = replayed_ops;
        self.large_value_metrics.total_replayed_ops = self
            .large_value_metrics
            .total_replayed_ops
            .saturating_add(replayed_ops as u64);
        self.large_value_metrics.last_replayed_versions = suffix.len();
        if replayed_ops >= self.large_value_checkpoint_op_interval {
            self.put_large_value_checkpoint(table, winner, column, &value)?;
            self.large_value_metrics.checkpoint_writes =
                self.large_value_metrics.checkpoint_writes.saturating_add(1);
        }
        self.cache_large_value_materialization(cache_key, value.clone());
        Ok(value)
    }

    fn cache_large_value_materialization(&mut self, key: LargeValueCacheKey, value: Vec<u8>) {
        if !self.large_value_materialization_cache.contains_key(&key)
            && self.large_value_materialization_cache.len()
                >= LARGE_VALUE_MATERIALIZATION_CACHE_MAX_ENTRIES
            && let Some(oldest_key) = self
                .large_value_materialization_cache
                .first_key_value()
                .map(|(key, _)| key.clone())
        {
            self.large_value_materialization_cache.remove(&oldest_key);
        }
        self.large_value_materialization_cache.insert(key, value);
    }

    pub(super) fn cached_tx_version_tables(&self, tx_id: TxId) -> Option<BTreeSet<String>> {
        self.query.tx_version_tables_cache.get(&tx_id).cloned()
    }

    pub(super) fn cached_tx_versions(&self, tx_id: TxId) -> Option<Vec<VersionRow>> {
        self.query.tx_versions_cache.get(&tx_id).cloned()
    }

    pub(super) fn cache_tx_version_tables(&mut self, tx_id: TxId, tables: BTreeSet<String>) {
        self.touch_tx_version_cache_entry(tx_id);
        self.query.tx_version_tables_cache.insert(tx_id, tables);
        self.bound_tx_version_cache();
    }

    pub(super) fn cache_tx_versions(&mut self, tx_id: TxId, versions: Vec<VersionRow>) {
        self.touch_tx_version_cache_entry(tx_id);
        self.query.tx_versions_cache.insert(tx_id, versions);
        self.bound_tx_version_cache();
    }

    fn touch_tx_version_cache_entry(&mut self, tx_id: TxId) {
        if self.query.tx_version_tables_cache_order_set.insert(tx_id) {
            self.query.tx_version_tables_cache_order.push_back(tx_id);
        }
    }

    fn bound_tx_version_cache(&mut self) {
        while self.query.tx_version_tables_cache.len() > TX_VERSION_TABLE_CACHE_MAX_ENTRIES
            || self.query.tx_versions_cache.len() > TX_VERSION_TABLE_CACHE_MAX_ENTRIES
        {
            let Some(oldest) = self.query.tx_version_tables_cache_order.pop_front() else {
                break;
            };
            if !self.query.tx_version_tables_cache_order_set.remove(&oldest) {
                continue;
            }
            self.query.tx_version_tables_cache.remove(&oldest);
            self.query.tx_versions_cache.remove(&oldest);
        }
    }

    pub(super) fn invalidate_tx_version_tables_cache(&mut self, tx_id: TxId) {
        self.query.tx_version_tables_cache.remove(&tx_id);
        self.query.tx_versions_cache.remove(&tx_id);
        self.query.tx_version_tables_cache_order_set.remove(&tx_id);
    }

    pub(super) fn invalidate_tx_version_table_names_cache(&mut self, tx_id: TxId) {
        self.query.tx_version_tables_cache.remove(&tx_id);
    }

    fn large_value_checkpoint(
        &self,
        table: &TableSchema,
        row_uuid: RowUuid,
        column: &str,
        version: TxId,
    ) -> Result<Option<Vec<u8>>, Error> {
        self.content_store()
            .checkpoint(&table.name, row_uuid, column, version)
    }

    fn put_large_value_checkpoint(
        &self,
        table: &TableSchema,
        version: &VersionRow,
        column: &str,
        value: &[u8],
    ) -> Result<(), Error> {
        let tx_id = self.version_tx_id(version)?;
        self.content_store()
            .put_checkpoint(&table.name, version.row_uuid(), column, tx_id, value)
    }

    pub(super) fn checkpoint_large_values_for_tx(&mut self, tx_id: TxId) -> Result<(), Error> {
        let versions = self.query_versions_for_tx(tx_id)?;
        self.checkpoint_large_values_for_versions(&versions)
    }

    pub(super) fn checkpoint_large_values_for_versions(
        &mut self,
        versions: &[VersionRow],
    ) -> Result<(), Error> {
        for version in versions {
            if version.layer() != VersionLayer::Content {
                continue;
            }
            let schema_version = self
                .schema_version_for_alias(version.schema_version_alias())
                .ok_or(Error::InvalidStoredValue("unknown schema version alias"))?;
            let table = self.table_in_schema(version.table(), schema_version)?;
            for column in table
                .columns
                .iter()
                .filter(|column| column.large_value.is_some())
            {
                if version.cell(&table, &column.name)?.is_none() {
                    continue;
                }
                if self.large_value_replay_ops_since_checkpoint(&table, &version, &column.name)?
                    < self.large_value_checkpoint_op_interval
                {
                    continue;
                }
                let _ = self.materialize_large_value_column(&table, &version, &column.name)?;
            }
        }
        Ok(())
    }

    fn large_value_replay_ops_since_checkpoint(
        &mut self,
        table: &TableSchema,
        winner: &VersionRow,
        column: &str,
    ) -> Result<usize, Error> {
        let mut replayed_ops = 0usize;
        let mut current = self.version_tx_id(winner)?;
        loop {
            let version = self
                .query_versions_for_tx(current)?
                .into_iter()
                .find(|version| {
                    version.table() == table.name
                        && version.row_uuid() == winner.row_uuid()
                        && version.layer() == VersionLayer::Content
                })
                .ok_or(Error::MissingTransaction(current))?;
            if self
                .large_value_checkpoint(table, version.row_uuid(), column, current)?
                .is_some()
            {
                return Ok(replayed_ops);
            }
            if let Some(Value::Bytes(payload)) = version.cell(table, column)? {
                let op_count = match column_large_value_kind(table, column)? {
                    LargeValueKind::Text => self.decode_text_storage_op(&payload)?.runs().len(),
                    LargeValueKind::Blob => text_oplog::decode(&payload)?.len(),
                };
                replayed_ops =
                    replayed_ops
                        .checked_add(op_count)
                        .ok_or(Error::InvalidStoredValue(
                            "large value replay op count overflow",
                        ))?;
            }
            let parents = version.parents();
            match parents.as_slice() {
                [] => return Ok(replayed_ops),
                [parent] => current = *parent,
                _ => current = self.large_value_primary_parent(&parents)?,
            }
        }
    }

    fn large_value_column_len(
        &mut self,
        table: &TableSchema,
        winner: &VersionRow,
        column: &str,
    ) -> Result<usize, Error> {
        let mut suffix = Vec::new();
        let mut current = self.version_tx_id(winner)?;
        let mut checkpoint_len = None;
        loop {
            let version = self
                .query_versions_for_tx(current)?
                .into_iter()
                .find(|version| {
                    version.table() == table.name
                        && version.row_uuid() == winner.row_uuid()
                        && version.layer() == VersionLayer::Content
                })
                .ok_or(Error::MissingTransaction(current))?;
            if let Some(value) =
                self.large_value_checkpoint(table, version.row_uuid(), column, current)?
            {
                checkpoint_len = Some(value.len());
                break;
            }
            let parents = version.parents();
            suffix.push(version);
            match parents.as_slice() {
                [] => break,
                [parent] => current = *parent,
                _ => current = self.large_value_primary_parent(&parents)?,
            }
        }
        suffix.reverse();

        let mut value_len = checkpoint_len.unwrap_or_default();
        for version in &suffix {
            let Some(Value::Bytes(payload)) = version.cell(table, column)? else {
                continue;
            };
            match column_large_value_kind(table, column)? {
                LargeValueKind::Text => {
                    let op = self.decode_text_storage_op(&payload)?;
                    let value = vec![0; value_len];
                    value_len = op
                        .apply(&value)
                        .map_err(|_| Error::InvalidStoredValue("invalid text op payload"))?
                        .len();
                }
                LargeValueKind::Blob => {
                    for op in text_oplog::decode(&payload)? {
                        match op {
                            TextOp::Insert { content, .. } => {
                                value_len =
                                    value_len.checked_add(text_content_len(&content)?).ok_or(
                                        Error::InvalidStoredValue("large value length overflow"),
                                    )?;
                            }
                            TextOp::Delete { len, .. } => {
                                value_len = value_len.checked_sub(len).ok_or(
                                    Error::InvalidStoredValue("large value length underflow"),
                                )?;
                            }
                        }
                    }
                }
            }
        }
        Ok(value_len)
    }

    fn large_value_primary_parent(&mut self, parents: &[TxId]) -> Result<TxId, Error> {
        parents
            .iter()
            .copied()
            .map(|parent| {
                let made_at = self
                    .transaction_made_at(parent)?
                    .ok_or(Error::MissingTransaction(parent))?;
                Ok((made_at.sort_key(parent.node), parent))
            })
            .collect::<Result<Vec<_>, Error>>()?
            .into_iter()
            .max_by_key(|(key, _)| *key)
            .map(|(_, parent)| parent)
            .ok_or(Error::InvalidStoredValue(
                "large value materialization requires at least one parent",
            ))
    }

    /// Deterministic counters for large-value materialization and checkpoint use.
    pub fn large_value_metrics(&self) -> &LargeValueMetrics {
        &self.large_value_metrics
    }

    /// Reset large-value materialization counters.
    pub fn reset_large_value_metrics(&mut self) {
        self.large_value_metrics = LargeValueMetrics::default();
    }

    fn resolve_text_op_refs(&self, ops: Vec<TextOp>) -> Result<Vec<TextOp>, Error> {
        ops.into_iter()
            .map(|op| match op {
                TextOp::Insert {
                    pos,
                    content: TextContent::Ref(extent),
                } => Ok(TextOp::Insert {
                    pos,
                    content: TextContent::Inline(self.content_store().read(&extent)?),
                }),
                TextOp::Insert { .. } | TextOp::Delete { .. } => Ok(op),
            })
            .collect()
    }

    fn decode_text_storage_op(&self, payload: &[u8]) -> Result<PlainTextOp, Error> {
        if let Some(extent_payload) = payload.strip_prefix(TEXT_EXTENT_OPS_MAGIC) {
            let ops = self.resolve_text_op_refs(text_oplog::decode(extent_payload)?)?;
            let mut runs = Vec::new();
            let mut cursor = 0usize;
            for op in ops {
                match op {
                    TextOp::Insert {
                        pos,
                        content: TextContent::Inline(bytes),
                    } => {
                        if pos > cursor {
                            runs.push(PlainTextRun::Retain(pos - cursor));
                            cursor = pos;
                        }
                        runs.push(PlainTextRun::Insert(bytes));
                    }
                    TextOp::Insert {
                        content: TextContent::Ref(_),
                        ..
                    } => {
                        return Err(Error::InvalidStoredValue(
                            "text extent op refs must be resolved",
                        ));
                    }
                    TextOp::Delete { pos, len } => {
                        if pos > cursor {
                            runs.push(PlainTextRun::Retain(pos - cursor));
                            cursor = pos;
                        }
                        runs.push(PlainTextRun::Delete(len));
                        cursor = cursor
                            .checked_add(len)
                            .ok_or(Error::InvalidStoredValue("text extent op cursor overflow"))?;
                    }
                }
            }
            return Ok(PlainTextOp::new(runs));
        }
        decode_plain_text_op(payload)
    }

    fn materialize_current_row(
        &mut self,
        table: &TableSchema,
        row: CurrentRow,
    ) -> Result<CurrentRow, Error> {
        if !table
            .columns
            .iter()
            .any(|column| column.large_value.is_some())
        {
            return Ok(row);
        }
        let Some((tx_time, tx_node_alias)) = row.projected_tx_alias() else {
            return Ok(row);
        };
        let Some(version) = self.query_version_by_alias(
            &table.name,
            row.row_uuid(),
            VersionLayer::Content,
            tx_time,
            tx_node_alias,
        )?
        else {
            return Ok(row);
        };
        self.current_row_from_materialized_version(table, &version)
    }

    pub(crate) fn local_current_row(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<CurrentRow>, Error> {
        let table_schema =
            self.table_in_schema(table, self.catalogue.current_write_schema.schema)?;
        let content = self.local_current_content_row_candidate(&table_schema, row_uuid)?;
        let deletion = self.local_current_deletion_candidate(&table_schema, row_uuid)?;
        if let (Some((_, content_tx)), Some((deletion, deletion_tx))) = (&content, &deletion)
            && deletion_tx > content_tx
            && *deletion == DeletionEvent::Deleted
        {
            return Ok(None);
        }
        content
            .map(|(row, _)| self.materialize_current_row(&table_schema, row))
            .transpose()
    }

    fn local_current_content_row_candidate(
        &mut self,
        table: &TableSchema,
        row_uuid: RowUuid,
    ) -> Result<Option<(CurrentRow, (TxTime, NodeUuid))>, Error> {
        let mut candidates = Vec::new();
        let global_tables = table.global_current_storage_tables();
        if let Some(raw) = self
            .database
            .primary_key_get_raw(&global_tables[0].name, &[Value::Uuid(row_uuid.0)])?
        {
            let record = raw.record();
            let tx = self.current_record_sort_key(record)?;
            candidates.push((decode_current_row(table, record)?, tx));
        }
        let ahead_tables = table.ahead_current_storage_tables();
        if let Some((tx_time, tx_node_alias)) = self
            .ahead_current_latest
            .get(&(table.name.clone(), VersionLayer::Content, row_uuid))
            .copied()
        {
            if let Some(raw) = self.database.primary_key_get_raw(
                &ahead_tables[0].name,
                &[
                    Value::Uuid(row_uuid.0),
                    Value::U64(tx_time.0),
                    Value::U64(tx_node_alias.0),
                ],
            )? {
                let record = raw.record();
                let tx = self.current_record_sort_key(record)?;
                candidates.push((decode_current_row(table, record)?, tx));
            }
        }
        Ok(candidates.into_iter().max_by_key(|(_, tx)| *tx))
    }

    fn local_current_deletion_candidate(
        &mut self,
        table: &TableSchema,
        row_uuid: RowUuid,
    ) -> Result<Option<(DeletionEvent, (TxTime, NodeUuid))>, Error> {
        let mut candidates = Vec::new();
        let global_tables = table.global_current_storage_tables();
        if let Some(raw) = self
            .database
            .primary_key_get_raw(&global_tables[1].name, &[Value::Uuid(row_uuid.0)])?
        {
            let record = raw.record();
            candidates.push((
                deletion_event_from_value(
                    record.get_idx(RegisterGlobalCurrentRowRecord::FIELD__DELETION_IDX)?,
                )?,
                self.current_record_sort_key(record)?,
            ));
        }
        let ahead_tables = table.ahead_current_storage_tables();
        if let Some((tx_time, tx_node_alias)) = self
            .ahead_current_latest
            .get(&(table.name.clone(), VersionLayer::Deletion, row_uuid))
            .copied()
        {
            if let Some(raw) = self.database.primary_key_get_raw(
                &ahead_tables[1].name,
                &[
                    Value::Uuid(row_uuid.0),
                    Value::U64(tx_time.0),
                    Value::U64(tx_node_alias.0),
                ],
            )? {
                let record = raw.record();
                candidates.push((
                    deletion_event_from_value(
                        record.get_idx(RegisterGlobalCurrentRowRecord::FIELD__DELETION_IDX)?,
                    )?,
                    self.current_record_sort_key(record)?,
                ));
            }
        }
        Ok(candidates.into_iter().max_by_key(|(_, tx)| *tx))
    }

    fn current_record_sort_key(
        &self,
        record: BorrowedRecord<'_>,
    ) -> Result<(TxTime, NodeUuid), Error> {
        let tx_time = TxTime(record.get_u64(GlobalCurrentRowRecord::FIELD_TX_TIME_IDX)?);
        let tx_node_alias =
            NodeAlias(record.get_u64(GlobalCurrentRowRecord::FIELD_TX_NODE_ID_IDX)?);
        let tx_node = self
            .node_aliases
            .iter()
            .find_map(|(node, alias)| (*alias == tx_node_alias).then_some(*node))
            .ok_or(Error::InvalidStoredValue(
                "current row references unknown node alias",
            ))?;
        Ok((tx_time, tx_node))
    }

    fn current_row_from_materialized_version(
        &mut self,
        table: &TableSchema,
        version: &VersionRow,
    ) -> Result<CurrentRow, Error> {
        if !table
            .columns
            .iter()
            .any(|column| column.large_value.is_some())
        {
            return current_row_from_version_projection(table, version);
        }
        let cells = self.materialized_cells_for_version(table, version)?;
        current_row_from_materialized_cells(table, version, &cells)
    }

    fn materialized_cells_for_version(
        &mut self,
        table: &TableSchema,
        version: &VersionRow,
    ) -> Result<BTreeMap<String, Value>, Error> {
        if !table
            .columns
            .iter()
            .any(|column| column.large_value.is_some())
        {
            return version.cells(table);
        }
        let mut cells = BTreeMap::new();
        for column in &table.columns {
            let value = if let Some(kind) = column.large_value {
                Some(Value::Bytes(self.large_value_handle_for_version(
                    table,
                    version,
                    &column.name,
                    kind,
                )?))
            } else {
                version.cell(table, &column.name)?
            };
            if let Some(value) = value {
                cells.insert(column.name.clone(), value);
            }
        }
        Ok(cells)
    }

    fn large_value_handle_for_version(
        &mut self,
        table: &TableSchema,
        version: &VersionRow,
        column: &str,
        kind: LargeValueKind,
    ) -> Result<Vec<u8>, Error> {
        let len = self.large_value_column_len(table, version, column)?;
        let refs = self.large_value_extent_refs_for_version(table, version, column, kind)?;
        let tx_id = self.version_tx_id(version)?;
        encode_large_value_handle(table, version.row_uuid(), column, tx_id, kind, len, refs)
    }

    fn large_value_extent_refs_for_version(
        &mut self,
        table: &TableSchema,
        winner: &VersionRow,
        column: &str,
        kind: LargeValueKind,
    ) -> Result<Vec<content_store::Extent>, Error> {
        let mut suffix = Vec::new();
        let mut current = self.version_tx_id(winner)?;
        loop {
            let version = self
                .query_versions_for_tx(current)?
                .into_iter()
                .find(|version| {
                    version.table() == table.name
                        && version.row_uuid() == winner.row_uuid()
                        && version.layer() == VersionLayer::Content
                })
                .ok_or(Error::MissingTransaction(current))?;
            let parents = version.parents();
            suffix.push(version);
            match parents.as_slice() {
                [] => break,
                [parent] => current = *parent,
                _ => current = self.large_value_primary_parent(&parents)?,
            }
        }
        suffix.reverse();

        let mut refs = Vec::new();
        for version in &suffix {
            let Some(Value::Bytes(payload)) = version.cell(table, column)? else {
                continue;
            };
            match kind {
                LargeValueKind::Text => {
                    if let Some(extent_payload) = payload.strip_prefix(TEXT_EXTENT_OPS_MAGIC) {
                        refs.extend(content_refs_in_text_ops(text_oplog::decode(
                            extent_payload,
                        )?));
                    }
                }
                LargeValueKind::Blob => {
                    refs.extend(content_refs_in_text_ops(text_oplog::decode(&payload)?));
                }
            }
        }
        refs.sort();
        refs.dedup();
        Ok(refs)
    }

    /// Materialize the bytes referenced by a large-value handle returned in a row cell.
    pub fn hydrate_large_value_handle(&mut self, handle: &[u8]) -> Result<Vec<u8>, Error> {
        let handle = decode_large_value_handle(handle)?;
        let table = self.table(&handle.table)?.clone();
        let version = self
            .query_versions_for_tx(handle.tx_id)?
            .into_iter()
            .find(|version| {
                version.table() == handle.table
                    && version.row_uuid() == handle.row_uuid
                    && version.layer() == VersionLayer::Content
            })
            .ok_or(Error::MissingTransaction(handle.tx_id))?;
        if column_large_value_kind(&table, &handle.column)? != handle.kind {
            return Err(Error::InvalidStoredValue(
                "large-value handle kind mismatch",
            ));
        }
        self.materialize_large_value_column(&table, &version, &handle.column)
    }

    /// Subscribe to the raw history storage table.
    pub fn sync_metrics(&self) -> &SyncMetrics {
        &self.sync_metrics
    }

    pub(crate) fn record_dropped_peer_request(&mut self) {
        self.sync_metrics.dropped_peer_request_messages += 1;
    }

    pub(crate) fn record_transport_backpressure_retry(&mut self) {
        self.sync_metrics.transport_backpressure_retries += 1;
    }

    pub(crate) fn record_authoritative_reset_missing_payload_fallback(&mut self) {
        self.sync_metrics
            .authoritative_reset_missing_payload_fallbacks += 1;
    }

    pub(crate) fn record_peer_payload_inventory_missing_fallback(&mut self) {
        self.sync_metrics.peer_payload_inventory_missing_fallbacks += 1;
    }

    /// Deterministic counters for query-engine read authorization paths.
    pub fn query_engine_read_metrics(&self) -> &QueryEngineReadMetrics {
        &self.query_engine_read_metrics
    }

    /// Reset query-engine read authorization counters.
    pub fn reset_query_engine_read_metrics(&mut self) {
        self.query_engine_read_metrics = QueryEngineReadMetrics::default();
    }

    /// Published schema-version payloads known to this node.
    pub fn catalogue_schemas(&self) -> &BTreeMap<SchemaVersionId, SchemaVersion> {
        &self.catalogue.catalogue_schemas
    }

    /// Published migration lenses known to this node.
    pub fn catalogue_lenses(&self) -> &BTreeMap<MigrationLensId, MigrationLens> {
        &self.catalogue.catalogue_lenses
    }

    /// Current write-schema pointer known to this node.
    pub fn current_write_schema(&self) -> CurrentWriteSchema {
        self.catalogue.current_write_schema
    }

    /// Durable partition registry entries known at open time.
    pub fn partitions(&self) -> &BTreeSet<(String, SchemaVersionId)> {
        &self.catalogue.partitions
    }

    /// Return a historical read handle at an exact global settle position.
    pub fn at(&mut self, position: GlobalSeq) -> HistoricalRead<'_, S> {
        HistoricalRead {
            node: self,
            position,
        }
    }

    /// Return a historical read handle for the latest settle position whose
    /// transaction time is less than or equal to `time`.
    ///
    /// This is deterministic, not a wall-clock truth claim: concurrent or
    /// offline writers can settle in an order that disagrees with transaction
    /// HLC time, so this convenience address is best-effort under clock skew.
    pub fn at_time(&mut self, time: TxTime) -> Result<HistoricalRead<'_, S>, Error> {
        let position = self.resolve_time_travel_position(time)?;
        Ok(self.at(position))
    }

    /// Return whether this node can answer a historical query locally.
    ///
    /// v1 is conservative: authorities/history-complete nodes can answer cuts
    /// up to their contiguous applied watermark; partial clients return false
    /// so callers route the one-shot read to a server in a later protocol slice.
    pub fn is_history_complete_for(&self, _shape: &ValidatedQuery, position: GlobalSeq) -> bool {
        self.history_complete && position <= self.clock.applied_global_watermark
    }

    /// Return current rows for a subscription at the requested tier.
    pub fn subscription_current_rows(
        &mut self,
        table: &str,
        settled: DurabilityTier,
    ) -> Result<Vec<CurrentRow>, Error> {
        let table_schema = self.table(table)?.clone();
        let subscription = self.whole_table_subscription_key(table)?;
        match settled {
            DurabilityTier::None | DurabilityTier::Local => self.current_rows(table, settled),
            DurabilityTier::Edge => self.current_rows(table, settled),
            DurabilityTier::Global => {
                let binding_view_key =
                    BindingViewKey::from_canonical_subscription_key(subscription);
                let Some(row_result_set) = self.query.settled_result_sets.get(&binding_view_key)
                else {
                    return Ok(Vec::new());
                };
                let row_entries = row_result_set
                    .iter()
                    .filter_map(ResultMemberEntry::as_row)
                    .collect::<Vec<_>>();
                let content_descriptor = table_schema.history_storage_table().record_schema();
                let mut rows = Vec::new();
                for (entry_table, row_uuid, tx_id) in row_entries {
                    if entry_table.as_str() != table {
                        continue;
                    }
                    let tx_node_alias = self
                        .node_aliases
                        .get(&tx_id.node)
                        .copied()
                        .ok_or(Error::MissingTransaction(tx_id))?;
                    let version = self
                        .query_version_by_alias_with_descriptor(
                            table,
                            row_uuid,
                            VersionLayer::Content,
                            tx_id.time,
                            tx_node_alias,
                            &content_descriptor,
                        )?
                        .ok_or(Error::MissingTransaction(tx_id))?;
                    rows.push(self.current_row_from_materialized_version(&table_schema, &version)?);
                }
                sort_current_rows(&mut rows);
                Ok(rows)
            }
        }
    }

    /// Return the legacy transaction fate tuple.
    pub fn transaction_state(
        &mut self,
        tx_id: TxId,
    ) -> Option<(Fate, Option<GlobalSeq>, DurabilityTier)> {
        self.transaction_record(tx_id)
            .map(|record| (record.fate, record.global_seq, record.durability))
    }

    /// Return the durable audit record for a transaction, including rejected
    /// transactions whose row versions were removed from history.
    pub fn transaction_record(&mut self, tx_id: TxId) -> Option<TransactionRecord> {
        self.query_transaction(tx_id)
            .ok()
            .flatten()
            .map(|stored| stored.to_record())
    }

    /// Resolve creator/updater provenance for a projected current row.
    pub fn row_provenance(&mut self, row: &CurrentRow) -> Result<Option<RowProvenance>, Error> {
        row.provenance()
    }

    pub(crate) fn current_row_tx_id(&mut self, row: &CurrentRow) -> Option<TxId> {
        let (time, alias) = row.projected_tx_alias()?;
        Some(TxId::new(time, self.resolve_node_alias(alias).ok()??))
    }

    pub(crate) fn persist_known_state_fact(
        &self,
        binding_view_key: BindingViewKey,
        settled_through: GlobalSeq,
    ) -> Result<(), Error> {
        self.database
            .direct_record_store(KNOWN_STATE_FACTS_STORE)?
            .set(
                &known_state_fact_key(binding_view_key),
                &[Value::U64(settled_through.0)],
            )?;
        Ok(())
    }

    pub(crate) fn load_known_state_fact(
        &mut self,
        binding_view_key: BindingViewKey,
    ) -> Result<Option<GlobalSeq>, Error> {
        let store = self.database.direct_record_store(KNOWN_STATE_FACTS_STORE)?;
        let Some(record) = store.get(&known_state_fact_key(binding_view_key))? else {
            return Ok(None);
        };
        let settled_through = match record.get_idx(0)? {
            Value::U64(value) => GlobalSeq(value),
            _ => {
                return Err(Error::InvalidStoredValue(
                    "known-state settled-through must be u64",
                ));
            }
        };
        self.query
            .settled_through_by_binding_view
            .insert(binding_view_key, settled_through);
        Ok(Some(settled_through))
    }

    pub(crate) fn clear_all_known_state_facts(&mut self) -> Result<(), Error> {
        let store = self.database.direct_record_store(KNOWN_STATE_FACTS_STORE)?;
        let keys = store
            .prefix_entries(&[])?
            .into_iter()
            .map(|entry| entry.key)
            .collect::<Vec<_>>();
        for key in keys {
            store.delete(&key)?;
        }
        self.query.settled_through_by_binding_view.clear();
        self.clear_all_settled_result_state()?;
        Ok(())
    }

    pub(crate) fn persist_settled_result_state_delta(
        &self,
        binding_view_key: BindingViewKey,
        cleared: bool,
        member_adds: &[ResultMemberEntry],
        member_removes: &[ResultMemberEntry],
        member_rewrite: Option<&BTreeSet<ResultMemberEntry>>,
        fact_adds: &[ViewFactEntry],
        fact_removes: &[ViewFactEntry],
        fact_rewrite: Option<&BTreeSet<ViewFactEntry>>,
    ) -> Result<(), Error> {
        self.persist_settled_result_members_delta(
            binding_view_key,
            cleared,
            member_adds,
            member_removes,
            member_rewrite,
        )?;
        self.persist_settled_program_facts_delta(
            binding_view_key,
            cleared,
            fact_adds,
            fact_removes,
            fact_rewrite,
        )?;
        Ok(())
    }

    fn persist_settled_result_members_delta(
        &self,
        binding_view_key: BindingViewKey,
        cleared: bool,
        adds: &[ResultMemberEntry],
        removes: &[ResultMemberEntry],
        rewrite: Option<&BTreeSet<ResultMemberEntry>>,
    ) -> Result<(), Error> {
        let store = self
            .database
            .direct_record_store(SETTLED_RESULT_MEMBERS_STORE)?;
        if cleared || rewrite.is_some() {
            let prefix = binding_view_store_prefix(binding_view_key);
            let keys = store
                .prefix_entries(&prefix)?
                .into_iter()
                .map(|entry| entry.key)
                .collect::<Vec<_>>();
            let mut operations = keys
                .into_iter()
                .map(|key| DirectRecordStoreWrite::Delete { key })
                .collect::<Vec<_>>();
            if let Some(members) = rewrite {
                for member in members {
                    operations.push(DirectRecordStoreWrite::Set {
                        key: settled_result_member_key(binding_view_key, member)?,
                        value: vec![Value::U64(1)],
                    });
                }
            } else {
                for member in adds {
                    operations.push(DirectRecordStoreWrite::Set {
                        key: settled_result_member_key(binding_view_key, member)?,
                        value: vec![Value::U64(1)],
                    });
                }
            }
            store.write_many(&operations)?;
            return Ok(());
        }

        let mut operations = Vec::with_capacity(removes.len() + adds.len());
        for member in removes {
            operations.push(DirectRecordStoreWrite::Delete {
                key: settled_result_member_key(binding_view_key, member)?,
            });
        }
        for member in adds {
            operations.push(DirectRecordStoreWrite::Set {
                key: settled_result_member_key(binding_view_key, member)?,
                value: vec![Value::U64(1)],
            });
        }
        if !operations.is_empty() {
            store.write_many(&operations)?;
        }
        Ok(())
    }

    fn persist_settled_program_facts_delta(
        &self,
        binding_view_key: BindingViewKey,
        cleared: bool,
        adds: &[ViewFactEntry],
        removes: &[ViewFactEntry],
        rewrite: Option<&BTreeSet<ViewFactEntry>>,
    ) -> Result<(), Error> {
        let store = self
            .database
            .direct_record_store(SETTLED_PROGRAM_FACTS_STORE)?;
        if cleared || rewrite.is_some() {
            let prefix = binding_view_store_prefix(binding_view_key);
            let keys = store
                .prefix_entries(&prefix)?
                .into_iter()
                .map(|entry| entry.key)
                .collect::<Vec<_>>();
            let mut operations = keys
                .into_iter()
                .map(|key| DirectRecordStoreWrite::Delete { key })
                .collect::<Vec<_>>();
            if let Some(facts) = rewrite {
                for fact in facts {
                    operations.push(DirectRecordStoreWrite::Set {
                        key: settled_program_fact_key(binding_view_key, fact)?,
                        value: vec![Value::U64(1)],
                    });
                }
            } else {
                for fact in adds {
                    operations.push(DirectRecordStoreWrite::Set {
                        key: settled_program_fact_key(binding_view_key, fact)?,
                        value: vec![Value::U64(1)],
                    });
                }
            }
            store.write_many(&operations)?;
            return Ok(());
        }

        let mut operations = Vec::with_capacity(removes.len() + adds.len());
        for fact in removes {
            operations.push(DirectRecordStoreWrite::Delete {
                key: settled_program_fact_key(binding_view_key, fact)?,
            });
        }
        for fact in adds {
            operations.push(DirectRecordStoreWrite::Set {
                key: settled_program_fact_key(binding_view_key, fact)?,
                value: vec![Value::U64(1)],
            });
        }
        if !operations.is_empty() {
            store.write_many(&operations)?;
        }
        Ok(())
    }

    fn clear_all_settled_result_state(&mut self) -> Result<(), Error> {
        for store_name in [SETTLED_RESULT_MEMBERS_STORE, SETTLED_PROGRAM_FACTS_STORE] {
            let store = self.database.direct_record_store(store_name)?;
            let keys = store
                .prefix_entries(&[])?
                .into_iter()
                .map(|entry| entry.key)
                .collect::<Vec<_>>();
            for key in keys {
                store.delete(&key)?;
            }
        }
        self.query.settled_result_sets.clear();
        self.query.settled_result_row_index.clear();
        self.query.settled_program_facts.clear();
        Ok(())
    }

    pub(crate) fn close(&mut self) -> Result<(), Error> {
        self.database.flush()?;
        self.persist_clean_close_marker()?;
        self.database.close()?;
        Ok(())
    }

    fn persist_clean_close_marker(&self) -> Result<(), Error> {
        self.database
            .direct_record_store(CLEAN_CLOSE_MARKERS_STORE)?
            .set(
                &clean_close_marker_key(),
                &[
                    Value::U64(CLEAN_CLOSE_MARKER_VERSION),
                    Value::Uuid(self.node_uuid.0),
                ],
            )?;
        Ok(())
    }

    fn take_valid_clean_close_marker(&mut self) -> Result<bool, Error> {
        let store = self
            .database
            .direct_record_store(CLEAN_CLOSE_MARKERS_STORE)?;
        let key = clean_close_marker_key();
        let Some(record) = store.get(&key)? else {
            return Ok(false);
        };
        store.delete(&key)?;

        let version = match record.get_idx(0)? {
            Value::U64(value) => value,
            _ => return Ok(false),
        };
        let node = match record.get_idx(1)? {
            Value::Uuid(value) => value,
            _ => return Ok(false),
        };
        Ok(version == CLEAN_CLOSE_MARKER_VERSION && node == self.node_uuid.0)
    }

    pub(super) fn persist_storage_consistency_marker_through(
        &self,
        tx_time: TxTime,
    ) -> Result<(), Error> {
        let store = self
            .database
            .direct_record_store(STORAGE_CONSISTENCY_MARKERS_STORE)?;
        let key = storage_consistency_marker_key();
        if let Some(record) = store.get(&key)?
            && matches!(
                record.get_idx(0)?,
                Value::U64(STORAGE_CONSISTENCY_MARKER_VERSION)
            )
            && matches!(record.get_idx(1)?, Value::Uuid(node) if node == self.node_uuid.0)
            && let Value::U64(existing) = record.get_idx(2)?
            && existing >= tx_time.0
        {
            return Ok(());
        }
        store.set(
            &key,
            &[
                Value::U64(STORAGE_CONSISTENCY_MARKER_VERSION),
                Value::Uuid(self.node_uuid.0),
                Value::U64(tx_time.0),
            ],
        )?;
        Ok(())
    }

    fn valid_storage_consistency_marker(&self) -> Result<Option<TxTime>, Error> {
        let store = self
            .database
            .direct_record_store(STORAGE_CONSISTENCY_MARKERS_STORE)?;
        let Some(record) = store.get(&storage_consistency_marker_key())? else {
            return Ok(None);
        };
        let version = match record.get_idx(0)? {
            Value::U64(value) => value,
            _ => return Ok(None),
        };
        let node = match record.get_idx(1)? {
            Value::Uuid(value) => value,
            _ => return Ok(None),
        };
        let tx_time = match record.get_idx(2)? {
            Value::U64(value) => value,
            _ => return Ok(None),
        };
        if version == STORAGE_CONSISTENCY_MARKER_VERSION && node == self.node_uuid.0 {
            Ok(Some(TxTime(tx_time)))
        } else {
            Ok(None)
        }
    }

    fn recover_known_state_facts(&mut self) -> Result<(), Error> {
        self.query.settled_through_by_binding_view.clear();
        self.query.settled_result_sets.clear();
        self.query.settled_result_row_index.clear();
        self.query.settled_program_facts.clear();
        let store = self.database.direct_record_store(KNOWN_STATE_FACTS_STORE)?;
        for entry in store.prefix_entries(&[])? {
            if entry.key.len() != 3 {
                return Err(Error::InvalidStoredValue(
                    "known-state fact key must have three columns",
                ));
            }
            let shape_id = match &entry.key[0] {
                Value::Uuid(uuid) => ShapeId(*uuid),
                _ => {
                    return Err(Error::InvalidStoredValue(
                        "known-state shape id must be uuid",
                    ));
                }
            };
            let binding_id = match &entry.key[1] {
                Value::Uuid(uuid) => BindingId(*uuid),
                _ => {
                    return Err(Error::InvalidStoredValue(
                        "known-state binding id must be uuid",
                    ));
                }
            };
            let read_view = match &entry.key[2] {
                Value::Uuid(uuid) => ReadViewKey { id: *uuid },
                _ => {
                    return Err(Error::InvalidStoredValue(
                        "known-state read view must be uuid",
                    ));
                }
            };
            let settled_through = match entry.value.get_idx(0)? {
                Value::U64(value) => GlobalSeq(value),
                _ => {
                    return Err(Error::InvalidStoredValue(
                        "known-state settled-through must be u64",
                    ));
                }
            };
            self.query.settled_through_by_binding_view.insert(
                BindingViewKey::new(shape_id, binding_id, read_view),
                settled_through,
            );
        }
        let store = self
            .database
            .direct_record_store(SETTLED_RESULT_MEMBERS_STORE)?;
        let mut recovered_members = Vec::new();
        for entry in store.prefix_entries(&[])? {
            if entry.key.len() != 4 {
                return Err(Error::InvalidStoredValue(
                    "settled result member key must have four columns",
                ));
            }
            let binding_view_key = binding_view_key_from_store_key(
                &entry.key,
                "settled result member binding key must be valid",
            )?;
            let member_bytes = match &entry.key[3] {
                Value::Bytes(bytes) => bytes,
                _ => {
                    return Err(Error::InvalidStoredValue(
                        "settled result member payload must be bytes",
                    ));
                }
            };
            let member = postcard::from_bytes::<ResultMemberEntry>(member_bytes).map_err(|_| {
                Error::InvalidStoredValue("settled result member payload must decode")
            })?;
            recovered_members.push((binding_view_key, member));
        }
        drop(store);
        for (binding_view_key, member) in recovered_members {
            self.insert_settled_result_member_indexed(binding_view_key, member);
        }

        let store = self
            .database
            .direct_record_store(SETTLED_PROGRAM_FACTS_STORE)?;
        for entry in store.prefix_entries(&[])? {
            if entry.key.len() != 4 {
                return Err(Error::InvalidStoredValue(
                    "settled program fact key must have four columns",
                ));
            }
            let binding_view_key = binding_view_key_from_store_key(
                &entry.key,
                "settled program fact binding key must be valid",
            )?;
            let fact_bytes = match &entry.key[3] {
                Value::Bytes(bytes) => bytes,
                _ => {
                    return Err(Error::InvalidStoredValue(
                        "settled program fact payload must be bytes",
                    ));
                }
            };
            let fact = postcard::from_bytes::<ViewFactEntry>(fact_bytes).map_err(|_| {
                Error::InvalidStoredValue("settled program fact payload must decode")
            })?;
            self.query
                .settled_program_facts
                .entry(binding_view_key)
                .or_default()
                .insert(fact);
        }
        Ok(())
    }

    /// Return locally-originated rejected transactions retained for retry.
    pub fn rejected_transactions(&self) -> Vec<TxId> {
        self.rejections
            .rejected_transactions
            .keys()
            .copied()
            .collect()
    }

    /// Return a locally-originated rejected transaction payload retained for retry.
    pub fn rejected_transaction(&self, tx_id: TxId) -> Option<RejectedTransaction> {
        self.rejections.rejected_transactions.get(&tx_id).cloned()
    }

    /// Discard a locally-retained rejected transaction after the app acknowledges it.
    pub fn discard_rejection(&mut self, tx_id: TxId) -> Result<(), Error> {
        if tx_id.node != self.node_uuid {
            return Ok(());
        }
        let Some(alias) = self.node_aliases.get(&self.node_uuid).copied() else {
            return Ok(());
        };
        let mut batch = self.database.open_batch();
        batch.delete(
            "jazz_rejected_transactions",
            rejected_transaction_primary_key(alias, tx_id),
        );
        for table in self.catalogue.schema.tables.clone() {
            let storage_table = rejected_versions_table_name(&table.name);
            for raw in self.database.primary_key_scan_raw(
                &storage_table,
                &[Value::U64(tx_id.time.0), Value::U64(alias.0)],
            )? {
                let record = raw.record();
                let node_id = record.get_u64(RejectedVersionRowRecord::FIELD_TX_NODE_ID_IDX)?;
                let time = record.get_u64(RejectedVersionRowRecord::FIELD_TX_TIME_IDX)?;
                if node_id != alias.0 || time != tx_id.time.0 {
                    continue;
                }
                batch.delete(
                    storage_table.clone(),
                    rejected_version_primary_key_from_record(&record)?,
                );
            }
        }
        self.database.commit_batch(batch)?;
        self.rejections.rejected_transactions.remove(&tx_id);
        Ok(())
    }

    /// Return stored edit-history entries for one row ordered by HLC
    /// observation order.
    ///
    /// The parents DAG is the authoritative causal structure; HLC order is a
    /// readable observation order. This method intentionally does no policy
    /// filtering: per the README visibility rule, if a current version is
    /// readable then all history for that visible row is readable, and a node
    /// only stores versions it may hold. Rejected transaction versions are not
    /// returned because rejection cleanup removes their stored row versions;
    /// use [`SingleNode::transaction_record`] for the transaction audit state.
    pub fn row_history(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Vec<HistoryEntry>, Error> {
        let mut entries = Vec::new();
        for version in self.query_row_versions(table, row_uuid)? {
            let tx_id = self.version_tx_id(&version)?;
            let tx = self
                .query_transaction(tx_id)?
                .ok_or(Error::MissingTransaction(tx_id))?;
            let local_current = self
                .query_local_layer_winner(table, row_uuid, version.layer())?
                .as_ref()
                .map(|winner| {
                    self.version_tx_id(winner)
                        .is_ok_and(|winner_tx| winner_tx == tx_id)
                })
                .unwrap_or(false);
            let global_current = self
                .query_global_layer_winner(table, row_uuid, version.layer())?
                .as_ref()
                .map(|winner| {
                    self.version_tx_id(winner)
                        .is_ok_and(|winner_tx| winner_tx == tx_id)
                })
                .unwrap_or(false);
            entries.push(version.to_history_entry(&tx, local_current, global_current));
        }
        entries.sort_by_key(|entry| entry.tx_id().time.sort_key(entry.tx_id().node));
        Ok(entries)
    }

    /// Consume the node and return the underlying groove database.
    pub fn into_database(self) -> Database<S> {
        self.database.into_inner()
    }

    /// Eagerly remove a Groove subscription from the runtime.
    pub(crate) fn unsubscribe_groove_subscription(
        &mut self,
        subscription_id: groove::ivm::SubscriptionId,
    ) -> bool {
        self.database.unsubscribe(subscription_id)
    }

    pub(crate) fn flush_query_runtime(&mut self) -> Result<(), Error> {
        self.database.flush().map_err(Error::Groove)
    }

    pub(crate) fn post_tick_consolidate_history_windows(
        &mut self,
        max_windows: usize,
    ) -> Result<WindowConsolidation, Error> {
        let report = self.database.consolidate_history_windows(
            groove::window_codec::TARGET_RECORDS_PER_WINDOW,
            max_windows,
        )?;
        if report.windows > 0 {
            self.query.tx_version_tables_cache.clear();
        }
        Ok(report)
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only hook for receipt runs that need to drive bounded
    /// post-tick maintenance to a fixed point without changing production tick
    /// cadence.
    pub fn consolidate_history_windows_for_test(
        &mut self,
        max_windows: usize,
    ) -> Result<WindowConsolidation, Error> {
        self.post_tick_consolidate_history_windows(max_windows)
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only history-class byte estimate. The underlying contract is
    /// cheap whole-class sizing, not logical-prefix accounting.
    pub fn history_class_bytes_for_test(&self) -> Result<Option<u64>, Error> {
        self.database
            .approximate_class_bytes("__groove_class_history")
            .map_err(Error::Groove)
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only estimate of all Jazz physical-class bytes. This is the
    /// cheap class-CF meter used for memory-amplification receipts; it is not a
    /// logical table-prefix scan.
    pub fn encoded_storage_bytes_for_test(&self) -> Result<u64, Error> {
        let mut total = 0_u64;
        for class_cf in [
            "__groove_class_history",
            "__groove_class_register",
            "__groove_class_global_current",
            "__groove_class_ahead_current",
            "__groove_class_changes",
            "__groove_class_indices",
            "__groove_class_content",
            "__groove_class_meta",
        ] {
            total += self
                .database
                .approximate_class_bytes(class_cf)
                .map_err(Error::Groove)?
                .unwrap_or_default();
        }
        Ok(total)
    }

    pub(crate) fn groove_runtime_token(&self) -> u64 {
        self.groove_runtime_token
    }

    /// Return metrics for the most recent committed storage batch, if any.
    pub fn last_commit_metrics(&self) -> Option<&CommitMetrics> {
        self.database.last_commit_metrics()
    }

    /// Return metrics for the most recent Groove runtime tick, if any.
    pub fn last_tick_metrics(&self) -> Option<&groove::ivm::TickMetrics> {
        self.database.last_tick_metrics()
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only runtime diagnostics used by performance receipts.
    pub fn runtime_stats_for_test(&self) -> groove::ivm::RuntimeStats {
        self.database.runtime_stats()
    }

    /// Return accumulated storage-read metrics since the last reset.
    pub fn storage_read_metrics(&self) -> groove::db::StorageReadMetrics {
        self.database.storage_read_metrics()
    }

    /// Reset accumulated storage-read metrics.
    pub fn reset_storage_read_metrics(&self) {
        self.database.reset_storage_read_metrics();
    }

    /// Return accumulated storage-read metrics and reset them.
    pub fn take_storage_read_metrics(&self) -> groove::db::StorageReadMetrics {
        self.database.take_storage_read_metrics()
    }

    fn persist_catalogue_schema(&mut self, schema: &SchemaVersion) -> Result<(), Error> {
        let mut batch = self.database.open_batch();
        batch.update(
            "jazz_catalogue",
            vec![
                Value::Bytes(b"schema".to_vec()),
                Value::Uuid(schema.id.0),
                Value::Bytes(serde_json::to_vec(schema)?),
            ],
        );
        self.database.commit_batch(batch)?;
        Ok(())
    }

    fn persist_catalogue_lens(&mut self, lens: &MigrationLens) -> Result<(), Error> {
        let mut batch = self.database.open_batch();
        batch.update(
            "jazz_catalogue",
            vec![
                Value::Bytes(b"lens".to_vec()),
                Value::Uuid(lens.id.0),
                Value::Bytes(serde_json::to_vec(lens)?),
            ],
        );
        self.database.commit_batch(batch)?;
        Ok(())
    }

    fn persist_catalogue_pointer(&mut self, pointer: CurrentWriteSchema) -> Result<(), Error> {
        let mut batch = self.database.open_batch();
        batch.update(
            "jazz_catalogue_pointer",
            vec![Value::U64(pointer.revision), Value::Uuid(pointer.schema.0)],
        );
        self.database.commit_batch(batch)?;
        Ok(())
    }

    fn persist_partition(
        &mut self,
        table: impl Into<String>,
        schema_version: SchemaVersionId,
    ) -> Result<bool, Error> {
        let table = table.into();
        if !self
            .catalogue
            .partitions
            .insert((table.clone(), schema_version))
        {
            return Ok(false);
        }
        self.query.version_storage_sources_cache.clear();
        let mut batch = self.database.open_batch();
        batch.update(
            "jazz_partitions",
            vec![
                Value::Bytes(table.as_bytes().to_vec()),
                Value::Uuid(schema_version.0),
            ],
        );
        self.database.commit_batch(batch)?;
        Ok(true)
    }

    fn ensure_node_alias(&mut self, node_uuid: NodeUuid) -> Result<NodeAlias, Error> {
        if node_uuid == self.node_uuid
            && let Some(alias) = self.self_node_alias
        {
            return Ok(alias);
        }
        if let Some(alias) = self.node_aliases.get(&node_uuid) {
            if node_uuid == self.node_uuid {
                self.self_node_alias = Some(*alias);
            }
            return Ok(*alias);
        }
        let mut max_alias = self
            .node_aliases
            .values()
            .map(|alias| alias.0)
            .max()
            .unwrap_or(0);
        for raw in self.database.primary_key_scan_raw("jazz_nodes", &[])? {
            let record = raw.record();
            let alias = NodeAlias(record.get_u64(NodeAliasRowRecord::FIELD_ID_IDX)?);
            max_alias = max_alias.max(alias.0);
            if record.get_uuid(NodeAliasRowRecord::FIELD_UUID_IDX)? == node_uuid.0 {
                self.node_aliases.insert(node_uuid, alias);
                if node_uuid == self.node_uuid {
                    self.self_node_alias = Some(alias);
                }
                return Ok(alias);
            }
        }
        let alias = NodeAlias(max_alias + 1);
        self.node_aliases.insert(node_uuid, alias);
        if node_uuid == self.node_uuid {
            self.self_node_alias = Some(alias);
        }
        let mut batch = self.database.open_batch();
        batch.insert(
            "jazz_nodes",
            vec![Value::U64(alias.0), Value::Uuid(node_uuid.0)],
        );
        self.database.commit_batch(batch)?;
        Ok(alias)
    }

    fn ensure_schema_version_alias(
        &mut self,
        schema_version_id: SchemaVersionId,
    ) -> Result<SchemaVersionAlias, Error> {
        if schema_version_id == self.catalogue.current_schema_version_id
            && let Some(alias) = self.catalogue.current_schema_version_alias
        {
            return Ok(alias);
        }
        if let Some(alias) = self
            .catalogue
            .schema_version_aliases
            .get(&schema_version_id)
        {
            if schema_version_id == self.catalogue.current_schema_version_id {
                self.catalogue.current_schema_version_alias = Some(*alias);
            }
            return Ok(*alias);
        }
        let mut max_alias = self
            .catalogue
            .schema_version_aliases
            .values()
            .map(|alias| alias.0)
            .max()
            .unwrap_or(0);
        for raw in self
            .database
            .primary_key_scan_raw("jazz_schema_versions", &[])?
        {
            let record = raw.record();
            let alias =
                SchemaVersionAlias(record.get_u64(SchemaVersionAliasRowRecord::FIELD_ID_IDX)?);
            max_alias = max_alias.max(alias.0);
            if record.get_uuid(SchemaVersionAliasRowRecord::FIELD_UUID_IDX)? == schema_version_id.0
            {
                self.catalogue
                    .schema_version_aliases
                    .insert(schema_version_id, alias);
                if schema_version_id == self.catalogue.current_schema_version_id {
                    self.catalogue.current_schema_version_alias = Some(alias);
                }
                return Ok(alias);
            }
        }
        let alias = SchemaVersionAlias(max_alias + 1);
        self.catalogue
            .schema_version_aliases
            .insert(schema_version_id, alias);
        if schema_version_id == self.catalogue.current_schema_version_id {
            self.catalogue.current_schema_version_alias = Some(alias);
        }
        let mut batch = self.database.open_batch();
        batch.insert(
            "jazz_schema_versions",
            vec![Value::U64(alias.0), Value::Uuid(schema_version_id.0)],
        );
        self.database.commit_batch(batch)?;
        Ok(alias)
    }

    pub(super) fn schema_version_for_alias(
        &self,
        alias: SchemaVersionAlias,
    ) -> Option<SchemaVersionId> {
        self.catalogue
            .schema_version_aliases
            .iter()
            .find_map(|(id, candidate)| (*candidate == alias).then_some(*id))
    }

    fn record_child_edges(&mut self, child: TxId, parents: impl IntoIterator<Item = TxId>) {
        if self
            .query_transaction(child)
            .ok()
            .flatten()
            .is_some_and(|tx| !matches!(tx.fate, Fate::Pending))
        {
            return;
        }
        for parent in parents {
            if self
                .query_transaction(parent)
                .ok()
                .flatten()
                .is_some_and(|tx| !matches!(tx.fate, Fate::Pending))
            {
                continue;
            }
            self.rejections
                .child_txs_by_parent
                .entry(parent)
                .or_default()
                .insert(child);
        }
    }

    fn prune_child_edges(&mut self, child: TxId) {
        self.rejections.child_txs_by_parent.retain(|_, children| {
            children.remove(&child);
            !children.is_empty()
        });
    }

    pub(crate) fn table(&self, table: &str) -> Result<&TableSchema, Error> {
        self.catalogue
            .schema
            .tables
            .iter()
            .find(|candidate| candidate.name == table)
            .ok_or_else(|| Error::TableNotFound(table.to_owned()))
    }

    pub(super) fn table_in_schema(
        &self,
        table: &str,
        schema_version: SchemaVersionId,
    ) -> Result<TableSchema, Error> {
        if schema_version == self.catalogue.current_schema_version_id {
            return self.table(table).cloned();
        }
        self.catalogue
            .catalogue_schemas
            .get(&schema_version)
            .and_then(|schema| {
                schema
                    .schema
                    .tables
                    .iter()
                    .find(|candidate| candidate.name == table)
                    .cloned()
            })
            .ok_or_else(|| Error::TableNotFound(table.to_owned()))
    }

    pub(super) fn shortest_lens_path_ids_cached(
        &mut self,
        source: SchemaVersionId,
        target: SchemaVersionId,
        direction: LensPathDirection,
    ) -> Option<Vec<MigrationLensId>> {
        let key = LensPathCacheKey {
            source,
            target,
            direction,
        };
        if let Some(path) = self.catalogue.lens_path_cache.get(&key) {
            return path.clone();
        }
        let path = self.shortest_lens_path_ids(source, target, direction);
        self.catalogue.lens_path_cache.insert(key, path.clone());
        path
    }

    fn shortest_lens_path_ids(
        &self,
        source: SchemaVersionId,
        target: SchemaVersionId,
        direction: LensPathDirection,
    ) -> Option<Vec<MigrationLensId>> {
        if source == target {
            return Some(Vec::new());
        }

        let mut seen = BTreeSet::from([source]);
        let mut queue = VecDeque::from([(source, Vec::<MigrationLensId>::new())]);
        while let Some((schema, path)) = queue.pop_front() {
            for lens in self.ordered_lens_edges(schema, direction) {
                let next = match direction {
                    LensPathDirection::Forward => lens.target,
                    LensPathDirection::Reverse => lens.source,
                };
                if seen.contains(&next) {
                    continue;
                }
                let mut next_path = path.clone();
                next_path.push(lens.id);
                if next == target {
                    return Some(next_path);
                }
                seen.insert(next);
                queue.push_back((next, next_path));
            }
        }
        None
    }

    pub(super) fn compiled_lens_path(
        &mut self,
        source: SchemaVersionId,
        target: SchemaVersionId,
        direction: LensPathDirection,
        table: &str,
    ) -> Result<Option<CompiledLensPath>, Error> {
        let key = CompiledLensCacheKey {
            source,
            target,
            direction,
            table: table.to_owned(),
        };
        if let Some(path) = self.catalogue.compiled_lens_cache.get(&key) {
            return Ok(path.clone());
        }

        let Some(lens_ids) = self.shortest_lens_path_ids_cached(source, target, direction) else {
            self.catalogue.compiled_lens_cache.insert(key, None);
            return Ok(None);
        };
        let mut current_table = table.to_owned();
        let mut ops = Vec::new();
        for lens_id in lens_ids {
            let lens = self
                .catalogue
                .catalogue_lenses
                .get(&lens_id)
                .ok_or(Error::InvalidCatalogueUpdate("lens chain is unknown"))?;
            let table_lens = match direction {
                LensPathDirection::Forward => lens
                    .table_lenses
                    .iter()
                    .find(|candidate| candidate.source_table == current_table),
                LensPathDirection::Reverse => lens
                    .table_lenses
                    .iter()
                    .find(|candidate| candidate.target_table == current_table),
            }
            .ok_or(Error::InvalidCatalogueUpdate("table lens is unknown"))?;
            match direction {
                LensPathDirection::Forward => {
                    for op in &table_lens.ops {
                        push_compiled_forward_lens_op(op, &mut ops)?;
                    }
                    current_table = table_lens.target_table.clone();
                }
                LensPathDirection::Reverse => {
                    for op in table_lens.ops.iter().rev() {
                        push_compiled_reverse_lens_op(op, &mut ops)?;
                    }
                    current_table = table_lens.source_table.clone();
                }
            }
        }
        let path = Some(CompiledLensPath {
            target_table: current_table,
            ops,
        });
        self.catalogue.compiled_lens_cache.insert(key, path.clone());
        Ok(path)
    }

    fn ordered_lens_edges(
        &self,
        schema: SchemaVersionId,
        direction: LensPathDirection,
    ) -> Vec<&MigrationLens> {
        let mut edges = self
            .catalogue
            .catalogue_lenses
            .values()
            .filter(|lens| match direction {
                LensPathDirection::Forward => lens.source == schema,
                LensPathDirection::Reverse => lens.target == schema,
            })
            .collect::<Vec<_>>();
        edges.sort_by(|left, right| {
            let left_next = match direction {
                LensPathDirection::Forward => left.target,
                LensPathDirection::Reverse => left.source,
            };
            let right_next = match direction {
                LensPathDirection::Forward => right.target,
                LensPathDirection::Reverse => right.source,
            };
            left_next
                .cmp(&right_next)
                .then_with(|| left.id.cmp(&right.id))
        });
        edges
    }

    fn node_for_alias(&self, alias: NodeAlias) -> Option<NodeUuid> {
        self.node_aliases
            .iter()
            .find_map(|(node, candidate)| (*candidate == alias).then_some(*node))
    }

    pub(super) fn resolve_node_alias(
        &mut self,
        alias: NodeAlias,
    ) -> Result<Option<NodeUuid>, Error> {
        if let Some(node) = self.node_for_alias(alias) {
            return Ok(Some(node));
        }
        for raw in self.database.primary_key_scan_raw("jazz_nodes", &[])? {
            let record = raw.record();
            if NodeAlias(record.get_u64(NodeAliasRowRecord::FIELD_ID_IDX)?) != alias {
                continue;
            }
            let node = NodeUuid(record.get_uuid(NodeAliasRowRecord::FIELD_UUID_IDX)?);
            self.node_aliases.insert(node, alias);
            if node == self.node_uuid {
                self.self_node_alias = Some(alias);
            }
            return Ok(Some(node));
        }
        Ok(None)
    }

    pub(super) fn version_tx_id(&self, version: &VersionRow) -> Result<TxId, Error> {
        let node =
            self.node_for_alias(version.tx_node_alias())
                .ok_or(Error::InvalidStoredValue(
                    "history tx node alias must exist",
                ))?;
        Ok(TxId::new(version.tx_time(), node))
    }

    fn version_made_at(&mut self, version: &VersionRow) -> Result<TxTime, Error> {
        let tx_id = self.version_tx_id(version)?;
        self.transaction_made_at(tx_id)?
            .ok_or(Error::MissingTransaction(tx_id))
    }

    fn version_record_from_row(&self, version: &VersionRow) -> Result<VersionRecord, Error> {
        let schema_version = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue(
                "history schema version alias must exist",
            ))?;
        let table = self.table_in_schema(version.table(), schema_version)?;
        VersionRecord::from_stored(version, &table, schema_version)
    }

    pub(crate) fn row_version_payloads_for_refs(
        &mut self,
        requests: &[RowVersionRef],
        identity: AuthorId,
    ) -> Result<Vec<VersionBundle>, Error> {
        let mut by_tx = BTreeMap::<TxId, Vec<VersionRow>>::new();
        for request in requests {
            if !self.dry_run_read_current_allows(&request.table, request.row_uuid, identity)? {
                continue;
            }
            let tx_id = request.tx_id();
            for version in self.query_versions_for_tx(tx_id)? {
                if version.table() == request.table.as_str()
                    && version.row_uuid() == request.row_uuid
                    && version.tx_time() == request.tx_time
                    && self.node_for_alias(version.tx_node_alias()) == Some(request.tx_node_id)
                {
                    by_tx.entry(tx_id).or_default().push(version);
                    break;
                }
            }
        }
        let mut out = Vec::new();
        for (tx_id, versions) in by_tx {
            let stored = self
                .query_transaction(tx_id)?
                .ok_or(Error::MissingTransaction(tx_id))?;
            out.push(self.version_bundle_for_maintained_view_versions_with_tx(&stored, &versions)?);
        }
        Ok(out)
    }

    #[allow(dead_code)]
    pub(crate) fn apply_row_version_payloads_for_requests(
        &mut self,
        requests: &[RowVersionRef],
        version_bundles: Vec<VersionBundle>,
    ) -> Result<(), Error> {
        let request_set = requests.iter().cloned().collect::<BTreeSet<_>>();
        for bundle in version_bundles {
            let versions = bundle
                .versions
                .into_iter()
                .filter(|version| {
                    request_set.contains(&RowVersionRef::new(
                        version.table().to_owned(),
                        version.row_uuid(),
                        bundle.tx.tx_id,
                    ))
                })
                .collect::<Vec<_>>();
            if versions.is_empty() {
                continue;
            }
            self.ingest_known_transaction(
                bundle.tx,
                versions,
                bundle.fate,
                bundle.global_seq,
                bundle.durability,
            )?;
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn missing_known_state_row_version_refs(
        &mut self,
        message: &SyncMessage,
    ) -> Result<Vec<RowVersionRef>, Error> {
        let (result_member_adds, version_carriers, version_bundles, program_fact_adds) =
            match message {
                SyncMessage::ViewUpdate {
                    result_member_adds,
                    version_carriers,
                    version_bundles,
                    program_fact_adds,
                    ..
                }
                | SyncMessage::ViewUpdateChunk {
                    result_member_adds,
                    version_carriers,
                    version_bundles,
                    program_fact_adds,
                    ..
                } => (
                    result_member_adds,
                    version_carriers,
                    version_bundles,
                    program_fact_adds,
                ),
                _ => return Ok(Vec::new()),
            };
        let mut normalized_bundles = version_bundles.clone();
        normalized_bundles.extend(
            expand_version_carriers(version_carriers)
                .map_err(|_| Error::UnsupportedSyncMessage("malformed version-bundle run"))?,
        );
        let incoming = normalized_bundles
            .iter()
            .flat_map(|bundle| {
                bundle.versions.iter().map(|version| {
                    RowVersionRef::new(
                        version.table().to_owned(),
                        version.row_uuid(),
                        bundle.tx.tx_id,
                    )
                })
            })
            .collect::<BTreeSet<_>>();
        let mut missing = BTreeSet::new();
        let mut visited_text_ancestors = BTreeSet::new();
        for bundle in &normalized_bundles {
            for version in &bundle.versions {
                self.collect_missing_text_ancestor_refs(
                    version,
                    &mut missing,
                    &mut visited_text_ancestors,
                )?;
            }
        }
        // Only additions require repair. Removals are self-sufficient because
        // the removed version may now be policy-invisible to this receiver, in
        // which case fetching the body is both unnecessary and allowed to
        // return no payload.
        for (table, row_uuid, tx_id) in result_member_adds
            .iter()
            .filter_map(ResultMemberEntry::as_row)
        {
            let version_ref = RowVersionRef::new(table.to_string(), row_uuid, tx_id);
            if incoming.contains(&version_ref) {
                continue;
            }
            let has_body = self.local_version_row_for_ref(&version_ref)?.is_some()
                && self.query_transaction(tx_id)?.is_some();
            if !has_body {
                missing.insert(version_ref);
            } else if let Some(version) = self.local_version_record_for_ref(&version_ref)? {
                self.collect_missing_text_ancestor_refs(
                    &version,
                    &mut missing,
                    &mut visited_text_ancestors,
                )?;
            }
        }
        for (table, row_uuid, tx_id) in program_fact_adds
            .iter()
            .filter_map(|fact| match fact {
                ProgramFactEntry::RelationEdge(edge) => Some(edge),
                _ => None,
            })
            .flat_map(|edge| {
                [
                    edge.source_version.as_ref().map(|version| {
                        (edge.source_table.to_string(), edge.source_row, version.tx)
                    }),
                    edge.target_version.as_ref().map(|version| {
                        (edge.target_table.to_string(), edge.target_row, version.tx)
                    }),
                ]
            })
            .flatten()
        {
            let version_ref = RowVersionRef::new(table, row_uuid, tx_id);
            if incoming.contains(&version_ref) {
                continue;
            }
            let has_body = self.local_version_row_for_ref(&version_ref)?.is_some()
                && self.query_transaction(tx_id)?.is_some();
            if !has_body {
                missing.insert(version_ref);
            } else if let Some(version) = self.local_version_record_for_ref(&version_ref)? {
                self.collect_missing_text_ancestor_refs(
                    &version,
                    &mut missing,
                    &mut visited_text_ancestors,
                )?;
            }
        }
        Ok(missing.into_iter().collect())
    }

    fn collect_missing_text_ancestor_refs(
        &mut self,
        version: &VersionRecord,
        missing: &mut BTreeSet<RowVersionRef>,
        visited: &mut BTreeSet<RowVersionRef>,
    ) -> Result<(), Error> {
        if !self.version_record_has_text_cell(version)? {
            return Ok(());
        }
        for parent in version.parents() {
            let parent_ref =
                RowVersionRef::new(version.table().to_owned(), version.row_uuid(), parent);
            if !visited.insert(parent_ref.clone()) {
                continue;
            }
            if self.query_transaction(parent)?.is_none() {
                missing.insert(parent_ref);
                continue;
            }
            let Some(parent_version) = self.local_version_record_for_ref(&parent_ref)? else {
                missing.insert(parent_ref);
                continue;
            };
            self.collect_missing_text_ancestor_refs(&parent_version, missing, visited)?;
        }
        Ok(())
    }

    fn local_version_record_for_ref(
        &mut self,
        version_ref: &RowVersionRef,
    ) -> Result<Option<VersionRecord>, Error> {
        let Some(version) = self.local_version_row_for_ref(version_ref)? else {
            return Ok(None);
        };
        self.version_record_from_row(&version).map(Some)
    }

    fn local_version_row_for_ref(
        &mut self,
        version_ref: &RowVersionRef,
    ) -> Result<Option<VersionRow>, Error> {
        let Some(tx_node_alias) = self.node_aliases.get(&version_ref.tx_node_id).copied() else {
            return Ok(None);
        };
        for layer in [VersionLayer::Content, VersionLayer::Deletion] {
            if let Some(version) = self.query_version_by_alias(
                &version_ref.table,
                version_ref.row_uuid,
                layer,
                version_ref.tx_time,
                tx_node_alias,
            )? {
                return Ok(Some(version));
            }
        }
        Ok(None)
    }

    fn version_record_has_text_cell(&self, version: &VersionRecord) -> Result<bool, Error> {
        let table = self.table_in_schema(version.table(), version.schema_version())?;
        Ok(table.columns.iter().enumerate().any(|(position, column)| {
            column.large_value == Some(LargeValueKind::Text)
                && version.optional_cell_at(position).is_some()
        }))
    }

    fn mint_tx_time(&mut self, now_ms: u64) -> TxTime {
        let made_at = TxTime::tick(self.clock.tx_time, now_ms);
        self.clock.tx_time = made_at;
        made_at
    }

    fn merge_tx_time(&mut self, observed: TxTime) {
        self.clock.tx_time = self.clock.tx_time.max(observed);
    }
}

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
        nullable_value(
            self.record
                .borrowed()
                .get_idx(CurrentRowRecord::USER_CELLS + column_position)
                .expect("valid current user cell"),
        )
        .expect("valid nullable current user cell")
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
        nullable_value(self.record.borrowed().get_idx(idx).ok()?).ok()?
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
                        records::ValueType::Nullable(Box::new(
                            column.column_type.clone().value_type(),
                        )),
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
            values.push(Value::Nullable(
                self.cell(table, &column.name).map(Box::new),
            ));
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
                if !matches!(field.value_type, records::ValueType::Nullable(_)) {
                    return None;
                }
                let name = field.name.as_ref()?.as_str();
                let name = self::query_engine::logical_user_column(name).to_owned();
                let value = nullable_value(self.record.borrowed().get_idx(idx).ok()?).ok()??;
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
    S: OrderedKvStorage,
{
    node: &'node mut NodeState<S>,
    position: GlobalSeq,
}

impl<S> HistoricalRead<'_, S>
where
    S: OrderedKvStorage,
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
    /// Rung-3 text strategies that degraded to the builtin char-walk merge.
    pub rung3_text_merge_fallbacks: u64,
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

/// Deterministic counters for large-value materialization and checkpoint use.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LargeValueMetrics {
    /// Number of large-value materializations performed.
    pub materializations: u64,
    /// Total decoded edit operations replayed across materializations.
    pub total_replayed_ops: u64,
    /// Edit operations replayed by the most recent materialization.
    pub last_replayed_ops: usize,
    /// Version rows replayed by the most recent materialization.
    pub last_replayed_versions: usize,
    /// Materializations that found and used a local checkpoint.
    pub checkpoint_hits: u64,
    /// Local checkpoints written by materialization.
    pub checkpoint_writes: u64,
}

/// Handle for an open exclusive transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OpenTxId(u64);

/// Explicit edit operation for one text/blob column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LargeValueEditOp {
    /// Insert bytes at a parent-relative byte offset.
    Insert(usize, Vec<u8>),
    /// Delete bytes from a parent-relative byte range.
    Delete(usize, usize),
}

/// Builder for a local mergeable text/blob edit commit.
#[derive(Clone, Debug)]
pub struct LargeValueEditCommit {
    /// Target table.
    pub table: String,
    /// Target row.
    pub row_uuid: RowUuid,
    /// Target text/blob column.
    pub column: String,
    /// Author making the commit.
    pub made_by: AuthorId,
    /// Abstract wall clock at the committing node.
    pub now_ms: u64,
    /// Explicit edit operations.
    pub ops: Vec<LargeValueEditOp>,
    /// Optional application metadata.
    pub user_metadata_json: Option<String>,
}

impl LargeValueEditCommit {
    /// Construct an empty explicit edit commit builder.
    pub fn new(
        table: impl Into<String>,
        row_uuid: RowUuid,
        column: impl Into<String>,
        now_ms: u64,
    ) -> Self {
        Self {
            table: table.into(),
            row_uuid,
            column: column.into(),
            made_by: AuthorId::SYSTEM,
            now_ms,
            ops: Vec::new(),
            user_metadata_json: None,
        }
    }

    /// Set the commit author.
    pub fn made_by(mut self, made_by: AuthorId) -> Self {
        self.made_by = made_by;
        self
    }

    /// Append an insert operation.
    pub fn insert(mut self, pos: usize, bytes: impl Into<Vec<u8>>) -> Self {
        self.ops.push(LargeValueEditOp::Insert(pos, bytes.into()));
        self
    }

    /// Append a delete operation.
    pub fn delete(mut self, pos: usize, len: usize) -> Self {
        self.ops.push(LargeValueEditOp::Delete(pos, len));
        self
    }

    /// Replace the edit operations.
    pub fn ops(mut self, ops: Vec<LargeValueEditOp>) -> Self {
        self.ops = ops;
        self
    }

    /// Attach application metadata.
    pub fn user_metadata(mut self, json: String) -> Self {
        self.user_metadata_json = Some(json);
        self
    }

    fn validate(&self) -> Result<(), Error> {
        if self.ops.is_empty() {
            return Err(Error::InvalidMergeableCommit(
                "large-value edit requires at least one operation",
            ));
        }
        Ok(())
    }
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
    /// Deletion-register event, if any.
    pub deletion: Option<DeletionEvent>,
    /// Parent content versions.
    pub parents: Vec<TxId>,
    /// Optional application metadata.
    pub user_metadata_json: Option<String>,
    /// Recorded merge strategy for system-created merge versions.
    pub merge_strategy: Option<RecordedMergeStrategy>,
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
            deletion: None,
            parents: Vec::new(),
            user_metadata_json: None,
            merge_strategy: None,
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

    pub(crate) fn merge_strategy(mut self, strategy: RecordedMergeStrategy) -> Self {
        self.merge_strategy = Some(strategy);
        self
    }

    fn validate(&self) -> Result<(), Error> {
        validate_mergeable_write_shape(self.cells.is_empty(), self.deletion.is_some())
    }
}

pub(crate) struct ViewUpdateParts {
    pub(crate) subscription: SubscriptionKey,
    pub(crate) settled_through: GlobalSeq,
    pub(crate) defer_settlement: bool,
    pub(crate) reset_result_set: bool,
    pub(crate) version_carriers: Vec<VersionCarrier>,
    pub(crate) version_bundles: Vec<VersionBundle>,
    pub(crate) peer_complete_tx_payload_refs: Vec<TxId>,
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

struct CatalogueOpenState<S> {
    storage: S,
    schemas: BTreeMap<SchemaVersionId, SchemaVersion>,
    lenses: BTreeMap<MigrationLensId, MigrationLens>,
    current_write_schema: CurrentWriteSchema,
    partitions: BTreeSet<(String, SchemaVersionId)>,
    branch_partitions: BTreeSet<(String, SchemaVersionId, BranchId)>,
}

struct DatabaseSlot<S> {
    database: Option<Database<S>>,
}

impl<S> DatabaseSlot<S> {
    fn new(database: Database<S>) -> Self {
        Self {
            database: Some(database),
        }
    }

    fn take(&mut self) -> Database<S> {
        self.database
            .take()
            .expect("node database slot must be populated outside rebuild")
    }

    fn replace(&mut self, database: Database<S>) {
        debug_assert!(self.database.is_none());
        self.database = Some(database);
    }

    fn into_inner(mut self) -> Database<S> {
        self.take()
    }
}

impl<S> Deref for DatabaseSlot<S> {
    type Target = Database<S>;

    fn deref(&self) -> &Self::Target {
        self.database
            .as_ref()
            .expect("node database slot must be populated")
    }
}

impl<S> DerefMut for DatabaseSlot<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.database
            .as_mut()
            .expect("node database slot must be populated")
    }
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

fn validate_large_value_edit_ranges(
    parent_len: usize,
    ops: &[LargeValueEditOp],
) -> Result<(), Error> {
    let text_ops = large_value_edit_ops_to_legacy_text_ops(ops.to_vec());
    validate_text_edit_ranges(parent_len, &text_ops)
}

fn large_value_edit_ops_to_legacy_text_ops(ops: Vec<LargeValueEditOp>) -> Vec<TextOp> {
    ops.into_iter()
        .map(|op| match op {
            LargeValueEditOp::Insert(pos, bytes) => TextOp::Insert {
                pos,
                content: TextContent::Inline(bytes),
            },
            LargeValueEditOp::Delete(pos, len) => TextOp::Delete { pos, len },
        })
        .collect()
}

fn encode_extent_text_ops(ops: &[TextOp]) -> Vec<u8> {
    let mut bytes = Vec::from(TEXT_EXTENT_OPS_MAGIC);
    bytes.extend(text_oplog::encode(ops));
    bytes
}

fn encode_large_value_handle(
    table: &TableSchema,
    row_uuid: RowUuid,
    column: &str,
    tx_id: TxId,
    kind: LargeValueKind,
    len: usize,
    refs: Vec<content_store::Extent>,
) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::from(LARGE_VALUE_HANDLE_MAGIC);
    write_handle_string(&mut bytes, &table.name)?;
    bytes.extend_from_slice(row_uuid.as_bytes());
    write_handle_string(&mut bytes, column)?;
    bytes.extend_from_slice(&tx_id.time.0.to_be_bytes());
    bytes.extend_from_slice(tx_id.node.as_bytes());
    bytes.push(match kind {
        LargeValueKind::Text => 1,
        LargeValueKind::Blob => 2,
    });
    bytes.extend_from_slice(
        &u64::try_from(len)
            .map_err(|_| Error::InvalidStoredValue("large-value handle length exceeds u64"))?
            .to_be_bytes(),
    );
    bytes.extend_from_slice(
        &u64::try_from(refs.len())
            .map_err(|_| Error::InvalidStoredValue("large-value handle refs exceed u64"))?
            .to_be_bytes(),
    );
    for extent in refs {
        bytes.extend_from_slice(extent.writer.as_bytes());
        bytes.extend_from_slice(extent.row.as_bytes());
        let column = extent.column.as_bytes();
        bytes.extend_from_slice(
            &u32::try_from(column.len())
                .map_err(|_| Error::InvalidStoredValue("large-value handle column too long"))?
                .to_be_bytes(),
        );
        bytes.extend_from_slice(column);
        bytes.extend_from_slice(&extent.offset.to_be_bytes());
        bytes.extend_from_slice(&extent.len.to_be_bytes());
    }
    Ok(bytes)
}

fn write_handle_string(bytes: &mut Vec<u8>, value: &str) -> Result<(), Error> {
    bytes.extend_from_slice(
        &u32::try_from(value.len())
            .map_err(|_| Error::InvalidStoredValue("large-value handle string too long"))?
            .to_be_bytes(),
    );
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}

fn content_refs_in_text_ops(ops: Vec<TextOp>) -> Vec<content_store::Extent> {
    ops.into_iter()
        .filter_map(|op| match op {
            TextOp::Insert {
                content: TextContent::Ref(extent),
                ..
            } => Some(extent),
            TextOp::Insert { .. } | TextOp::Delete { .. } => None,
        })
        .collect()
}

struct DecodedLargeValueHandle {
    table: String,
    row_uuid: RowUuid,
    column: String,
    tx_id: TxId,
    kind: LargeValueKind,
}

fn decode_large_value_handle(bytes: &[u8]) -> Result<DecodedLargeValueHandle, Error> {
    let mut cursor = HandleCursor::new(
        bytes
            .strip_prefix(LARGE_VALUE_HANDLE_MAGIC)
            .ok_or(Error::InvalidStoredValue("invalid large-value handle"))?,
    );
    let table = cursor.read_string()?;
    let row_uuid = RowUuid(uuid::Uuid::from_bytes(cursor.read_array()?));
    let column = cursor.read_string()?;
    let tx_time = TxTime(cursor.read_u64()?);
    let tx_node = NodeUuid(uuid::Uuid::from_bytes(cursor.read_array()?));
    let kind = match cursor.read_u8()? {
        1 => LargeValueKind::Text,
        2 => LargeValueKind::Blob,
        _ => return Err(Error::InvalidStoredValue("invalid large-value handle kind")),
    };
    let _len = cursor.read_u64()?;
    let refs = cursor.read_u64()?;
    for _ in 0..refs {
        let _writer = AuthorId(uuid::Uuid::from_bytes(cursor.read_array()?));
        let _row = RowUuid(uuid::Uuid::from_bytes(cursor.read_array()?));
        let _column = cursor.read_string()?;
        let _offset = cursor.read_u64()?;
        let _len = cursor.read_u64()?;
    }
    if !cursor.is_empty() {
        return Err(Error::InvalidStoredValue(
            "trailing large-value handle bytes",
        ));
    }
    Ok(DecodedLargeValueHandle {
        table,
        row_uuid,
        column,
        tx_id: TxId::new(tx_time, tx_node),
        kind,
    })
}

struct HandleCursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> HandleCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn is_empty(&self) -> bool {
        self.pos == self.bytes.len()
    }

    fn read_u8(&mut self) -> Result<u8, Error> {
        let value = *self
            .bytes
            .get(self.pos)
            .ok_or(Error::InvalidStoredValue("truncated large-value handle"))?;
        self.pos += 1;
        Ok(value)
    }

    fn read_u32(&mut self) -> Result<u32, Error> {
        Ok(u32::from_be_bytes(self.read_array()?))
    }

    fn read_u64(&mut self) -> Result<u64, Error> {
        Ok(u64::from_be_bytes(self.read_array()?))
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], Error> {
        self.read_bytes(N)?
            .try_into()
            .map_err(|_| Error::InvalidStoredValue("invalid large-value handle bytes"))
    }

    fn read_string(&mut self) -> Result<String, Error> {
        let len = usize::try_from(self.read_u32()?)
            .map_err(|_| Error::InvalidStoredValue("large-value handle string too long"))?;
        String::from_utf8(self.read_bytes(len)?.to_vec())
            .map_err(|_| Error::InvalidStoredValue("large-value handle string is not utf-8"))
    }

    fn read_bytes(&mut self, len: usize) -> Result<&'a [u8], Error> {
        let end = self.pos.checked_add(len).ok_or(Error::InvalidStoredValue(
            "large-value handle length overflow",
        ))?;
        let bytes = self
            .bytes
            .get(self.pos..end)
            .ok_or(Error::InvalidStoredValue("truncated large-value handle"))?;
        self.pos = end;
        Ok(bytes)
    }
}

fn decode_plain_text_op(payload: &[u8]) -> Result<PlainTextOp, Error> {
    crate::text_merge::decode(payload)
        .map_err(|_| Error::InvalidStoredValue("text op payload failed to decode"))
}

fn column_large_value_kind(table: &TableSchema, column: &str) -> Result<LargeValueKind, Error> {
    table
        .columns
        .iter()
        .find(|candidate| candidate.name == column)
        .and_then(|column| column.large_value)
        .ok_or(Error::InvalidStoredValue("large value column kind missing"))
}

fn validate_text_edit_ranges(parent_len: usize, ops: &[TextOp]) -> Result<(), Error> {
    let mut value_len = parent_len;
    for (op_index, op) in ops.iter().enumerate() {
        let pos = match op {
            TextOp::Insert { pos, .. } | TextOp::Delete { pos, .. } => *pos,
        };
        let adjusted_pos = adjusted_text_edit_pos(pos, &ops[..op_index], value_len)?;
        match op {
            TextOp::Insert { content, .. } => {
                if adjusted_pos > value_len {
                    return Err(Error::InvalidMergeableCommit(
                        "large-value insert position is out of bounds",
                    ));
                }
                value_len = value_len.checked_add(text_content_len(content)?).ok_or(
                    Error::InvalidMergeableCommit("large-value edit length overflows"),
                )?;
            }
            TextOp::Delete { len, .. } => {
                let end = adjusted_pos
                    .checked_add(*len)
                    .ok_or(Error::InvalidMergeableCommit(
                        "large-value delete range overflows",
                    ))?;
                if end > value_len {
                    return Err(Error::InvalidMergeableCommit(
                        "large-value delete range is out of bounds",
                    ));
                }
                value_len -= len;
            }
        }
    }
    Ok(())
}

fn adjusted_text_edit_pos(
    pos: usize,
    prior_ops: &[TextOp],
    value_len: usize,
) -> Result<usize, Error> {
    let mut adjusted = pos;
    for prior_op in prior_ops {
        match prior_op {
            TextOp::Insert {
                pos: prior_pos,
                content,
            } => {
                if *prior_pos <= pos {
                    adjusted = adjusted.checked_add(text_content_len(content)?).ok_or(
                        Error::InvalidMergeableCommit("large-value edit position overflows"),
                    )?;
                }
            }
            TextOp::Delete {
                pos: prior_pos,
                len,
            } => {
                if *prior_pos < pos {
                    let deleted_before_pos = (*len).min(pos - prior_pos);
                    adjusted = adjusted.checked_sub(deleted_before_pos).ok_or(
                        Error::InvalidMergeableCommit("large-value edit position underflows"),
                    )?;
                }
            }
        }
    }
    if adjusted > value_len {
        return Err(Error::InvalidMergeableCommit(
            "large-value edit position is out of bounds",
        ));
    }
    Ok(adjusted)
}

fn text_content_len(content: &TextContent) -> Result<usize, Error> {
    match content {
        TextContent::Inline(bytes) => Ok(bytes.len()),
        TextContent::Ref(extent) => usize::try_from(extent.len).map_err(|_| {
            Error::InvalidMergeableCommit("large-value edit content length exceeds usize")
        }),
    }
}

fn large_value_cache_key(
    table: &TableSchema,
    row_uuid: RowUuid,
    column: &str,
    tx_id: TxId,
) -> LargeValueCacheKey {
    (table.name.clone(), row_uuid, column.to_owned(), tx_id)
}

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

fn binding_view_key_from_store_key(
    key: &[Value],
    context: &'static str,
) -> Result<BindingViewKey, Error> {
    if key.len() < 3 {
        return Err(Error::InvalidStoredValue(context));
    }
    let shape_id = match &key[0] {
        Value::Uuid(uuid) => ShapeId(*uuid),
        _ => return Err(Error::InvalidStoredValue(context)),
    };
    let binding_id = match &key[1] {
        Value::Uuid(uuid) => BindingId(*uuid),
        _ => return Err(Error::InvalidStoredValue(context)),
    };
    let read_view = match &key[2] {
        Value::Uuid(uuid) => ReadViewKey { id: *uuid },
        _ => return Err(Error::InvalidStoredValue(context)),
    };
    Ok(BindingViewKey::new(shape_id, binding_id, read_view))
}

fn clean_close_marker_key() -> [Value; 1] {
    [Value::String(CLEAN_CLOSE_MARKER_NAME.to_owned())]
}

fn storage_consistency_marker_key() -> [Value; 1] {
    [Value::String(STORAGE_CONSISTENCY_MARKER_NAME.to_owned())]
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
    /// Error returned by storage.
    #[error(transparent)]
    Storage(#[from] storage::Error),
    /// Error returned by query validation or binding.
    #[error(transparent)]
    Query(#[from] QueryError),
    /// Query could not be represented by the unified query engine.
    #[error("query lowering failed: {0}")]
    QueryLowering(String),
    /// Query-engine capability report for a currently unsupported program.
    #[error("query capability unsupported: {0}")]
    QueryCapability(String),
    /// Table was not found in the schema.
    #[error("table not found: {0}")]
    TableNotFound(String),
    /// Column type is not supported by Jazz v0.
    #[error("M1 only supports string user columns, got unsupported column: {0}")]
    UnsupportedColumnType(String),
    /// Mergeable commit shape is invalid.
    #[error("invalid mergeable commit: {0}")]
    InvalidMergeableCommit(&'static str),
    /// Stored value failed validation.
    #[error("invalid stored value: {0}")]
    InvalidStoredValue(&'static str),
    /// Content extent bytes are referenced by stored history but not hydrated locally.
    #[error("missing content extent: {0:?}")]
    MissingContentExtent(content_store::Extent),
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
    MissingOpenTx(OpenTxId),
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
    /// The authenticated identity is not authorized for this operation.
    #[error("authorization denied")]
    AuthorizationDenied,
    /// A prepared point-read subscription closed before its initial snapshot.
    #[error("prepared point-read subscription closed")]
    SubscriptionClosed,
}
