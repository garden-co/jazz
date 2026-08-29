//! Synchronous IVM graph runtime and tick-loop narrative.
//!
//! This module owns executable state for hash-consed graphs: subscriptions,
//! prepared-shape bindings, durable index nodes, per-operator state, reusable
//! join arrangements, recursive state, and per-tick memoization. The reading
//! order is tick-loop first: start at [`IvmRuntime::tick_with_params`], then
//! follow [`TickEvaluator::update_node`] to operator evaluation. Subscription
//! setup, graph insertion, retainers, and GC live after that narrative. Query
//! lowering lives in [`crate::ivm::planner`], graph identity in
//! [`crate::ivm::graph`], and storage mechanics in [`crate::storage`].

use bytes::{Bytes, BytesMut};
use std::cell::{Cell, RefCell};
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::{
    Arc, Weak,
    mpsc::{self, Receiver, RecvError, Sender, TryRecvError},
};

use rustc_hash::FxHashMap as HashMap;

use crate::ivm::{
    AggregateExpr, AggregateFunction, AggregateOp, ArgMaxByOp, ArgMinByOp, ArrangeOp,
    ArrangementDescriptor, BindingSourceOp, CollectByBuilder, CollectByField, CollectByMode,
    CollectByOp, CollectByProjection, CollectBySlot, CollectBySlotBuilder, DurableStorage,
    FieldRef, FilterOp, FrontierName, FrontierSourceOp, GraphBuilder, IndexByOp, IndexSourceOp,
    InlineRecordsOp, IvmGraph, JoinOp, JoinOpKind, LiteralValue, MAX_COLLECT_BY_TREE_DEPTH,
    MapProjectOp, NodeDescriptor, NodeDurability, NodeId, NodeOutput, OpType, PersistOp, PlanExpr,
    PredicateExpr, ProjectExpr, ProjectField, ProjectionExpr, RecursiveEnumRemaps, RecursiveOp,
    Retainer, StaticScanSpec, StreamingChecksumOp, TableSourceOp, TopByDirection, TopByLimit,
    TopByOp, TopByOrderField, UnnestOp, UnwrapNullableOp, ValueComparison, VariantProjectOp,
    VariantProjectionTarget,
};
use crate::records::{
    self, BorrowedRecord, EnumSchema, EnumValue, OwnedRecord, RawProjectionField,
    RawProjectionScratch, RecordDescriptor, Value, ValueType, collect_by_ordered_scalar,
};
use crate::schema::{DatabaseSchema, IndexSchema, TableSchema};
use crate::storage::{OrderedKvStorage, RecordStore, ScanBounds, ScanDirection, ScanRequest};
use thiserror::Error;

mod aggregate;
pub(crate) mod evaluation_session;
mod join;
mod persist;
mod recursion;
mod state;
mod terminal;

use aggregate::{aggregate_row_from_records, records_before_from_deltas, resolve_aggregate_expr};
use join::{AntiJoinState, ArrangementState, JoinState, touched_join_keys};
use persist::apply_persist_delta;
use recursion::{
    RecursiveState, hydrate_recursive_arrangements, recompute_recursive, recursive_delta,
    recursive_read_tables, require_snapshot_inputs, snapshot_requirement,
};
use state::{
    ArrangementKey, ArrangementUpdateMode, AsOf, EvalContext, EvalMemoEntry, EvalMemoKey, EvalMode,
    HydrationMode, NodeInputSignature, OperatorStateKey, ScopeId, SubTick, Tick,
};
pub use state::{RuntimeStats, TickMetrics};
pub use terminal::{TerminalDeltas, TerminalEdit, TerminalOperation, TerminalPathSegment};
use terminal::{order_terminal_snapshot, terminal_deltas_from_record_deltas};

const DEFAULT_SINK: &str = "__default";
const EVAL_MEMO_MAX_ENTRIES: usize = 8192;
const EVAL_MEMO_MAX_BYTES: usize = 128 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct VariantProjectionKey {
    table: String,
    target: VariantProjectionTarget,
}

#[derive(Clone, Debug)]
pub(super) struct VariantProjection {
    output: RecordDescriptor,
    cases: HashMap<u32, VariantProjectionCase>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum VariantProjectionCase {
    Project {
        source: RecordDescriptor,
        project: MapProjectOp,
        raw_projection: Option<Arc<[RawProjectionField]>>,
        /// A Jazz schema-read boundary may exclude rows containing a case the
        /// target schema cannot represent. This is deliberately narrower than
        /// a general projection error: malformed values still fail loudly.
        omit_unrepresentable_enum_rows: bool,
    },
    Ignore {
        source: RecordDescriptor,
    },
    Enum {
        source: RecordDescriptor,
        tag: u32,
        payload: RecordDescriptor,
        project: MapProjectOp,
        raw_projection: Option<Arc<[RawProjectionField]>>,
    },
}

impl VariantProjectionCase {
    fn source(&self) -> RecordDescriptor {
        match self {
            Self::Project { source, .. } | Self::Ignore { source } | Self::Enum { source, .. } => {
                *source
            }
        }
    }

    /// A raw source case may refresh after live registry evolution only when
    /// its projection program is unchanged and its input descriptor advances
    /// append-only. In particular, this rejects a replacement mapping, a
    /// dropped/renamed field, and an enum payload/type mutation.
    fn can_refresh_registries_to(&self, next: &Self) -> bool {
        match (self, next) {
            (
                Self::Project {
                    source: current_source,
                    project: current_project,
                    omit_unrepresentable_enum_rows: current_omit,
                    ..
                },
                Self::Project {
                    source: next_source,
                    project: next_project,
                    omit_unrepresentable_enum_rows: next_omit,
                    ..
                },
            ) => {
                current_omit == next_omit
                    && current_project == next_project
                    && current_source.can_evolve_registry_to(next_source)
            }
            (Self::Ignore { source: current }, Self::Ignore { source: next }) => {
                current.can_evolve_registry_to(next)
            }
            _ => false,
        }
    }
}

// These maps are keyed by local runtime/schema/graph metadata produced after
// validation. Wire-facing or otherwise adversarial-input maps must keep the
// standard hasher; this alias is intentionally scoped to the IVM runtime.

/// Stateful executor for deduplicated IVM graphs and subscriptions.
#[derive(Clone, Debug)]
pub struct IvmRuntime {
    schema: DatabaseSchema,
    chunk_provider: crate::chunks::OwnedChunkProvider,
    table_storage_descriptors: HashMap<String, RecordDescriptor>,
    table_descriptors: HashMap<String, RecordDescriptor>,
    variant_descriptors: HashMap<String, HashMap<u32, RecordDescriptor>>,
    /// Append-only, fixed-output projection families for heterogeneous table
    /// sources. Cases are runtime input metadata rather than graph identity.
    variant_projections: HashMap<VariantProjectionKey, VariantProjection>,
    graph: IvmGraph,
    multisink_subscriptions: HashMap<SubscriptionId, MultisinkSubscriptionState>,
    subscriptions_by_output_node: HashMap<NodeId, HashSet<SubscriptionId>>,
    pending_incremental: runtime_tick::PendingIncrementalEvaluation,
    prepared_shapes: HashMap<PreparedShapeId, RoutedMultisinkShapeState>,
    auto_direct_families: HashMap<AutoDirectFamilyKey, PreparedShapeId>,
    binding_sources: HashMap<String, BindingSourceState>,
    /// Binding retractions discovered while routing notifications cannot tick
    /// recursively; the next public tick drains them before user deltas run.
    pending_binding_retractions: Vec<BindingDelta>,
    deferred_notifications: HashMap<PublicationId, Vec<(SubscriptionId, QueuedMultisinkDeltas)>>,
    durable_notification_publications: HashSet<PublicationId>,
    completed_deferred_publications: HashSet<PublicationId>,
    /// Persistent operator state keyed by scope and node. This survives ticks;
    /// see [`EvalMemoKey`] for per-evaluation caching.
    operator_states: HashMap<OperatorStateKey, OperatorState>,
    /// Reusable indexed multisets for join inputs. These are keyed by input
    /// fragment, key fields, descriptor, and scope so similar queries can share
    /// expensive context-independent arrangements.
    arrangement_states: HashMap<ArrangementKey, AsOf<ArrangementState, SubTick>>,
    arrangement_keys_by_input: HashMap<NodeId, HashSet<ArrangementKey>>,
    /// Input-owned memoization for pure node evaluation results. Entries are
    /// keyed by node/scope/context inputs and validated against per-input
    /// frontier counters before reuse; operator state remains owned separately.
    eval_memo: HashMap<EvalMemoKey, EvalMemoEntry>,
    table_frontiers: HashMap<String, u64>,
    binding_frontiers: HashMap<String, u64>,
    memo_use_clock: u64,
    eval_memo_bytes: usize,
    hydration_memo_hits: u64,
    hydration_memo_computes: u64,
    hydration_memo_computed_nodes: HashSet<NodeId>,
    /// Retainers and GC age live outside operator state so stateless leaf nodes
    /// can be retained without allocating fake operator state.
    node_meta: HashMap<NodeId, NodeRuntimeMeta>,
    current_tick: u64,
    next_subscription_id: u64,
    next_shape_id: u64,
    logical_nodes_requested: u64,
    auto_direct_family_enabled: bool,
    collect_tick_runtime_stats: bool,
}

impl IvmRuntime {
    pub fn new(schema: DatabaseSchema) -> Result<Self, IvmRuntimeError> {
        let table_storage_descriptors = schema
            .tables
            .iter()
            .map(|table| (table.name.clone(), table.record_schema()))
            .collect();
        let table_descriptors = schema
            .tables
            .iter()
            .filter(|table| !table.has_variants())
            .map(|table| (table.name.clone(), table.record_schema()))
            .collect();
        let variant_descriptors = schema
            .tables
            .iter()
            .filter(|table| table.has_variants())
            .map(|table| {
                let descriptors = table
                    .variants
                    .iter()
                    .filter_map(|variant| {
                        table
                            .record_schema_for_variant(variant.tag)
                            .map(|descriptor| (variant.tag, descriptor))
                    })
                    .collect();
                (table.name.clone(), descriptors)
            })
            .collect();
        let mut runtime = Self {
            schema,
            chunk_provider: crate::chunks::OwnedChunkProvider::default(),
            table_storage_descriptors,
            table_descriptors,
            variant_descriptors,
            variant_projections: HashMap::default(),
            graph: IvmGraph::new(),
            multisink_subscriptions: HashMap::default(),
            subscriptions_by_output_node: HashMap::default(),
            pending_incremental: runtime_tick::PendingIncrementalEvaluation::default(),
            operator_states: HashMap::default(),
            arrangement_states: HashMap::default(),
            arrangement_keys_by_input: HashMap::default(),
            eval_memo: HashMap::default(),
            table_frontiers: HashMap::default(),
            binding_frontiers: HashMap::default(),
            memo_use_clock: 0,
            eval_memo_bytes: 0,
            hydration_memo_hits: 0,
            hydration_memo_computes: 0,
            hydration_memo_computed_nodes: HashSet::default(),
            node_meta: HashMap::default(),
            current_tick: 0,
            next_subscription_id: 1,
            next_shape_id: 1,
            logical_nodes_requested: 0,
            auto_direct_family_enabled: true,
            collect_tick_runtime_stats: false,
            prepared_shapes: HashMap::default(),
            auto_direct_families: HashMap::default(),
            binding_sources: HashMap::default(),
            pending_binding_retractions: Vec::new(),
            deferred_notifications: HashMap::default(),
            durable_notification_publications: HashSet::default(),
            completed_deferred_publications: HashSet::default(),
        };
        runtime.define_schema_index_variant_projections()?;
        runtime.add_dedup_schema_indices()?;
        Ok(runtime)
    }

    pub(crate) fn set_chunk_provider(
        &mut self,
        provider: std::rc::Rc<dyn crate::chunks::ChunkProvider>,
    ) {
        self.chunk_provider = crate::chunks::OwnedChunkProvider::new(provider);
    }

    pub(crate) fn set_owned_chunk_provider(&mut self, provider: crate::chunks::OwnedChunkProvider) {
        self.chunk_provider = provider;
    }

    pub(crate) fn chunk_provider(&self) -> crate::chunks::OwnedChunkProvider {
        self.chunk_provider.clone()
    }

    pub fn set_tick_runtime_stats_enabled(&mut self, enabled: bool) {
        self.collect_tick_runtime_stats = enabled;
    }

    pub fn graph(&self) -> &IvmGraph {
        &self.graph
    }

    pub fn set_auto_direct_family_enabled(&mut self, enabled: bool) {
        self.auto_direct_family_enabled = enabled;
    }

    pub fn schema(&self) -> &DatabaseSchema {
        &self.schema
    }

    pub fn table(&self, table: &str) -> Option<&TableSchema> {
        self.schema.table(table)
    }

    pub fn table_descriptor(&self, table: &str) -> Option<&RecordDescriptor> {
        self.table_descriptors.get(table)
    }

    pub(crate) fn table_storage_descriptor(&self, table: &str) -> Option<&RecordDescriptor> {
        self.table_storage_descriptors.get(table)
    }

    pub(crate) fn record_descriptor(
        &self,
        table: &str,
        variant_tag: u32,
    ) -> Option<&RecordDescriptor> {
        self.table_descriptors
            .get(table)
            .filter(|_| variant_tag == 0)
            .or_else(|| {
                self.variant_descriptors
                    .get(table)
                    .and_then(|descriptors| descriptors.get(&variant_tag))
            })
    }
}

mod compilation;
mod graph_lifecycle;
mod runtime_tick;
mod schema;
mod subscriptions;
pub use subscriptions::*;
mod operator_updates;
use operator_updates::*;
mod evaluator;
use evaluator::*;
mod record_projection;
use record_projection::*;
mod key_encoding;
use key_encoding::*;
pub(crate) use key_encoding::{durable_index_key_prefix, encode_key_part};
mod windows;
use windows::*;

#[derive(Debug, Error)]
pub enum IvmRuntimeError {
    #[error("aggregate result exceeds its declared numeric width")]
    AggregateOverflow,
    #[error("graph field not found: {0}")]
    GraphFieldNotFound(String),
    #[error("graph field index out of bounds: {0}")]
    GraphFieldIndexOutOfBounds(usize),
    #[error("graph node has unexpected input arity: {0:?}")]
    GraphInputArityMismatch(NodeId),
    #[error("graph node is missing input: {0:?}")]
    GraphInputMissing(NodeId),
    #[error("graph node not found: {0:?}")]
    GraphNodeNotFound(NodeId),
    #[error("graph contains a dependency cycle at node: {0:?}")]
    GraphCycle(NodeId),
    #[error("graph output descriptors do not match")]
    GraphOutputMismatch,
    #[error("enum tag {tag} is absent from this projection target")]
    EnumTagProjectionAbsent { tag: u8 },
    #[error("enum tag projection requires an enum value")]
    EnumTagProjectionNonEnum,
    #[error("payload enum tag {tag} is absent from this projection target")]
    EnumProjectionAbsent { tag: u32 },
    #[error("payload enum projection requires an enum value")]
    EnumProjectionNonEnum,
    #[error("index not found: {0}")]
    IndexNotFound(String),
    #[error("invalid persisted index entry: {0}")]
    InvalidPersistedIndex(String),
    #[error("intersected index sources currently require prefix scans")]
    UnsupportedIndexIntersectionScan,
    #[error("join key arity mismatch: left={left}, right={right}")]
    JoinKeyArityMismatch { left: usize, right: usize },
    #[error("shape key field not found: {0}")]
    ShapeKeyFieldNotFound(String),
    #[error("table has no primary key: {0}")]
    MissingPrimaryKey(String),
    #[error("runtime node state missing: {0:?}")]
    NodeStateMissing(NodeId),
    #[error("runtime node state operator mismatch: {0:?}")]
    NodeStateOperatorMismatch(NodeId),
    #[error("runtime state advanced out of order: current={current}, next={next}")]
    OutOfOrderRuntimeState { current: String, next: String },
    #[error("persist node expected key/value bytes")]
    PersistRecordMismatch,
    #[error("binding sources can only be evaluated through prepared shapes")]
    BindingSourceRequiresPrepare,
    #[error("multisink subscription must have at least one sink")]
    EmptyMultisinkSubscription,
    #[error("multisink sink already exists: {0}")]
    DuplicateMultisinkSink(String),
    #[error("multisink sink requires prepare because it contains a binding source: {0}")]
    MultisinkSinkRequiresPrepare(String),
    #[error("routed multisink sink {sink} has {actual} route fields, expected {expected}")]
    RoutedMultisinkRouteArityMismatch {
        sink: String,
        expected: usize,
        actual: usize,
    },
    #[error("binding source not found: {0}")]
    BindingSourceNotFound(String),
    #[error("binding source descriptor mismatch: {0}")]
    BindingSourceDescriptorMismatch(String),
    #[error("duplicate schema version {version} for table {table}")]
    DuplicateTableVariant { table: String, version: u64 },
    #[error(transparent)]
    RecordEncoding(#[from] records::Error),
    #[error("recursive node {node:?} exceeded iteration limit {max_iters}")]
    RecursiveIterationLimit { node: NodeId, max_iters: usize },
    #[error(transparent)]
    Storage(#[from] crate::storage::Error),
    #[error(transparent)]
    Chunk(#[from] crate::chunks::ChunkError),
    #[error(transparent)]
    LargeValue(#[from] crate::large_values::Error),
    #[error("storage unavailable for durable node")]
    StorageUnavailable,
    #[error("evaluation blocked on a non-resident storage input")]
    EvaluationBlocked,
    #[error("streaming checksum window and work budgets must be non-zero")]
    InvalidStreamingChecksumBudget,
    #[error("streaming checksum requires a String or Bytes field")]
    StreamingChecksumTypeMismatch,
    #[error("subscription shape not found: {0:?}")]
    PreparedShapeNotFound(PreparedShapeId),
    #[error("cannot retire prepared shape {0:?} while it has active bindings")]
    PreparedShapeHasActiveBindings(PreparedShapeId),
    #[error("runtime state is stale: expected={expected}, actual={actual:?}")]
    StaleRuntimeState {
        expected: String,
        actual: Option<String>,
    },
    #[error("table not found: {0}")]
    TableNotFound(String),
    #[error("table already exists: {0}")]
    TableAlreadyExists(String),
    #[error("field already exists in the live catalogue: {table}.{field}")]
    TableFieldAlreadyExists { table: String, field: String },
    #[error("unknown schema version {version} for table {table}")]
    UnknownTableVariant { table: String, version: u64 },
    #[error("variant projection not found: {table}.{target}")]
    VariantProjectionNotFound { table: String, target: String },
    #[error("schema-variant table requires a fixed-output projection: {0}")]
    VariantProjectionRequired(String),
    #[error("variant projection output descriptor mismatch: {table}.{target}")]
    VariantProjectionOutputMismatch { table: String, target: String },
    #[error(
        "variant projection case already registered with different semantics: {table}.{target} version {version}"
    )]
    VariantProjectionCaseAlreadyRegistered {
        table: String,
        target: String,
        version: u64,
    },
    #[error("variant projection case not found: {table}.{target} version {version}")]
    VariantProjectionCaseNotFound {
        table: String,
        target: String,
        version: u64,
    },
    #[error("variant projection source descriptor mismatch: {table}.{target} version {version}")]
    VariantProjectionSourceMismatch {
        table: String,
        target: String,
        version: u64,
    },
    #[error("variant projection enum field not found: {table}.{target}.{field}")]
    VariantProjectionEnumFieldNotFound {
        table: String,
        target: String,
        field: String,
    },
    #[error("variant projection enum output must contain exactly one field: {table}.{target}")]
    VariantProjectionEnumOutputMustBeSingleField { table: String, target: String },
    #[error("variant projection field is not an enum: {table}.{target}.{field}")]
    VariantProjectionEnumFieldTypeMismatch {
        table: String,
        target: String,
        field: String,
    },
    #[error("variant projection enum schema does not match fixed output: {table}.{target}.{field}")]
    VariantProjectionEnumSchemaMismatch {
        table: String,
        target: String,
        field: String,
    },
    #[error("variant projection payload does not match enum case: {table}.{target}.{case}")]
    VariantProjectionEnumPayloadMismatch {
        table: String,
        target: String,
        case: String,
    },
    #[error("recursive enum projection descriptor mismatch at {path}")]
    RecursiveEnumProjectionDescriptorMismatch { path: String },
    #[error("enum match field is not an enum: {field}")]
    VariantProjectFieldTypeMismatch { field: String },
    #[error("enum match payload descriptor does not match its declared case: {field}")]
    VariantProjectPayloadMismatch { field: String },
    #[error("unique index violation: {index}")]
    UniqueIndexViolation { index: String },
    #[error("unsupported join key")]
    UnsupportedJoinKey,
    #[error("non-monotone recursive delta reached positive-only incremental recursion")]
    UnsupportedNonMonotoneRecursion,
    #[error("nested recursive graphs are not supported in v0")]
    UnsupportedNestedRecursion,
    #[error("unsupported arg_max_by graph: {0}")]
    UnsupportedArgMaxBy(String),
    #[error("collect_by is terminal-only and cannot feed another graph node")]
    CollectByMustBeTerminal,
    #[error("invalid collect_by descriptor: {0}")]
    InvalidCollectBy(String),
    #[error("invalid top_by descriptor: {0}")]
    InvalidTopBy(String),
    #[error("collect_by expand encountered duplicate output occurrence source ids")]
    DuplicateCollectByOccurrenceId,
    #[error("unsupported operator")]
    UnsupportedOperator,
}

#[cfg(test)]
mod tests;
