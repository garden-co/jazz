//! Shared state and evaluation context for the IVM runtime.

use std::{
    collections::{BTreeSet, HashSet, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    sync::{Arc, OnceLock},
};

use rustc_hash::FxHashMap as HashMap;

use crate::ivm::{BindingSourceKey, FrontierName, NodeId};

use super::{IvmRuntimeError, RecordDeltas, record_deltas_digest};

/// Point-in-time runtime counters for benchmark and diagnostics reporting.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RuntimeStats {
    pub graph_nodes: usize,
    pub active_subscriptions: usize,
    pub active_prepared_shapes: usize,
    pub active_shape_params: usize,
    pub arrangement_count: usize,
    pub eval_memo_entries: usize,
    pub eval_memo_bytes: usize,
    pub hydration_memo_entries: usize,
    pub hydration_memo_hits: u64,
    pub hydration_memo_computes: u64,
    pub hydration_memo_distinct_computed_nodes: usize,
    pub arrangement_rows: usize,
    pub arrangement_encoded_bytes: usize,
    pub recursive_state_count: usize,
    pub recursive_accumulated_rows: usize,
    pub recursive_accumulated_encoded_bytes: usize,
    pub logical_nodes_requested: u64,
    pub deduped_graph_nodes: usize,
}

impl RuntimeStats {
    pub fn dedupe_ratio(&self) -> f64 {
        if self.logical_nodes_requested == 0 {
            return 1.0;
        }
        self.deduped_graph_nodes as f64 / self.logical_nodes_requested as f64
    }
}

/// Metrics produced by one runtime tick.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TickMetrics {
    pub tick: u64,
    pub table_delta_records: usize,
    pub records_processed: usize,
    pub recursive_recomputes: usize,
    pub hydration_memo_hits: u64,
    pub hydration_memo_computes: u64,
    pub hydration_memo_computed_nodes: HashSet<NodeId>,
    pub notifications_sent: usize,
    pub notification_records: usize,
    pub notification_encoded_bytes: usize,
    pub runtime_stats: RuntimeStats,
}

/// Recursive scope path used to namespace context-dependent state.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct ScopePath(Vec<NodeId>);

impl ScopePath {
    pub(super) fn root() -> Self {
        Self(Vec::new())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ScopeId(crate::Intern<ScopePath>);

impl ScopeId {
    pub(super) fn root() -> Self {
        static ROOT: OnceLock<ScopeId> = OnceLock::new();
        *ROOT.get_or_init(|| Self(crate::Intern::new(ScopePath::root())))
    }

    pub(super) fn child(self, recursive_node: NodeId) -> Self {
        let mut scope = self.0.0.clone();
        scope.push(recursive_node);
        Self(crate::Intern::new(ScopePath(scope)))
    }
}

impl Default for ScopeId {
    fn default() -> Self {
        Self::root()
    }
}

/// Key for operator state that must survive across ticks.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct OperatorStateKey {
    /// Empty for normal query execution; nested recursive scopes append their
    /// recursive node ids here.
    pub(super) scope: ScopeId,
    pub(super) node: NodeId,
}

/// Key for a reusable join-side arrangement.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct ArrangementKey {
    /// Context-independent inputs use the root scope and can be shared across
    /// unrelated subscriptions.
    pub(super) scope: ScopeId,
    /// The typed `Arrange` graph node. Its descriptor owns the input, fields,
    /// record type, and comparison semantics.
    pub(super) input: NodeId,
}

/// Database tick plus recursive sub-tick for scoped arrangement freshness.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(super) struct SubTick {
    pub(super) tick: u64,
    pub(super) sub_tick: u64,
}

/// Logical database tick for state whose contents are only root-tick scoped.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(super) struct Tick(pub(super) u64);

/// Runtime state together with the logical time its contents reflect.
///
/// Keeping the "as of" time outside operator-specific state makes freshness
/// checks visible at shared-state access sites instead of burying them inside
/// joins, recursion, or future stateful operators.
#[derive(Clone, Debug)]
pub(super) struct AsOf<T, S> {
    pub(super) value: T,
    pub(super) as_of: Option<S>,
}

impl<T, S> AsOf<T, S> {
    pub(super) fn new(value: T) -> Self {
        Self { value, as_of: None }
    }

    pub(super) fn value(&self) -> &T {
        &self.value
    }

    pub(super) fn value_mut(&mut self) -> &mut T {
        &mut self.value
    }

    pub(super) fn as_of(&self) -> Option<S>
    where
        S: Copy,
    {
        self.as_of
    }
}

impl<T, S> AsOf<T, S>
where
    S: Copy + Ord + std::fmt::Debug,
{
    pub(super) fn value_at(&self, expected: S) -> Result<&T, IvmRuntimeError> {
        if self.as_of == Some(expected) {
            return Ok(&self.value);
        }
        Err(IvmRuntimeError::StaleRuntimeState {
            expected: format!("{expected:?}"),
            actual: self.as_of.map(|actual| format!("{actual:?}")),
        })
    }

    pub(super) fn mark_forward_as_of(&mut self, next: S) -> Result<(), IvmRuntimeError> {
        if self.as_of.is_some_and(|current| current > next) {
            return Err(IvmRuntimeError::OutOfOrderRuntimeState {
                current: format!("{:?}", self.as_of.expect("checked above")),
                next: format!("{next:?}"),
            });
        }
        self.as_of = Some(next);
        Ok(())
    }

    pub(super) fn replace_as_of_at_least(&mut self, next: S) {
        if self.as_of.is_none_or(|current| current <= next) {
            self.as_of = Some(next);
        }
    }
}

impl<T: Default, S> Default for AsOf<T, S> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

/// Whether an arrangement should consume a delta or be rebuilt from a snapshot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum ArrangementUpdateMode {
    #[default]
    Accumulate,
    Replace,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum EvalMode {
    #[default]
    Tick,
    Hydrate,
}

/// Hydration consumers that need aggregate arrangements rebuilt opt into the
/// subscription policy; all other snapshot consumers retain probe semantics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum HydrationMode {
    Ordinary,
    Subscription,
}

/// Key for one cached node evaluation within a logical tick.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct EvalMemoKey {
    pub(super) scope: ScopeId,
    pub(super) node: NodeId,
    pub(super) input_signature_hash: u64,
    /// Tick-mode results are deltas and are only reusable inside one public
    /// tick. Hydration results are snapshots, so their key omits the tick and
    /// validity is owned by the input frontier vector stored on the entry.
    pub(super) tick_epoch: Option<u64>,
    /// Recursive sub-ticks intentionally affect memoization, not operator
    /// state identity.
    pub(super) sub_tick: u64,
    pub(super) context_digest: u64,
}

#[derive(Clone, Debug)]
pub(super) struct EvalMemoEntry {
    pub(super) records: Arc<RecordDeltas>,
    pub(super) input_watermark: u64,
    pub(super) payload_bytes: usize,
    pub(super) last_used: u64,
}

impl EvalMemoEntry {
    pub(super) fn new(
        records: Arc<RecordDeltas>,
        input_watermark: u64,
        payload_bytes: usize,
        last_used: u64,
    ) -> Self {
        Self {
            records,
            input_watermark,
            payload_bytes,
            last_used,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct NodeInputSignature {
    pub(super) tables: Arc<[String]>,
    pub(super) bindings: Arc<[BindingSourceKey]>,
    pub(super) frontier_bindings: Arc<[FrontierName]>,
    pub(super) hash: u64,
}

impl NodeInputSignature {
    pub(super) fn from_sets(
        tables: BTreeSet<String>,
        bindings: BTreeSet<BindingSourceKey>,
        frontier_bindings: BTreeSet<FrontierName>,
    ) -> Self {
        let tables = tables.into_iter().collect::<Arc<[_]>>();
        let bindings = bindings.into_iter().collect::<Arc<[_]>>();
        let frontier_bindings = frontier_bindings.into_iter().collect::<Arc<[_]>>();
        let mut hasher = DefaultHasher::new();
        tables.hash(&mut hasher);
        bindings.hash(&mut hasher);
        frontier_bindings.hash(&mut hasher);
        let hash = hasher.finish();
        Self {
            tables,
            bindings,
            frontier_bindings,
            hash,
        }
    }
}

/// Current scoped inputs and logical time for node evaluation.
#[derive(Clone, Debug, Default)]
pub(super) struct EvalContext {
    /// Current operator-state namespace.
    pub(super) scope: ScopeId,
    /// Logical time within a recursive fixed-point evaluation.
    pub(super) sub_tick: u64,
    /// FrontierSource bindings, currently used for recursive frontiers.
    pub(super) bindings: HashMap<FrontierName, RecordDeltas>,
    pub(super) binding_digests: HashMap<FrontierName, u64>,
    /// Hydrate preparation rebuilds arrangements instead of layering onto them.
    pub(super) arrangement_update_mode: ArrangementUpdateMode,
    pub(super) eval_mode: EvalMode,
    pub(super) hydrate_arrangements: bool,
}

impl EvalContext {
    pub(super) fn root() -> Self {
        Self {
            scope: ScopeId::root(),
            sub_tick: 0,
            bindings: HashMap::default(),
            binding_digests: HashMap::default(),
            arrangement_update_mode: ArrangementUpdateMode::Accumulate,
            eval_mode: EvalMode::Tick,
            hydrate_arrangements: false,
        }
    }

    pub(super) fn root_snapshot() -> Self {
        Self {
            scope: ScopeId::root(),
            sub_tick: 0,
            bindings: HashMap::default(),
            binding_digests: HashMap::default(),
            arrangement_update_mode: ArrangementUpdateMode::Replace,
            eval_mode: EvalMode::Hydrate,
            hydrate_arrangements: false,
        }
    }

    pub(super) fn root_subscription_snapshot() -> Self {
        Self {
            scope: ScopeId::root(),
            sub_tick: 0,
            bindings: HashMap::default(),
            binding_digests: HashMap::default(),
            arrangement_update_mode: ArrangementUpdateMode::Replace,
            eval_mode: EvalMode::Hydrate,
            hydrate_arrangements: true,
        }
    }

    pub(super) fn with_binding(
        scope: ScopeId,
        sub_tick: u64,
        binding: FrontierName,
        deltas: RecordDeltas,
    ) -> Self {
        let mut bindings = HashMap::default();
        let digest = record_deltas_digest(&deltas);
        bindings.insert(binding.clone(), deltas);
        let mut binding_digests = HashMap::default();
        binding_digests.insert(binding, digest);
        Self {
            scope,
            sub_tick,
            bindings,
            binding_digests,
            arrangement_update_mode: ArrangementUpdateMode::Accumulate,
            eval_mode: EvalMode::Tick,
            hydrate_arrangements: false,
        }
    }

    pub(super) fn with_binding_and_arrangement_mode(
        scope: ScopeId,
        sub_tick: u64,
        binding: FrontierName,
        deltas: RecordDeltas,
        arrangement_update_mode: ArrangementUpdateMode,
    ) -> Self {
        let mut bindings = HashMap::default();
        let digest = record_deltas_digest(&deltas);
        bindings.insert(binding.clone(), deltas);
        let mut binding_digests = HashMap::default();
        binding_digests.insert(binding, digest);
        Self {
            scope,
            sub_tick,
            bindings,
            binding_digests,
            arrangement_update_mode,
            eval_mode: EvalMode::Tick,
            hydrate_arrangements: false,
        }
    }
}
