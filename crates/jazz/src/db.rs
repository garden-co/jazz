//! High-level thread-affine database facade described by `jazz/API.md`. This
//! module owns application-facing handles, read/write options, and facade-level
//! sync plumbing; durable version storage, validation, policy checks, and view
//! construction live in [`crate::node`], while link-local shipped state lives in
//! [`crate::peer`]. In the layer map this is the top `Db` facade over the node.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::{Pin, pin};
use std::rc::{Rc, Weak};
#[cfg(feature = "sync-autopsy")]
use std::sync::{
    LazyLock, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Context, Poll, Waker};

use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};
use futures_channel::oneshot;
use futures_core::Stream;
use groove::records::{OwnedRecord, RecordDescriptor, Value};
use groove::schema::ColumnType as GrooveColumnType;
use groove::storage::{OrderedKvStorage, ReopenableStorage};
use thiserror::Error;
#[cfg(feature = "cold-settle-attribution")]
use web_time::Instant;

use crate::authorization_scope::{
    AuthorityContext, AuthorityScopeAggregate, AuthorizationScopeAcquisition,
    AuthorizationScopeInstall, AuthorizationScopeLease, AuthorizationScopeOwnerToken,
    AuthorizationScopeReadiness, AuthorizationScopeRegistry, MAX_AUTHORIZATION_SCOPES,
};
use crate::ids::{AuthorId, NodeUuid, RowUuid, SchemaVersionId};
pub use crate::node::CommitUnitTrust;
#[cfg(feature = "testing")]
pub use crate::node::NodeOpenReceipt as DbOpenReceipt;
use crate::node::query_engine::QueryAuthorizationMode;
use crate::node::{
    CommitUnitIngestContext, CurrentRow, EdgeCacheBudget, LocalMaintainedViewSubscription,
    LocalMaintainedViewSubscriptionUpdate, MergeableCommit, NodeState, PreparedQueryPlanHandle,
    QueryReadProfile, RelationEdge, RelationSnapshot, RowProvenance, ViewUpdateParts,
};
use crate::peer::{PeerRole, PeerState};
pub use crate::protocol::PermissionAdvice;
#[cfg(feature = "sync-autopsy")]
use crate::protocol::expand_version_carriers;
use crate::protocol::{
    AuthorizationScopeReceipt, BindingViewKey, CoverageKey, CurrentWriteSchema, LensOp,
    MigrationLens, PermissionAdviceAction, PermissionAdviceRequestId, ReadViewKey,
    ReadViewSourceSpec, ReadViewSpec, RegisterShapeOptions, SchemaLineagePublication,
    SchemaVersion, ShapeAst, Subscribe, SubscribeRejectReason, SubscribeServerFailureCode,
    SubscriptionKey, SyncMessage, TableLens,
};
use crate::protocol_limits::{
    MAX_FETCH_BRANCH_METADATA, validate_fetch_branch_metadata, validate_fetch_row_versions,
    validate_known_state_declaration, validate_shape_ast_size,
};
use crate::query::{
    Binding, BindingId, Operand, Predicate, Query, QueryError, RelationQuery, ShapeId,
    ValidatedQuery, relation_query_to_query,
};
#[cfg(test)]
use crate::query::{
    RelationCmpOp, RelationColumnRef, RelationExpr, RelationJoinKind, RelationPredicate,
    RelationProjectExpr, RelationRowIdRef, RelationValueRef,
};
pub use crate::result_tree::{ResultNode, ResultRelation, ResultTree, ResultTreeReplacement};
use crate::schema::{JazzSchema, TableSchema};
use crate::time::GlobalSeq;
use crate::tools::OpenBatchId;
use crate::tools::{BatchId, ObjectId, OutputOccurrenceId, ResultKey};
use crate::tx::{DeletionEvent, DurabilityTier, Fate, RejectionReason, TxId, TxKind};
use crate::wire::{TransportError, WireAuthorityEndpoint, WireFeatures, encode_sync_message};

mod wire_transport;
#[cfg(test)]
use wire_transport::LogicalMessageReassembler;
pub use wire_transport::WireTransportAdapter;

/// How urgently a runtime should service pending peer-connection work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TickUrgency {
    /// Run as soon as the runtime can do so without re-entering the current
    /// mutable operation. Used when a query/subscription/transport event needs
    /// prompt coverage or inbound draining.
    Immediate,
    /// Coalesce bursty local work before ticking. Used for uploads created by
    /// local writes.
    Deferred,
}

/// Runtime-neutral wake hook for thread-affine [`Node`] sync work.
pub trait TickScheduler {
    /// Schedule a future [`Db::tick`] for pending peer-connection work.
    fn schedule_tick(&self, urgency: TickUrgency);
}

/// A locally-originated transaction rejection that was not consumed by an
/// active write waiter.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MutationErrorEvent {
    /// Stable machine-readable rejection code.
    pub code: String,
    /// Human-readable rejection reason.
    pub reason: String,
    /// The rejected local transaction.
    pub transaction: LocalTransactionRecord,
}

/// Binding-facing record for one locally committed transaction.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalTransactionRecord {
    /// Stable public identity derived from the core transaction id.
    pub batch_id: BatchId,
    /// Transaction semantics used by the commit.
    pub kind: TransactionKind,
    /// Committed transaction records are immutable.
    pub sealed: bool,
    /// Latest authority settlement observed for the transaction.
    pub latest_settlement: TransactionFate,
}

/// Binding-facing transaction kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TransactionKind {
    /// CRDT-style mergeable transaction.
    Mergeable,
    /// Authority-validated exclusive transaction.
    Exclusive,
}

impl From<TxKind> for TransactionKind {
    fn from(kind: TxKind) -> Self {
        match kind {
            TxKind::Mergeable => Self::Mergeable,
            TxKind::Exclusive => Self::Exclusive,
        }
    }
}

/// Binding-facing authority fate for a rejected transaction.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum TransactionFate {
    /// The authority rejected the transaction.
    Rejected {
        /// Stable public identity derived from the core transaction id.
        #[serde(rename = "batchId")]
        batch_id: BatchId,
        /// Stable machine-readable rejection code.
        code: String,
        /// Human-readable rejection reason.
        reason: String,
    },
}

/// Thread-affine callback used by bindings to surface unhandled rejections.
pub type MutationErrorCallback = Rc<dyn Fn(&MutationErrorEvent) + 'static>;

#[cfg(feature = "sync-autopsy")]
/// Debug-build sync trace buffer used by integration-test timeout autopsies.
pub mod sync_autopsy {
    use super::*;

    const MAX_EVENTS: usize = 512;

    static ENABLED: AtomicBool = AtomicBool::new(false);
    static EVENTS: LazyLock<Mutex<VecDeque<String>>> =
        LazyLock::new(|| Mutex::new(VecDeque::with_capacity(MAX_EVENTS)));

    /// Enable passive event capture for the current process.
    pub fn enable() {
        ENABLED.store(true, Ordering::Relaxed);
    }

    /// Clear buffered events.
    pub fn clear() {
        if let Ok(mut events) = EVENTS.lock() {
            events.clear();
        }
    }

    /// Return the current buffered event log.
    pub fn dump() -> String {
        let events = EVENTS.lock().ok();
        let mut out = String::from("sync autopsy events:\n");
        if let Some(events) = events {
            for event in events.iter() {
                out.push_str("  ");
                out.push_str(event);
                out.push('\n');
            }
        } else {
            out.push_str("  <event buffer poisoned>\n");
        }
        out
    }

    /// Append one event to the ring buffer when capture is enabled.
    pub fn record(event: impl Into<String>) {
        if !ENABLED.load(Ordering::Relaxed) {
            return;
        }
        let Ok(mut events) = EVENTS.lock() else {
            return;
        };
        if events.len() == MAX_EVENTS {
            events.pop_front();
        }
        events.push_back(event.into());
    }
}

#[cfg(not(feature = "sync-autopsy"))]
/// No-op sync trace buffer when sync autopsy capture is not compiled in.
pub mod sync_autopsy {
    /// Enable passive event capture for the current process.
    pub fn enable() {}
    /// Clear buffered events.
    pub fn clear() {}
    /// Return the current buffered event log.
    pub fn dump() -> String {
        String::new()
    }
}

/// Poll a ready-immediate thread-affine database future to completion.
///
/// This helper is intentionally tiny: it drives local-lane futures that are
/// expected to complete without an async runtime by using a no-op waker and
/// yielding the current thread when a future reports `Pending`.
pub fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut future = pin!(future);

    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

/// Thread-affine high-level database handle.
pub struct Db<S>
where
    S: OrderedKvStorage,
{
    schema: JazzSchema,
    schema_version_id: SchemaVersionId,
    schema_view_is_fixed: bool,
    schema_views: Rc<RefCell<BTreeMap<SchemaViewId, JazzSchema>>>,
    identity: DbIdentity,
    node: Rc<Node<S>>,
    row_id_source: Rc<RefCell<Box<dyn RowIdSource>>>,
    next_now_ms: Rc<Cell<u64>>,
}

/// Process-local, content-addressed identity for an exact typed schema view.
///
/// Unlike [`SchemaVersionId`], which identifies structural migration lineage,
/// this includes defaults and policy metadata used by a typed client facade.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SchemaViewId([u8; 32]);

impl SchemaViewId {
    /// Stable bytes suitable for binding-level map keys.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    fn for_schema(schema: &JazzSchema) -> Self {
        let bytes = postcard::to_allocvec(schema).expect("JazzSchema always serializes");
        Self(blake3::derive_key("jazz typed schema view id v1", &bytes))
    }
}

/// Shared list of live subscriptions. Held by both the `Node` and any
/// [`PeerConnection`], so an inbound sync update can push subscription events
/// through the same path a local write does.
type SubscriptionList = Rc<RefCell<Vec<Weak<RefCell<SubscriptionState>>>>>;
type PendingUpstreamCommands = Rc<RefCell<Vec<PendingUpstreamCommand>>>;
type LatestCoverageSubscriptions = Rc<RefCell<BTreeMap<CoverageKey, SubscriptionKey>>>;
type UpstreamCoverageRefCounts = Rc<RefCell<BTreeMap<CoverageKey, usize>>>;
type AwaitingInitialAuthorityCoverage = Rc<RefCell<BTreeSet<CoverageKey>>>;
type CoverageRefreshGenerations = Rc<RefCell<BTreeMap<CoverageKey, u64>>>;
type QueryCoverageRegistrations = Rc<RefCell<BTreeMap<SubscriptionKey, QueryCoverageRegistration>>>;
/// Ephemeral evidence that the current authority connection has confirmed a
/// canonical binding view. It is deliberately separate from durable
/// `settled_through`: cursors retain payload possession for repair/dedup, not
/// authority settlement.
type ActiveAuthorityViewReceipts = Rc<RefCell<Option<AuthorityViewReceipts>>>;
type UpstreamSubscriptionOwners =
    Rc<RefCell<BTreeMap<SubscriptionKey, Vec<Weak<RefCell<SubscriptionState>>>>>>;
type SharedTickScheduler = Rc<RefCell<Option<Rc<dyn TickScheduler>>>>;
type WriteStateWaiters = Rc<RefCell<BTreeMap<TxId, Vec<WriteStateWaiter>>>>;
type PermissionAdviceWaiters =
    Rc<RefCell<BTreeMap<PermissionAdviceRequestId, oneshot::Sender<PermissionAdvice>>>>;
type PendingDownstreamFates = Rc<RefCell<Vec<SyncMessage>>>;
type AdmittedUpstreamAuthorities = Rc<RefCell<Vec<AuthorityContext>>>;
const MAX_EDGE_FATE_ROUTES: usize = 1024;
const MAX_EDGE_FATE_ROUTES_PER_TX: usize = 8;

#[derive(Default)]
struct AuthorityViewReceipts {
    connection_epoch: u64,
    confirmation_floor: GlobalSeq,
    binding_views: BTreeSet<BindingViewKey>,
}

/// An inbound message held across authority selection. It preserves ordinary
/// sync delivery while ensuring traffic staged before selection cannot become
/// the selected connection's fresh view receipt.
struct StagedInboundMessage {
    message: SyncMessage,
    authority_receipt_eligible: bool,
}

struct PendingAuthorityViewUpdate {
    parts: ViewUpdateParts,
    authority_receipt_eligible: bool,
}

struct PendingBranchViewUpdate {
    message: SyncMessage,
    authority_receipt_eligible: bool,
}

struct EdgeFateRoute {
    authority: Option<AuthorityContext>,
    queue: Weak<RefCell<Vec<SyncMessage>>>,
}
type EdgeFateRoutes = Rc<RefCell<BTreeMap<TxId, Vec<EdgeFateRoute>>>>;

struct LocalFateRoute {
    queue: Weak<RefCell<Vec<SyncMessage>>>,
    local_acknowledged: bool,
}
type LocalFateRoutes = Rc<RefCell<BTreeMap<TxId, Vec<LocalFateRoute>>>>;

fn register_local_fate_route(
    routes: &LocalFateRoutes,
    tx_id: TxId,
    queue: &PendingDownstreamFates,
) {
    let mut routes = routes.borrow_mut();
    routes.retain(|_, pending| {
        pending.retain(|candidate| candidate.queue.upgrade().is_some());
        !pending.is_empty()
    });
    if routes.get(&tx_id).is_some_and(|pending| {
        pending.iter().any(|candidate| {
            candidate
                .queue
                .upgrade()
                .is_some_and(|candidate| Rc::ptr_eq(&candidate, queue))
        })
    }) {
        return;
    }
    routes.entry(tx_id).or_default().push(LocalFateRoute {
        queue: Rc::downgrade(queue),
        local_acknowledged: false,
    });
}

fn queue_local_acknowledgements<S>(routes: &LocalFateRoutes, node: &Rc<RefCell<NodeState<S>>>)
where
    S: OrderedKvStorage,
{
    let mut routes = routes.borrow_mut();
    let mut node = node.borrow_mut();
    routes.retain(|tx_id, pending| {
        let locally_durable = node
            .transaction_state(*tx_id)
            .is_some_and(|(_, _, durability)| durability >= DurabilityTier::Local);
        pending.retain_mut(|route| {
            let Some(queue) = route.queue.upgrade() else {
                return false;
            };
            if locally_durable && !route.local_acknowledged {
                queue.borrow_mut().push(SyncMessage::FateUpdate {
                    tx_id: *tx_id,
                    fate: Fate::Pending,
                    global_seq: None,
                    durability: Some(DurabilityTier::Local),
                });
                route.local_acknowledged = true;
            }
            true
        });
        !pending.is_empty()
    });
}

fn route_local_fate(routes: &LocalFateRoutes, tx_id: TxId, fate: &SyncMessage) {
    let terminal = matches!(
        fate,
        SyncMessage::FateUpdate {
            fate: Fate::Rejected(_),
            ..
        } | SyncMessage::FateUpdate {
            durability: Some(DurabilityTier::Global),
            ..
        }
    );
    let mut routes = routes.borrow_mut();
    let Some(pending) = routes.get_mut(&tx_id) else {
        return;
    };
    pending.retain(|candidate| {
        let Some(queue) = candidate.queue.upgrade() else {
            return false;
        };
        queue.borrow_mut().push(fate.clone());
        !terminal
    });
    if pending.is_empty() {
        routes.remove(&tx_id);
    }
}

fn collect_local_replay_commit_units<S>(
    node: &mut NodeState<S>,
    tx_id: TxId,
    visited: &mut BTreeSet<TxId>,
    units: &mut Vec<(TxId, SyncMessage)>,
) -> Result<(), Error>
where
    S: OrderedKvStorage,
{
    if !visited.insert(tx_id) {
        return Ok(());
    }
    let unit = node.commit_unit_for(tx_id)?;
    let SyncMessage::CommitUnit { versions, .. } = &unit else {
        unreachable!("commit_unit_for always returns a commit unit")
    };
    let parents = versions
        .iter()
        .flat_map(crate::protocol::VersionRecord::parents)
        .collect::<BTreeSet<_>>();
    for parent in parents {
        collect_local_replay_commit_units(node, parent, visited, units)?;
    }
    units.push((tx_id, unit));
    Ok(())
}

/// A parked fate either awaits its first admitted upstream or belongs to one
/// admitted upstream epoch. Drop routes for departed/replaced sessions (and
/// dead subscriber queues) eagerly: retaining a weak queue alone would let
/// arbitrary uploads grow this registry forever.
fn prune_edge_fate_routes(
    routes: &mut BTreeMap<TxId, Vec<EdgeFateRoute>>,
    admitted: Option<AuthorityContext>,
) {
    routes.retain(|_, pending| {
        pending.retain(|route| {
            route.queue.upgrade().is_some()
                && match (route.authority, admitted) {
                    (None, _) => true,
                    (Some(route), Some(admitted)) => admitted.same_admitted_link(route),
                    (Some(_), None) => false,
                }
        });
        !pending.is_empty()
    });
}
type SharedMutationErrors = Rc<RefCell<MutationErrorState>>;
type ShapeRegistrationKey = (ShapeId, ReadViewKey);

/// Per-subscriber state for a shape/read-view registration.
///
/// A missing runtime shape is ambiguous: catalogue admission is temporary,
/// whereas a capability rejection is permanent for this connection. Keep the
/// distinction at the protocol boundary instead of inferring either from the
/// node's registered-shape map.
#[derive(Clone)]
enum SubscriberShapeRegistration {
    Registered(RegisterShapeOptions),
    PendingCatalogueAdmission(RegisterShapeOptions),
    RejectedUnsupportedCapability(String),
}

fn default_cell_for_column_type(column_type: &GrooveColumnType, default: &Value) -> Value {
    match (column_type, default) {
        (GrooveColumnType::Nullable(_), Value::Nullable(_)) => default.clone(),
        (GrooveColumnType::Nullable(_), default) => {
            Value::Nullable(Some(Box::new(default.clone())))
        }
        _ => default.clone(),
    }
}

fn schema_view_column_default(column: &crate::schema::ColumnSchema) -> Result<Value, Error> {
    if let Some(default) = &column.default {
        return Ok(default.clone());
    }
    if matches!(column.column_type, GrooveColumnType::Nullable(_)) {
        return Ok(Value::Nullable(None));
    }
    Err(Error::new(
        ErrorCode::Schema,
        format!(
            "schema view column {} requires a migration default",
            column.name
        ),
    ))
}

fn direct_schema_view_lens(
    source: &JazzSchema,
    target: &JazzSchema,
) -> Result<(MigrationLens, Vec<String>, Vec<String>), Error> {
    let source_id = source.version_id();
    let target_id = target.version_id();
    let mut table_lenses = Vec::new();
    for source_table in &source.tables {
        let Some(target_table) = target
            .tables
            .iter()
            .find(|table| table.name == source_table.name)
        else {
            continue;
        };
        if source_table.references != target_table.references {
            return Err(Error::new(
                ErrorCode::Schema,
                format!(
                    "schema view changes references on {} without an explicit lens",
                    target_table.name
                ),
            ));
        }
        if source_table.merge_strategies != target_table.merge_strategies {
            return Err(Error::new(
                ErrorCode::Schema,
                format!(
                    "schema view changes merge strategies on {} without an explicit lens",
                    target_table.name
                ),
            ));
        }
        if source_table.indexed_columns != target_table.indexed_columns {
            return Err(Error::new(
                ErrorCode::Schema,
                format!(
                    "schema view changes indices on {} without explicit index admission",
                    target_table.name
                ),
            ));
        }
        let mut ops = Vec::new();
        for target_column in &target_table.columns {
            match source_table
                .columns
                .iter()
                .find(|column| column.name == target_column.name)
            {
                Some(source_column) if source_column.column_type == target_column.column_type => {}
                Some(_) => {
                    return Err(Error::new(
                        ErrorCode::Schema,
                        format!(
                            "schema view changes type of {}.{} without an explicit lens",
                            target_table.name, target_column.name
                        ),
                    ));
                }
                None => ops.push(LensOp::AddColumn {
                    column: target_column.name.clone(),
                    default: schema_view_column_default(target_column)?,
                }),
            }
        }
        for source_column in &source_table.columns {
            if target_table
                .columns
                .iter()
                .all(|column| column.name != source_column.name)
            {
                ops.push(LensOp::DropColumn {
                    column: source_column.name.clone(),
                    backwards_default: schema_view_column_default(source_column)?,
                });
            }
        }
        table_lenses.push(TableLens {
            source_table: source_table.name.clone(),
            target_table: target_table.name.clone(),
            ops,
        });
    }
    let new_tables = target
        .tables
        .iter()
        .filter(|table| source.tables.iter().all(|source| source.name != table.name))
        .map(|table| table.name.clone())
        .collect::<Vec<_>>();
    let dropped_tables = source
        .tables
        .iter()
        .filter(|table| target.tables.iter().all(|target| target.name != table.name))
        .map(|table| table.name.clone())
        .collect::<Vec<_>>();
    Ok((
        MigrationLens::new(source_id, target_id, table_lenses),
        new_tables,
        dropped_tables,
    ))
}

struct WriteStateWaiter {
    id: u64,
    notify: WriteStateWaiterNotify,
}

enum WriteStateWaiterNotify {
    Future(oneshot::Sender<()>),
    Callback(Box<dyn FnOnce()>),
}

#[derive(Default)]
struct MutationErrorState {
    callback: Option<MutationErrorCallback>,
    pending: BTreeMap<TxId, MutationErrorEvent>,
}

#[derive(Clone)]
enum PendingUpstreamCommand {
    Subscribe(PendingUpstreamSubscription),
    Unsubscribe(SubscriptionKey),
    AuthorizationScopeIntent {
        request_id: PermissionAdviceRequestId,
        action: PermissionAdviceAction,
    },
}

#[derive(Clone)]
struct PendingUpstreamSubscription {
    subscription: SubscriptionKey,
    shape: ValidatedQuery,
    binding: Binding,
    opts: RegisterShapeOptions,
    identity: AuthorId,
}

struct QueryCoverageRegistration {
    coverage: CoverageKey,
    subscription: PendingUpstreamSubscription,
    owns_subscription: bool,
    ref_count: usize,
}

#[derive(Clone)]
struct UpstreamCoverageHandle {
    coverage: CoverageKey,
    subscription: SubscriptionKey,
}

struct OpenedUpstreamCoverage {
    handles: Vec<UpstreamCoverageHandle>,
    awaits_initial_authority_response: bool,
}

struct CoverageGroup {
    shape: ValidatedQuery,
    binding: Binding,
    subscribers: BTreeSet<SubscriptionKey>,
    upstream_subscription: SubscriptionKey,
    upstream_opts: RegisterShapeOptions,
    awaiting_upstream_settlement: bool,
}

/// Authority-derived scope identity retained for a support subscription.
/// Never constructed from the caller's wire payload.
#[derive(Clone, Debug, PartialEq)]
struct AuthorizedScopePurpose {
    key: crate::protocol::AuthorizationSupportScopeKey,
    operation: crate::protocol::AuthorizationOperationKey,
    action: PermissionAdviceAction,
    expected_support: BTreeSet<(ShapeId, BindingId)>,
}

// Compatibility spelling retained for module-local tests while the actual
// implementation is the shared authority proof primitive.
#[cfg(test)]
type ScopeAggregate = AuthorityScopeAggregate;

/// One receipt-bound authorization operation owned by one admitted upstream.
///
/// This state deliberately lives on `ConnectionLink::Upstream`: a receipt is
/// only meaningful for the authenticated authority epoch that delivered it.
/// The manager additionally records every support subscription, since an
/// aggregate receipt names only the final completing subscription.
struct AuthorizationScopeLeaseRequest {
    action: PermissionAdviceAction,
    /// Every local caller sharing this authority hydration.  The first id is
    /// the wire correlation id; later ids never cause another support view.
    waiters: BTreeSet<PermissionAdviceRequestId>,
    key: Option<crate::protocol::AuthorizationSupportScopeKey>,
    lease: Option<AuthorizationScopeLease>,
    owner: Option<AuthorizationScopeOwnerToken>,
    clause_count: Option<u16>,
    applied_clauses: BTreeMap<u16, (SubscriptionKey, crate::time::GlobalSeq, u64)>,
}

/// Per-upstream admission manager for scope receipts and their retained leases.
#[derive(Default)]
struct AuthorizationScopeLeaseManager {
    registry: AuthorizationScopeRegistry,
    requests: BTreeMap<PermissionAdviceRequestId, AuthorizationScopeLeaseRequest>,
}

/// One authority-compiled support hydration retained only while every
/// authority revision and global cut it represents remains current.  It is
/// keyed by the support scope rather than the candidate operation, so distinct
/// rows/patches can reuse hydration but still evaluate their own action.
#[derive(Clone)]
struct ServedAuthorizationScopeHydration {
    clauses: Vec<ServedAuthorizationScopeClause>,
    receipt: AuthorizationScopeReceipt,
}

#[derive(Clone)]
struct ServedAuthorizationScopeClause {
    subscription: SubscriptionKey,
    register: SyncMessage,
    subscribe: SyncMessage,
    view: SyncMessage,
}

/// Locally-authored transactions awaiting upload, oldest first. Shared with
/// upstream [`PeerConnection`]s, each of which tracks how far it has shipped.
type Outbox = Rc<RefCell<Vec<PendingUpload>>>;

#[derive(Clone)]
struct PendingUpload {
    tx_id: TxId,
    unit: Option<SyncMessage>,
}

/// Application-visible fate and durability for a local write transaction.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct WriteState {
    /// Latest authority fate observed by this `Db`.
    pub fate: Fate,
    /// Highest durability tier observed by this `Db`.
    pub durability: DurabilityTier,
}

/// Explicit client durability cadence while the first server snapshot is
/// loading.
///
/// A crash can lose up to `M - 1` writes since the last completed boundary,
/// where `M` is this value. Older completed boundaries recover from the
/// storage WAL. The final partial initial-sync batch is always flushed before
/// the client returns to per-write durability.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InitialSyncFlushCadence(NonZeroUsize);

impl InitialSyncFlushCadence {
    /// The client default: a durability boundary every 512 initial-sync writes.
    pub const DEFAULT: Self = Self(NonZeroUsize::new(512).expect("non-zero"));

    /// Create a cadence with one durability boundary per `writes` initial-sync
    /// writes.
    pub const fn every(writes: NonZeroUsize) -> Self {
        Self(writes)
    }

    /// Number of writes between completed durability boundaries.
    pub const fn writes(self) -> usize {
        self.0.get()
    }
}

/// Usage-site query coverage attachment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryAttachment {
    subscriptions: Vec<SubscriptionKey>,
    required_after: Vec<(BindingViewKey, u64)>,
    /// Edge/Global coverage is live authority evidence, not merely a newer
    /// durable view generation.
    requires_current_authority_receipt: bool,
    registrations: Vec<SubscriptionKey>,
    refreshes: Vec<(CoverageKey, u64)>,
}

impl QueryAttachment {
    /// Wire subscription id owned by this attachment.
    pub fn subscription(&self) -> SubscriptionKey {
        self.subscriptions[0]
    }
}

/// Future that resolves when a database observes a write-state change.
///
/// This is a wake primitive: callers should read [`Db::write_state`] before
/// registering it, read again after registration, and then re-read after it
/// resolves.
pub struct WriteStateChange {
    waiters: WriteStateWaiters,
    tx_id: TxId,
    waiter_id: u64,
    receiver: oneshot::Receiver<()>,
}

impl Future for WriteStateChange {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.receiver).poll(cx) {
            Poll::Ready(_) => Poll::Ready(()),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for WriteStateChange {
    fn drop(&mut self) {
        let mut waiters = self.waiters.borrow_mut();
        let Some(tx_waiters) = waiters.get_mut(&self.tx_id) else {
            return;
        };
        tx_waiters.retain(|waiter| waiter.id != self.waiter_id);
        let empty = tx_waiters.is_empty();
        if empty {
            waiters.remove(&self.tx_id);
        }
    }
}

/// Cancel-safe future for one authoritative permission preflight.
pub struct PermissionAdviceFuture {
    waiters: PermissionAdviceWaiters,
    request_id: PermissionAdviceRequestId,
    receiver: oneshot::Receiver<PermissionAdvice>,
}

impl Future for PermissionAdviceFuture {
    type Output = PermissionAdvice;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.receiver).poll(cx) {
            Poll::Ready(Ok(advice)) => Poll::Ready(advice),
            Poll::Ready(Err(_)) => Poll::Ready(PermissionAdvice::Unknown),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl PermissionAdviceFuture {
    /// Opaque id used to cancel only this request.
    pub fn request_id(&self) -> PermissionAdviceRequestId {
        self.request_id
    }
}

impl Drop for PermissionAdviceFuture {
    fn drop(&mut self) {
        self.waiters.borrow_mut().remove(&self.request_id);
    }
}

impl<S> Db<S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    /// Open a database over the supplied storage and recover local state.
    ///
    /// ```rust
    /// # use jazz::db::{Db, DbConfig, DbIdentity, SeededRowIdSource};
    /// # use jazz::db::doctest_support::{block_on, schema, MemoryStorage};
    /// # use jazz::ids::{AuthorId, NodeUuid};
    /// let schema = schema();
    /// let column_families = schema.column_families();
    /// let refs = column_families.iter().map(String::as_str).collect::<Vec<_>>();
    /// let storage = MemoryStorage::new(&refs);
    ///
    /// let db = block_on(Db::open(DbConfig {
    ///     schema,
    ///     storage,
    ///     identity: DbIdentity {
    ///         node: NodeUuid::from_bytes([1; 16]),
    ///         author: AuthorId::from_bytes([2; 16]),
    ///     },
    ///     id_source: Some(Box::new(SeededRowIdSource::new(1))),
    /// }))?;
    ///
    /// let todos = db.prepare_query(&db.table("todos"))?;
    /// assert!(db.read(&todos)?.is_empty());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn open(config: DbConfig<S>) -> Result<Self, Error> {
        let schema_version_id = config.schema.version_id();
        let schema_views = Rc::new(RefCell::new(BTreeMap::from([(
            SchemaViewId::for_schema(&config.schema),
            config.schema.clone(),
        )])));
        let node = NodeState::new(config.identity.node, config.schema.clone(), config.storage)?;
        let node = Node::new(node);
        node.restore_pending_uploads(config.identity)?;
        Ok(Self {
            schema: config.schema,
            schema_version_id,
            schema_view_is_fixed: false,
            schema_views,
            identity: config.identity,
            node: Rc::new(node),
            row_id_source: Rc::new(RefCell::new(
                config
                    .id_source
                    .unwrap_or_else(|| Box::new(ProductionRowIdSource)),
            )),
            next_now_ms: Rc::new(Cell::new(1)),
        })
    }

    #[cfg(feature = "testing")]
    /// Open a database and return internal node-open phase timings for benchmarks.
    pub async fn open_with_receipt_for_test(
        config: DbConfig<S>,
    ) -> Result<(Self, DbOpenReceipt), Error> {
        let schema_version_id = config.schema.version_id();
        let schema_views = Rc::new(RefCell::new(BTreeMap::from([(
            SchemaViewId::for_schema(&config.schema),
            config.schema.clone(),
        )])));
        let (node, receipt) = NodeState::new_with_open_receipt_for_test(
            config.identity.node,
            config.schema.clone(),
            config.storage,
            false,
        )?;
        let db = Self {
            schema: config.schema,
            schema_version_id,
            schema_view_is_fixed: false,
            schema_views,
            identity: config.identity,
            node: Rc::new(Node::new(node)),
            row_id_source: Rc::new(RefCell::new(
                config
                    .id_source
                    .unwrap_or_else(|| Box::new(ProductionRowIdSource)),
            )),
            next_now_ms: Rc::new(Cell::new(1)),
        };
        Ok((db, receipt))
    }

    /// Open a database as a history-complete serving core.
    ///
    /// This mode is intended for server shells and tests that own authoritative
    /// in-memory history rather than a partial client replica.
    pub async fn open_history_complete(config: DbConfig<S>) -> Result<Self, Error> {
        let schema_version_id = config.schema.version_id();
        let schema_views = Rc::new(RefCell::new(BTreeMap::from([(
            SchemaViewId::for_schema(&config.schema),
            config.schema.clone(),
        )])));
        let node = NodeState::new_history_complete(
            config.identity.node,
            config.schema.clone(),
            config.storage,
        )?;
        Ok(Self {
            schema: config.schema,
            schema_version_id,
            schema_view_is_fixed: false,
            schema_views,
            identity: config.identity,
            node: Rc::new(Node::new(node)),
            row_id_source: Rc::new(RefCell::new(
                config
                    .id_source
                    .unwrap_or_else(|| Box::new(ProductionRowIdSource)),
            )),
            next_now_ms: Rc::new(Cell::new(1)),
        })
    }

    /// Open an edge whose durable store has no authority catalogue yet.
    ///
    /// This is deliberately narrower than [`Db::open`]: callers may only use
    /// it to receive one connection-authenticated catalogue snapshot and then
    /// select one of the snapshot's admitted schema views.  Until then the
    /// node has no application schema and rejects ordinary data/sync work.
    pub(crate) async fn open_catalogue_uninitialized_edge(
        config: DbConfig<S>,
    ) -> Result<Self, Error> {
        let bootstrap_schema = JazzSchema::new([]);
        let schema_version_id = bootstrap_schema.version_id();
        let schema_views = Rc::new(RefCell::new(BTreeMap::from([(
            SchemaViewId::for_schema(&bootstrap_schema),
            bootstrap_schema.clone(),
        )])));
        let node = NodeState::new_catalogue_uninitialized(config.identity.node, config.storage)?;
        let node = Node::new(node);
        node.restore_pending_uploads(config.identity)?;
        Ok(Self {
            schema: bootstrap_schema,
            schema_version_id,
            schema_view_is_fixed: false,
            schema_views,
            identity: config.identity,
            node: Rc::new(node),
            row_id_source: Rc::new(RefCell::new(
                config
                    .id_source
                    .unwrap_or_else(|| Box::new(ProductionRowIdSource)),
            )),
            next_now_ms: Rc::new(Cell::new(1)),
        })
    }

    /// Install a complete catalogue received over the authenticated upstream
    /// bootstrap link.  This is intentionally crate-private: ordinary wire
    /// dispatch must never turn an arbitrary peer's snapshot into authority.
    pub(crate) fn apply_trusted_catalogue_snapshot(
        &self,
        snapshot: crate::protocol::CatalogueSnapshot,
    ) -> Result<(), Error> {
        Ok(self
            .node
            .node
            .borrow_mut()
            .apply_trusted_catalogue_snapshot(snapshot)?)
    }

    #[cfg(test)]
    pub(crate) fn set_catalogue_activation_failpoint(
        &self,
        failpoint: crate::node::CatalogueActivationFailpoint,
    ) {
        self.node
            .node
            .borrow_mut()
            .set_catalogue_activation_failpoint(failpoint);
    }

    /// Produce the authority's complete catalogue for the privileged
    /// snapshot-only transport exchange.
    pub(crate) fn trusted_catalogue_snapshot(
        &self,
    ) -> Result<crate::protocol::CatalogueSnapshot, Error> {
        Ok(self.node.node.borrow().catalogue_snapshot()?)
    }

    /// Return the active authority-admitted schema, failing closed when this
    /// dynamic edge still has no bootstrap receipt.
    pub(crate) fn trusted_current_catalogue_schema(&self) -> Result<JazzSchema, Error> {
        let node = self.node.node.borrow();
        let pointer = node.current_write_schema()?;
        node.catalogue_schemas()
            .get(&pointer.schema)
            .map(|schema| schema.schema.clone())
            .ok_or_else(|| Error::new(ErrorCode::Schema, "active catalogue schema is missing"))
    }

    pub(crate) fn catalogue_bootstrap_is_ready(&self) -> bool {
        self.node.node.borrow().catalogue_bootstrap_state()
            == crate::node::CatalogueBootstrapState::Ready
    }

    /// Register a typed schema view on this database owner.
    ///
    /// Registration is process-local and idempotent by the exact typed schema
    /// content. It does not publish a catalogue entry or select the current
    /// write schema. The returned handle shares the owner's node, open batches,
    /// connections, row-id source, and logical clock while validating typed
    /// operations against this exact schema view.
    pub fn register_schema_view(&self, schema: JazzSchema) -> Result<Self, Error> {
        let schema_version_id = schema.version_id();
        let schema_view_id = SchemaViewId::for_schema(&schema);
        self.admit_local_schema_view_if_needed(&schema)?;
        {
            let node = self.node.node.borrow();
            let admitted = node
                .catalogue_schemas()
                .get(&schema_version_id)
                .ok_or_else(|| Error::new(ErrorCode::Schema, "registered schema is missing"))?;
            if !schema_policy_metadata_matches(&admitted.schema, &schema) {
                return Err(Error::new(
                    ErrorCode::Schema,
                    "schema view policy metadata conflicts with its admitted structural schema",
                ));
            }
            if !schema_index_metadata_matches(&admitted.schema, &schema) {
                return Err(Error::new(
                    ErrorCode::Schema,
                    "schema view index metadata conflicts with its admitted structural schema",
                ));
            }
        }
        let mut views = self.schema_views.borrow_mut();
        if let Some(existing) = views.get(&schema_view_id) {
            if existing != &schema {
                return Err(Error::new(
                    ErrorCode::Schema,
                    format!("schema view id collision for {schema_view_id:?}"),
                ));
            }
        } else {
            views.insert(schema_view_id, schema.clone());
        }
        drop(views);
        Ok(Self {
            schema,
            schema_version_id,
            schema_view_is_fixed: true,
            schema_views: Rc::clone(&self.schema_views),
            identity: self.identity,
            node: Rc::clone(&self.node),
            row_id_source: Rc::clone(&self.row_id_source),
            next_now_ms: Rc::clone(&self.next_now_ms),
        })
    }

    /// Attach an already-registered typed schema view to this owner.
    pub fn schema_view(&self, schema_view_id: SchemaViewId) -> Result<Self, Error> {
        let schema = self
            .schema_views
            .borrow()
            .get(&schema_view_id)
            .cloned()
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::Schema,
                    format!("schema view {schema_view_id:?} is not registered"),
                )
            })?;
        self.register_schema_view(schema)
    }

    /// Canonical id of this handle's typed schema view.
    pub fn schema_view_id(&self) -> SchemaViewId {
        SchemaViewId::for_schema(&self.schema)
    }

    /// Admit the first application schema into an owner deliberately opened
    /// with the empty schema. This is the local-first bootstrap equivalent of
    /// having opened the runtime with that schema originally; later schemas
    /// still arrive through ordinary catalogue lineage publication.
    fn admit_local_schema_view_if_needed(&self, schema: &JazzSchema) -> Result<(), Error> {
        let empty_schema = JazzSchema::new([]);
        let empty_id = empty_schema.version_id();
        let target_id = schema.version_id();
        let (source, catalogue_seq, bootstrap_current) = {
            let node = self.node.node.borrow();
            if node.catalogue_schemas().contains_key(&target_id) {
                return Ok(());
            }
            let current = node.current_write_schema().map_err(Error::from)?;
            let source = node
                .catalogue_schemas()
                .get(&current.schema)
                .map(|version| version.schema.clone())
                .ok_or_else(|| Error::new(ErrorCode::Schema, "current schema view is missing"))?;
            (
                source,
                node.active_catalogue_seq().saturating_add(1),
                current.schema == empty_id && node.catalogue_schemas().len() == 1,
            )
        };
        let (lens, new_tables, dropped_tables) = direct_schema_view_lens(&source, schema)?;
        let publication = SchemaLineagePublication::new(
            SchemaVersion::new(schema.clone()),
            lens,
            new_tables,
            dropped_tables,
        );
        let mut node = self.node.node.borrow_mut();
        node.apply_trusted_catalogue_message(SyncMessage::PublishSchemaWithLens {
            author: AuthorId::SYSTEM,
            catalogue_seq,
            publication: Box::new(publication),
        })?;
        if bootstrap_current {
            node.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
                author: AuthorId::SYSTEM,
                pointer: CurrentWriteSchema {
                    revision: 1,
                    schema: target_id,
                },
            })?;
        }
        Ok(())
    }

    /// Flush node-local maintenance state, write a clean-close marker, and
    /// close the underlying storage.
    pub fn close(&self) -> Result<(), Error> {
        if self.schema_view_is_fixed {
            return Ok(());
        }
        Ok(self.node.node.borrow_mut().close()?)
    }

    /// Configure this database as the optimistic, non-durable side of a
    /// browser client/worker pair. This must be called before application
    /// writes begin.
    pub fn set_non_durable_client(&self) {
        self.node.set_non_durable_client();
    }

    /// Configure this client database's first-snapshot durability cadence.
    ///
    /// Servers do not call this client-only setting and retain their existing
    /// storage durability behavior.
    pub fn set_initial_sync_flush_cadence(
        &self,
        cadence: InitialSyncFlushCadence,
    ) -> Result<(), Error> {
        Ok(self
            .node
            .node
            .borrow_mut()
            .set_initial_sync_flush_cadence(cadence.writes())?)
    }

    /// Create a snapshot-base branch immediately in local durable storage.
    ///
    /// Branch creation is local-first: no serving node round trip is required.
    /// The authenticated database identity is recorded as the immutable creator.
    pub fn create_branch(&self) -> Result<crate::ids::BranchId, Error> {
        let branch = crate::ids::BranchId(uuid::Uuid::now_v7());
        self.create_branch_with_id(branch)?;
        Ok(branch)
    }

    /// Create a local snapshot-base branch with a caller-supplied stable id.
    pub fn create_branch_with_id(&self, branch: crate::ids::BranchId) -> Result<(), Error> {
        self.node
            .node
            .borrow_mut()
            .create_branch_as(branch, self.identity.author)?;
        Ok(())
    }

    /// Insert a row into a locally-created branch and queue it for ordinary sync.
    pub fn insert_on_branch(
        &self,
        branch: crate::ids::BranchId,
        table: &str,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        let row = self.row_id_source.borrow_mut().next_row_id();
        let cells = self.apply_insert_defaults(table, cells)?;
        let tx_id = self
            .node
            .node
            .borrow_mut()
            .commit_mergeable_on_branch_in_schema(
                branch,
                self.schema_version_id,
                MergeableCommit::new(table, row, self.next_now_ms())
                    .made_by(self.identity.author)
                    .cells(cells),
            )?;
        let local_tier = self.finalize_local_commit(tx_id)?;
        self.refresh_subscriptions()?;
        Ok(WriteHandle {
            node: Rc::downgrade(&self.node.node),
            row_uuid: row,
            tx_id,
            local_tier,
        })
    }

    /// Seed a settled mergeable row for server bootstrap/import flows.
    ///
    /// This bypasses the client pending-upload path and immediately finalizes
    /// the commit in local history. It is intended only for history-complete
    /// server bootstrap/import state, not for general application writes or
    /// pending client write semantics.
    pub fn seed_settled_mergeable_for_bootstrap(
        &self,
        table: &str,
        row: RowUuid,
        made_by: AuthorId,
        cells: RowCells,
    ) -> Result<TxId, Error> {
        let cells = self.apply_insert_defaults(table, cells)?;
        let tx_id = self.node.node.borrow_mut().commit_mergeable_in_schema(
            self.schema_version_id,
            MergeableCommit::new(table, row, self.next_now_ms())
                .made_by(made_by)
                .cells(cells),
        )?;
        self.node
            .node
            .borrow_mut()
            .finalize_local_mergeable_commit(tx_id)?;
        self.refresh_subscriptions()?;
        self.node.mark_subscriber_connections_dirty();
        Ok(tx_id)
    }

    /// Seed a branch-local mergeable row for history-complete server bootstrap
    /// or import flows.
    ///
    /// The resulting row is evaluated through the ordinary branch read-view
    /// lowering path; this does not provide an application-facing branch write
    /// facade.
    pub fn seed_branch_mergeable_for_bootstrap(
        &self,
        branch: crate::ids::BranchId,
        table: &str,
        row: RowUuid,
        made_by: AuthorId,
        cells: RowCells,
    ) -> Result<TxId, Error> {
        let cells = self.apply_insert_defaults(table, cells)?;
        let mut node = self.node.node.borrow_mut();
        if node.branch_record(branch).is_none() {
            node.create_branch(branch)?;
        }
        let tx_id = node.commit_mergeable_on_branch_in_schema(
            branch,
            self.schema_version_id,
            MergeableCommit::new(table, row, self.next_now_ms())
                .made_by(made_by)
                .cells(cells),
        )?;
        node.finalize_local_mergeable_commit(tx_id)?;
        drop(node);
        self.refresh_subscriptions()?;
        self.node.mark_subscriber_connections_dirty();
        Ok(tx_id)
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only authority finalization for a locally committed mergeable
    /// transaction.
    ///
    /// This allows scale fixtures to use the ordinary batched transaction API
    /// before performing the same self-acceptance step as
    /// [`Db::seed_settled_mergeable_for_bootstrap`].
    pub fn finalize_local_mergeable_commit_for_test(&self, tx_id: TxId) -> Result<(), Error> {
        self.node
            .node
            .borrow_mut()
            .finalize_local_mergeable_commit(tx_id)?;
        self.refresh_subscriptions()?;
        self.node.mark_subscriber_connections_dirty();
        Ok(())
    }

    /// Return the locally observed fate and durability for a write transaction.
    pub fn write_state(&self, tx_id: TxId) -> Result<WriteState, Error> {
        let Some((fate, _, durability)) = self.node.node.borrow_mut().transaction_state(tx_id)
        else {
            return Err(Error::new(
                ErrorCode::NotObserved,
                "transaction is not known locally",
            ));
        };
        Ok(WriteState { fate, durability })
    }

    /// Wait until `tx_id` reaches `tier` or is rejected.
    ///
    /// An explicit wait consumes a rejection, preventing the same failure from
    /// subsequently being delivered through [`Db::on_mutation_error`]. The
    /// check/register/recheck sequence keeps that ownership decision inside
    /// the database and closes the race with an already-observed rejection.
    pub async fn wait_for_transaction(
        &self,
        tx_id: TxId,
        tier: DurabilityTier,
    ) -> Result<TxId, Error> {
        loop {
            if let Some(outcome) = self.node.transaction_wait_outcome(tx_id, tier) {
                return outcome;
            }
            let state_change = self.node.register_write_state_waiter(tx_id);
            if let Some(outcome) = self.node.transaction_wait_outcome(tx_id, tier) {
                drop(state_change);
                return outcome;
            }
            state_change.await;
        }
    }

    /// Callback form of [`Db::wait_for_transaction`] for bindings that cannot
    /// drive a thread-affine Rust future directly.
    pub fn wait_for_transaction_with(
        &self,
        tx_id: TxId,
        tier: DurabilityTier,
        callback: impl FnOnce(Result<TxId, Error>) + 'static,
    ) {
        self.node
            .wait_for_transaction_with(tx_id, tier, Box::new(callback));
    }

    /// Wait until this database observes another state transition for `tx_id`.
    ///
    /// Callers should always check [`Db::write_state`] before and after
    /// registering this future; this method is a wake primitive, not a predicate.
    pub fn next_write_state_change(&self, tx_id: TxId) -> WriteStateChange {
        self.node.register_write_state_waiter(tx_id)
    }

    /// Register the binding callback for rejected local transactions that no
    /// active application waiter consumed.
    pub fn on_mutation_error(&self, callback: MutationErrorCallback) {
        self.node.set_mutation_error_callback(Some(callback));
    }

    /// Remove the current mutation-error callback.
    pub fn clear_mutation_error_callback(&self) {
        self.node.set_mutation_error_callback(None);
    }

    /// Start a query rooted at `table`.
    ///
    /// ```rust
    /// # use jazz::db::doctest_support::{block_on, open_todos_db};
    /// # use jazz::query::{col, eq, lit};
    /// let db = block_on(open_todos_db())?;
    /// let open_todos = db
    ///     .table("todos")
    ///     .filter(eq(col("done"), lit(false)))
    ///     .select(["title", "done"]);
    ///
    /// let open_todos = db.prepare_query(&open_todos)?;
    /// assert!(db.read(&open_todos)?.is_empty());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn table(&self, table: impl Into<String>) -> Query {
        Query::from(table)
    }

    /// Prepare a query for repeated reads or subscriptions.
    ///
    /// ```rust
    /// # use jazz::db::doctest_support::{block_on, open_todos_db, todo_cells};
    /// let db = block_on(open_todos_db())?;
    /// let write = db.insert("todos", todo_cells("write docs", false))?;
    /// let todo = write.row_uuid();
    ///
    /// let query = db.prepare_query(&db.table("todos"))?;
    /// let rows = db.read(&query)?;
    /// assert_eq!(rows.len(), 1);
    /// assert_eq!(rows[0].row_uuid(), todo);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn prepare_query(&self, query: &Query) -> Result<PreparedQuery, Error> {
        self.prepare_query_bound(query, BTreeMap::new())
    }

    /// Prepare a query with explicit parameter bindings.
    pub fn prepare_query_bound(
        &self,
        query: &Query,
        params: BTreeMap<String, Value>,
    ) -> Result<PreparedQuery, Error> {
        let (schema, schema_version) = self.current_write_schema_for_query()?;
        self.prepare_query_bound_for_schema(query, params, &schema, schema_version)
    }

    /// Prepare a query against the schema this database handle was opened with.
    ///
    /// Typed client facades are pinned to that schema even when a catalogue
    /// snapshot advances or rolls back the separate current-write pointer.
    #[cfg(feature = "client")]
    pub(crate) fn prepare_query_for_open_schema(
        &self,
        query: &Query,
    ) -> Result<PreparedQuery, Error> {
        self.prepare_query_bound_for_schema(
            query,
            BTreeMap::new(),
            &self.schema,
            self.schema_version_id,
        )
    }

    fn prepare_query_bound_for_schema(
        &self,
        query: &Query,
        params: BTreeMap<String, Value>,
        schema: &JazzSchema,
        schema_version: SchemaVersionId,
    ) -> Result<PreparedQuery, Error> {
        let shape = query.validate_with_schema_version(schema, schema_version)?;
        let binding = shape.bind(params)?;
        let (local_plan, global_plan) = if should_install_prepared_plan(&shape)
            && !self.node.node.borrow().uses_schema_projected_read(&shape)
        {
            let mut node = self.node.node.borrow_mut();
            (
                Some(node.prepared_query_plan(
                    &shape,
                    &binding,
                    DurabilityTier::Local,
                    AuthorId::SYSTEM,
                )?),
                Some(node.prepared_query_plan(
                    &shape,
                    &binding,
                    DurabilityTier::Global,
                    AuthorId::SYSTEM,
                )?),
            )
        } else {
            (None, None)
        };
        let groove_runtime_token = self.node.node.borrow().groove_runtime_token();
        Ok(PreparedQuery {
            shape,
            binding,
            local_plan,
            global_plan,
            groove_runtime_token,
        })
    }

    /// Synchronously read rows for a prepared query.
    ///
    /// This is a synchronous local-preview read. Upstream/server settled
    /// coverage is tracked separately by query attachments and durability-aware
    /// subscription reads.
    pub fn read(&self, prepared: &PreparedQuery) -> Result<Vec<CurrentRow>, Error> {
        let mut node = self.node.node.borrow_mut();
        let groove_runtime_token = node.groove_runtime_token();
        node.query_rows_local_preview(
            &prepared.shape,
            &prepared.binding,
            prepared.plan_for_tier(DurabilityTier::Local, groove_runtime_token),
        )
        .map_err(Into::into)
    }

    #[cfg(any(test, feature = "testing"))]
    /// Test-only count of live Groove maintained subscriptions.
    pub fn active_groove_subscriptions_for_test(&self) -> usize {
        self.node
            .node
            .borrow()
            .runtime_stats_for_test()
            .active_subscriptions
    }

    /// Synchronously read rows and attribute work inside the node query path.
    ///
    /// The returned rows are identical to [`Self::read`]. This diagnostic
    /// variant exists so persisted-read benchmarks can locate first-read cost
    /// without adding clocks to the ordinary read path.
    pub fn read_profiled(
        &self,
        prepared: &PreparedQuery,
    ) -> Result<(Vec<CurrentRow>, QueryReadProfile), Error> {
        let mut node = self.node.node.borrow_mut();
        let groove_runtime_token = node.groove_runtime_token();
        node.query_rows_local_preview_profiled(
            &prepared.shape,
            &prepared.binding,
            prepared.plan_for_tier(DurabilityTier::Local, groove_runtime_token),
        )
        .map_err(Into::into)
    }

    /// Synchronously read exactly one local row if present.
    ///
    /// ```rust
    /// # use jazz::db::doctest_support::{block_on, open_todos_db, todo_cells};
    /// let db = block_on(open_todos_db())?;
    /// let todo = db.insert("todos", todo_cells("first item", false))?.row_uuid();
    ///
    /// let todos = db.prepare_query(&db.table("todos"))?;
    /// let found = db.one(&todos)?;
    /// assert_eq!(found.map(|row| row.row_uuid()), Some(todo));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn one(&self, prepared: &PreparedQuery) -> Result<Option<CurrentRow>, Error> {
        Ok(self.read(prepared)?.into_iter().next())
    }

    /// Resolve creator/updater provenance for a row returned by this database.
    pub fn row_provenance(&self, row: &CurrentRow) -> Result<Option<RowProvenance>, Error> {
        self.node
            .node
            .borrow_mut()
            .row_provenance(row)
            .map_err(Into::into)
    }

    /// Read local settled history at an exact global sequence cut.
    ///
    /// History-incomplete facades return `HistoricalReadRequiresServer` from
    /// the node layer instead of answering from a partial local prefix
    /// (ch11/INV-BRANCH-4).
    pub fn at(
        &self,
        position: GlobalSeq,
        prepared: &PreparedQuery,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.at_prepared(position, prepared)
    }

    fn at_prepared(
        &self,
        position: GlobalSeq,
        prepared: &PreparedQuery,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.node
            .node
            .borrow_mut()
            .at(position)
            .read(&prepared.shape, &prepared.binding)
            .map_err(Into::into)
    }

    /// Tier-gated one-shot read.
    ///
    /// ```rust
    /// # use jazz::db::{ReadOpts, LocalUpdates, Propagation};
    /// # use jazz::db::doctest_support::{block_on, open_todos_db, todo_cells};
    /// # use jazz::tx::DurabilityTier;
    /// let db = block_on(open_todos_db())?;
    /// db.insert("todos", todo_cells("visible locally", false))?;
    ///
    /// let opts = ReadOpts {
    ///     tier: DurabilityTier::Local,
    ///     local_updates: LocalUpdates::Immediate,
    ///     propagation: Propagation::LocalOnly,
    ///     include_deleted: false,
    ///     ..ReadOpts::default()
    /// };
    /// let todos = db.prepare_query(&db.table("todos"))?;
    /// let rows = block_on(db.all(&todos, opts))?;
    /// assert_eq!(rows.len(), 1);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn all(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.all_for_identity_in_authorization_mode(
            prepared,
            opts,
            self.identity.author,
            QueryAuthorizationMode::ClientLocal,
        )
        .await
    }

    /// Tier-gated one-shot read evaluated by a trusted host as `author`.
    ///
    /// Ordinary client reads use [`Db::all`] and never re-run read policy over
    /// locally received data. This explicit identity entry point is reserved
    /// for serving/request hosts that own policy enforcement before emission.
    pub async fn all_for_identity(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.all_for_identity_in_authorization_mode(
            prepared,
            opts,
            author,
            QueryAuthorizationMode::TrustedServing,
        )
        .await
    }

    async fn all_for_identity_in_authorization_mode(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorId,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<Vec<CurrentRow>, Error> {
        let tier = effective_read_tier(&opts);
        let mut node = self.node.node.borrow_mut();
        match &opts.read_view.source {
            ReadViewSourceSpec::Current => {}
            ReadViewSourceSpec::Branch { branch } if !opts.include_deleted => {
                return match authorization_mode {
                    QueryAuthorizationMode::TrustedServing => node
                        .query_rows_on_branch_for_link(
                            crate::ids::BranchId(*branch),
                            &prepared.shape,
                            &prepared.binding,
                            author,
                        )
                        .map_err(Into::into),
                    QueryAuthorizationMode::ClientLocal if tier < DurabilityTier::Edge => node
                        .query_rows_on_branch_for_client(
                            crate::ids::BranchId(*branch),
                            &prepared.shape,
                            &prepared.binding,
                            author,
                        )
                        .map_err(Into::into),
                    QueryAuthorizationMode::ClientLocal => node
                        .query_rows_for_client_read_view(
                            &prepared.shape,
                            &prepared.binding,
                            self.node
                                .upstream_register_shape_options(
                                    tier,
                                    opts.read_view.clone(),
                                    opts.propagation == Propagation::Full,
                                )
                                .tier,
                            &opts.read_view,
                        )
                        .map_err(Into::into),
                };
            }
            _ => ensure_default_read_view(&opts)?,
        }
        match (opts.include_deleted, authorization_mode) {
            (true, mode) => node.query_rows_including_deleted_in_authorization_mode(
                &prepared.shape,
                &prepared.binding,
                tier,
                None,
                author,
                mode,
            ),
            (false, QueryAuthorizationMode::TrustedServing) => node
                .query_rows_with_prepared_plan_for_identity(
                    &prepared.shape,
                    &prepared.binding,
                    tier,
                    None,
                    author,
                ),
            (false, QueryAuthorizationMode::ClientLocal) => {
                // A client consumes identity-scoped rows emitted by its
                // trusted upstream; local reads must not apply policy again.
                node.query_rows_for_client(&prepared.shape, &prepared.binding, tier, author)
            }
        }
        .map_err(Into::into)
    }

    /// Tier-gated one-shot relation read evaluated as the database identity.
    pub async fn all_relation_snapshot(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<RelationSnapshot, Error> {
        ensure_supported_read_view(&opts)?;
        if opts.include_deleted {
            return Err(Error::new(
                ErrorCode::Query,
                "relation snapshots do not support include_deleted yet",
            ));
        }
        let tier = effective_read_tier(&opts);
        self.node
            .node
            .borrow_mut()
            .query_relation_snapshot_for_client(
                &prepared.shape,
                &prepared.binding,
                tier,
                self.identity.author,
                &opts.read_view,
            )
            .map_err(Into::into)
    }

    /// Tier-gated one-shot relation read evaluated as `author`.
    pub async fn all_relation_snapshot_for_identity(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<RelationSnapshot, Error> {
        ensure_supported_read_view(&opts)?;
        if opts.include_deleted {
            return Err(Error::new(
                ErrorCode::Query,
                "relation snapshots do not support include_deleted yet",
            ));
        }
        let tier = effective_read_tier(&opts);
        self.node
            .node
            .borrow_mut()
            .query_relation_snapshot_for_serving_in_read_view(
                &prepared.shape,
                &prepared.binding,
                tier,
                author,
                &opts.read_view,
            )
            .map_err(Into::into)
    }

    /// Tier-gated canonical structured result read.
    ///
    /// This is the sole Jazz-boundary materialization of relation facts into
    /// recursive output. Wire delivery deliberately remains on its v3 carrier
    /// until the structured delivery migration.
    pub async fn all_result_tree(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<ResultTree, Error> {
        let snapshot = self.all_relation_snapshot(prepared, opts).await?;
        materialize_result_tree(prepared.shape.query(), snapshot)
    }

    /// Tier-gated one-shot output-changing relation read evaluated as the database identity.
    pub async fn all_relation_query(
        &self,
        query: &RelationQuery,
        opts: ReadOpts,
    ) -> Result<RelationSnapshot, Error> {
        ensure_default_read_view(&opts)?;
        let query = relation_query_to_query(query)?;
        let prepared = self.prepare_query(&query)?;
        // Output-changing relation queries currently normalize to a single
        // root row set. They have no array payload edges, so request ordinary
        // app rows instead of the relation-snapshot fact output (which is
        // reserved for correlated array/path materialization).
        let rows = self.all(&prepared, opts).await?;
        Ok(RelationSnapshot {
            root_count: rows.len(),
            rows,
            edges: Vec::new(),
        })
    }

    /// Tier-gated one-shot output-changing relation read evaluated as `author`.
    pub async fn all_relation_query_for_identity(
        &self,
        query: &RelationQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<RelationSnapshot, Error> {
        ensure_default_read_view(&opts)?;
        let query = relation_query_to_query(query)?;
        let prepared = self.prepare_query(&query)?;
        // Output-changing relation queries currently normalize to a single
        // root row set.  They have no array payload edges, so request ordinary
        // app rows instead of the relation-snapshot fact output (which is
        // reserved for correlated array/path materialization).
        let rows = self.all_for_identity(&prepared, opts, author).await?;
        Ok(RelationSnapshot {
            root_count: rows.len(),
            rows,
            edges: Vec::new(),
        })
    }

    /// Subscribe to a query and return a stream of materialized subscription events.
    ///
    /// ```rust
    /// # use jazz::db::{LocalUpdates, Propagation, ReadOpts, SubscriptionEvent};
    /// # use jazz::db::doctest_support::{block_on, open_todos_db, todo_cells};
    /// # use jazz::tx::DurabilityTier;
    /// let db = block_on(open_todos_db())?;
    /// let query = db.prepare_query(&db.table("todos"))?;
    /// let mut subscription = block_on(db.subscribe(
    ///     &query,
    ///     ReadOpts {
    ///         tier: DurabilityTier::Local,
    ///         local_updates: LocalUpdates::Immediate,
    ///         propagation: Propagation::LocalOnly,
    ///         include_deleted: false,
    ///         ..ReadOpts::default()
    ///     },
    /// ))?;
    /// let opened = block_on(subscription.next_event()).unwrap();
    /// let SubscriptionEvent::Delta { reset, added, .. } = opened else {
    ///     panic!("expected reset delta");
    /// };
    /// assert!(reset);
    /// assert!(added.is_empty());
    ///
    /// db.insert("todos", todo_cells("notify subscribers", false))?;
    /// let changed = block_on(subscription.next_event()).unwrap();
    /// let SubscriptionEvent::Delta { added, updated, removed, .. } = changed else {
    ///     panic!("expected subscription delta");
    /// };
    /// assert_eq!(added.len(), 1);
    /// assert!(updated.is_empty());
    /// assert!(removed.is_empty());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn subscribe(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<SubscriptionStream, Error> {
        self.open_subscription(
            prepared,
            opts,
            self.identity.author,
            QueryAuthorizationMode::ClientLocal,
        )
        .await
    }

    /// Subscribe to a query evaluated as `author`.
    pub async fn subscribe_for_identity(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<SubscriptionStream, Error> {
        self.open_subscription(
            prepared,
            opts,
            author,
            QueryAuthorizationMode::TrustedServing,
        )
        .await
    }

    /// Subscribe to an output-changing relation query.
    pub async fn subscribe_relation_query(
        &self,
        query: &RelationQuery,
        opts: ReadOpts,
    ) -> Result<SubscriptionStream, Error> {
        self.open_relation_subscription(
            query,
            opts,
            self.identity.author,
            QueryAuthorizationMode::ClientLocal,
        )
        .await
    }

    /// Subscribe to an output-changing relation query evaluated as `author`.
    pub async fn subscribe_relation_query_for_identity(
        &self,
        query: &RelationQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<SubscriptionStream, Error> {
        self.open_relation_subscription(query, opts, author, QueryAuthorizationMode::TrustedServing)
            .await
    }

    /// Attach a one-shot usage-site query coverage request.
    ///
    /// Bindings call this before an edge/global one-shot read, drive
    /// [`Db::tick`] until [`Db::query_attachment_is_covered`] is true, read, then
    /// call [`Db::detach_query`].
    pub fn attach_query_with_opts(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<QueryAttachment, Error> {
        ensure_supported_read_view(&opts)?;
        let upstream_opts = self.node.upstream_register_shape_options(
            effective_read_tier(&opts),
            opts.read_view.clone(),
            opts.propagation == Propagation::Full,
        );
        self.attach_or_refresh_query_coverage(
            &prepared.shape,
            &prepared.binding,
            upstream_opts,
            self.identity.author,
        )
    }

    /// Attach a one-shot usage-site query coverage request evaluated as `author`.
    pub fn attach_query_with_opts_for_identity(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorId,
    ) -> Result<QueryAttachment, Error> {
        ensure_supported_read_view(&opts)?;
        let upstream_opts = self.node.upstream_register_shape_options(
            effective_read_tier(&opts),
            opts.read_view.clone(),
            opts.propagation == Propagation::Full,
        );
        let (shape, binding, _) = self.node.node.borrow_mut().prepare_query_binding_for_link(
            &prepared.shape,
            &prepared.binding,
            upstream_opts.tier,
            author,
        )?;
        self.attach_or_refresh_query_coverage(&shape, &binding, upstream_opts, author)
    }

    fn attach_or_refresh_query_coverage(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        upstream_opts: RegisterShapeOptions,
        identity: AuthorId,
    ) -> Result<QueryAttachment, Error> {
        let requires_current_authority_receipt = upstream_opts.tier >= DurabilityTier::Edge;
        let binding_view = BindingViewKey::new(
            shape.shape_id(),
            binding.binding_id(),
            upstream_opts.read_view_key(),
        );
        let required_after = self
            .node
            .node
            .borrow()
            .applied_view_update_generation(binding_view);
        let coverage = coverage_key(shape, binding, upstream_opts.clone());
        if self
            .node
            .upstream_coverage_refcounts
            .borrow()
            .contains_key(&coverage)
            && let Some(subscription) = self
                .node
                .latest_coverage_subscriptions
                .borrow()
                .get(&coverage)
                .copied()
            && !self
                .node
                .query_coverage_registrations
                .borrow()
                .contains_key(&subscription)
        {
            *self
                .node
                .upstream_coverage_refcounts
                .borrow_mut()
                .entry(coverage.clone())
                .or_insert(0) += 1;
            let pending_subscription = PendingUpstreamSubscription {
                subscription,
                shape: shape.clone(),
                binding: binding.clone(),
                opts: upstream_opts.clone(),
                identity,
            };
            self.register_query_coverage(coverage.clone(), pending_subscription.clone(), false);
            let mut refreshes = self.node.coverage_refresh_generations.borrow_mut();
            if refreshes.get(&coverage).copied() != Some(required_after) {
                refreshes.insert(coverage.clone(), required_after);
                self.node
                    .upstream_subscriptions
                    .borrow_mut()
                    .push(PendingUpstreamCommand::Subscribe(pending_subscription));
                self.node.schedule_tick(TickUrgency::Immediate);
            }
            return Ok(QueryAttachment {
                subscriptions: vec![subscription],
                required_after: vec![(binding_view, required_after)],
                requires_current_authority_receipt,
                registrations: vec![subscription],
                refreshes: vec![(coverage, required_after)],
            });
        }
        let subscription = self.attach_query_shape_binding_with_opts(
            shape,
            binding,
            upstream_opts.clone(),
            identity,
        )?;
        *self
            .node
            .upstream_coverage_refcounts
            .borrow_mut()
            .entry(coverage.clone())
            .or_insert(0) += 1;
        self.register_query_coverage(
            coverage.clone(),
            PendingUpstreamSubscription {
                subscription,
                shape: shape.clone(),
                binding: binding.clone(),
                opts: upstream_opts,
                identity,
            },
            true,
        );
        Ok(QueryAttachment {
            subscriptions: vec![subscription],
            required_after: vec![(binding_view, required_after)],
            requires_current_authority_receipt,
            registrations: vec![subscription],
            refreshes: Vec::new(),
        })
    }

    fn register_query_coverage(
        &self,
        coverage: CoverageKey,
        subscription: PendingUpstreamSubscription,
        owns_subscription: bool,
    ) {
        let mut registrations = self.node.query_coverage_registrations.borrow_mut();
        registrations
            .entry(subscription.subscription)
            .and_modify(|registration| registration.ref_count += 1)
            .or_insert(QueryCoverageRegistration {
                coverage,
                subscription,
                owns_subscription,
                ref_count: 1,
            });
    }

    fn attach_query_shape_binding_with_opts(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        opts: RegisterShapeOptions,
        identity: AuthorId,
    ) -> Result<SubscriptionKey, Error> {
        let subscription = self.node.next_subscription_key(shape, opts.read_view_key());
        self.node
            .upstream_subscriptions
            .borrow_mut()
            .push(PendingUpstreamCommand::Subscribe(
                PendingUpstreamSubscription {
                    subscription,
                    shape: shape.clone(),
                    binding: binding.clone(),
                    opts: opts.clone(),
                    identity,
                },
            ));
        self.node
            .latest_coverage_subscriptions
            .borrow_mut()
            .insert(coverage_key(shape, binding, opts), subscription);
        self.node.schedule_tick(TickUrgency::Immediate);
        Ok(subscription)
    }

    /// Attach a one-shot usage-site query coverage request at the default tier.
    pub fn attach_query(&self, prepared: &PreparedQuery) -> Result<QueryAttachment, Error> {
        self.attach_query_with_opts(prepared, ReadOpts::default())
    }

    /// Return whether each usage-site attachment has observed a newer logical
    /// server receipt than the one it captured during registration.
    pub fn query_attachment_is_covered(&self, attachment: &QueryAttachment) -> bool {
        let node = self.node.node.borrow();
        let active_receipts = self.node.active_authority_view_receipts.borrow();
        let covered = attachment
            .required_after
            .iter()
            .all(|(binding_view, required_after)| {
                node.applied_view_update_generation(*binding_view) > *required_after
                    && !node.opening_pending_for_binding_view(*binding_view)
                    && (!attachment.requires_current_authority_receipt
                        || active_receipts
                            .as_ref()
                            .is_some_and(|receipts| receipts.binding_views.contains(binding_view)))
            });
        drop(node);
        drop(active_receipts);
        if covered {
            let mut refreshes = self.node.coverage_refresh_generations.borrow_mut();
            for (coverage, generation) in &attachment.refreshes {
                if refreshes.get(coverage).copied() == Some(*generation) {
                    refreshes.remove(coverage);
                }
            }
        }
        covered
    }

    /// Detach a one-shot query coverage request.
    pub fn detach_query(&self, attachment: QueryAttachment) {
        let mut removed_subscriptions = Vec::new();
        let mut registrations = self.node.query_coverage_registrations.borrow_mut();
        for subscription in attachment.registrations {
            let Some(registration) = registrations.get_mut(&subscription) else {
                continue;
            };
            let coverage = registration.coverage.clone();
            let owns_subscription = registration.owns_subscription;
            registration.ref_count = registration.ref_count.saturating_sub(1);
            let last_registration = registration.ref_count == 0;
            if last_registration {
                registrations.remove(&subscription);
            }
            let mut coverage_refcounts = self.node.upstream_coverage_refcounts.borrow_mut();
            let Some(count) = coverage_refcounts.get_mut(&coverage) else {
                continue;
            };
            *count = count.saturating_sub(1);
            let last_coverage_pin = *count == 0;
            if last_coverage_pin {
                coverage_refcounts.remove(&coverage);
                self.node
                    .awaiting_initial_authority_coverage
                    .borrow_mut()
                    .remove(&coverage);
            }
            let has_live_stream_owner = self
                .node
                .upstream_subscription_owners
                .borrow()
                .get(&subscription)
                .is_some_and(|owners| owners.iter().any(|owner| owner.strong_count() > 0));
            if (owns_subscription && last_registration && !has_live_stream_owner)
                || last_coverage_pin
            {
                removed_subscriptions.push((subscription, coverage));
            }
        }
        drop(registrations);
        for (subscription, coverage) in removed_subscriptions {
            self.node.node.borrow_mut().apply_unsubscribe(subscription);
            let replacement = self
                .node
                .query_coverage_registrations
                .borrow()
                .values()
                .find(|registration| registration.coverage == coverage)
                .map(|registration| registration.subscription.subscription);
            let mut latest = self.node.latest_coverage_subscriptions.borrow_mut();
            if latest.get(&coverage) == Some(&subscription) {
                if let Some(replacement) = replacement {
                    latest.insert(coverage.clone(), replacement);
                } else {
                    latest.remove(&coverage);
                }
            }
            drop(latest);
            self.node
                .upstream_subscriptions
                .borrow_mut()
                .push(PendingUpstreamCommand::Unsubscribe(subscription));
        }
        self.node.schedule_tick(TickUrgency::Immediate);
    }

    async fn open_subscription(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorId,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<SubscriptionStream, Error> {
        ensure_supported_subscription_read_opts(&opts)?;
        self.validate_prepared_shape_for_registration(prepared)?;
        let read_tier = effective_read_tier(&opts);
        self.node
            .node
            .borrow_mut()
            .ensure_peer_maintained_subscription_view_supported(
                &prepared.shape,
                &prepared.binding,
                read_tier,
                author,
                &opts.read_view,
                authorization_mode,
            )?;
        let (local_shape, local_binding, _local_plan) = self
            .node
            .node
            .borrow_mut()
            .prepare_query_binding_for_link_in_authorization_mode(
                &prepared.shape,
                &prepared.binding,
                read_tier,
                author,
                authorization_mode,
            )?;
        let (subscription, snapshot) = self
            .node
            .node
            .borrow_mut()
            .open_maintained_view_subscription_in_authorization_mode(
                &local_shape,
                &local_binding,
                author,
                read_tier,
                &opts.read_view,
                Some(_local_plan),
                authorization_mode,
            )?;
        let root_occurrence_ids = subscription.root_occurrence_ids().to_vec();
        let local_subscription_id = subscription.subscription_id();
        let local_node = Rc::clone(&self.node.node);
        let local_runtime_token = local_node.borrow().groove_runtime_token();
        let local_subscription_cleanup = Rc::new(Cell::new(Some((
            local_runtime_token,
            local_subscription_id,
        ))));
        let local_cleanup_handle = Rc::clone(&local_subscription_cleanup);
        let mut local_cleanup = CleanupGuard::new(Box::new(move || {
            let mut node = local_node.borrow_mut();
            if let Some((runtime_token, subscription_id)) = local_cleanup_handle.get()
                && node.groove_runtime_token() == runtime_token
            {
                node.unsubscribe_groove_subscription(subscription_id);
            }
        }));
        let mut maintained_subscription = Some(subscription);
        // A projected ordered root needs terminal patches even without nested
        // arrays: an unprojected sort-key mutation can move a visible row
        // without changing the projected payload. Unprojected roots retain
        // ordinary row deltas, including scope re-entry membership changes.
        let terminal_rows = !local_shape.query().array_subqueries.is_empty()
            || (local_shape.query().select.is_some() && !local_shape.query().order_by.is_empty());
        let mut state_shape = local_shape;
        let mut state_binding = local_binding;
        let mut remote_read_tier = None;
        let mut requires_authority_receipt = false;
        let mut upstream_subscription_handles = Vec::new();
        let mut suppress_provisional_opening = false;
        let remote_propagate_upstream = opts.propagation == Propagation::Full;
        // A non-durable browser client must still ask its durable worker for a
        // local-only view. The wire flag stops that request at the worker.
        let propagates_upstream = remote_propagate_upstream
            || self.node.upstream_durability_floor.get() == DurabilityTier::Local;
        if propagates_upstream {
            let upstream_opts = self.node.upstream_register_shape_options(
                effective_read_tier(&opts),
                opts.read_view.clone(),
                remote_propagate_upstream,
            );
            let (shape, binding) = if upstream_opts.tier == read_tier {
                (state_shape.clone(), state_binding.clone())
            } else {
                let (shape, binding, _) = self
                    .node
                    .node
                    .borrow_mut()
                    .prepare_query_binding_for_link_in_authorization_mode(
                        &prepared.shape,
                        &prepared.binding,
                        upstream_opts.tier,
                        author,
                        authorization_mode,
                    )?;
                (shape, binding)
            };
            state_shape = shape.clone();
            state_binding = binding.clone();
            remote_read_tier = Some(upstream_opts.tier);
            // Edge/Global cache possession is never a settlement receipt,
            // even when this subscription opens before an upstream exists.
            // The eventual connection must send its own ViewUpdate.
            requires_authority_receipt = upstream_opts.tier >= DurabilityTier::Edge;
            let opened = self.open_subscription_upstream_coverage(
                &shape,
                &binding,
                upstream_opts,
                author,
                authorization_mode,
            )?;
            upstream_subscription_handles = opened.handles;
            suppress_provisional_opening = authorization_mode
                == QueryAuthorizationMode::ClientLocal
                && read_tier >= DurabilityTier::Edge
                && opened.awaits_initial_authority_response
                && snapshot.root_count == 0
                && snapshot.edges.is_empty();
        }
        let settled_tier = remote_read_tier.unwrap_or(read_tier);
        if authorization_mode == QueryAuthorizationMode::ClientLocal
            && remote_read_tier.is_some()
            && state_shape.query().aggregate.is_none()
        {
            let binding_view_key = BindingViewKey {
                shape_id: state_shape.shape_id(),
                binding_id: state_binding.binding_id(),
                read_view: RegisterShapeOptions {
                    tier: settled_tier,
                    read_view: opts.read_view.clone(),
                    propagate_upstream: remote_propagate_upstream,
                }
                .read_view_key(),
            };
            if let Some(maintained) = maintained_subscription.as_mut() {
                self.node
                    .node
                    .borrow()
                    .seed_local_maintained_authoritative_result_membership(
                        maintained,
                        binding_view_key,
                    );
            }
        }
        let settled = subscription_is_settled(
            &self.node.node.borrow(),
            &self.node.active_authority_view_receipts,
            &state_shape,
            &state_binding,
            settled_tier,
            opts.read_view.clone(),
            remote_propagate_upstream,
            requires_authority_receipt,
        );
        // An empty local opening carries no observable result information at
        // an Edge/Global request.  Until the authority replies, publishing it
        // would let a public subscription report a provisional empty view as
        // its first delivery.  `awaits_initial_authority_response` is only
        // known while opening a fresh upstream handle, but an already-open
        // link has the same receipt requirement.
        suppress_provisional_opening |= authorization_mode == QueryAuthorizationMode::ClientLocal
            && read_tier >= DurabilityTier::Edge
            && remote_read_tier.is_some()
            && !settled
            && snapshot.root_count == 0
            && snapshot.edges.is_empty();
        let (sender, receiver) = unbounded();
        let initial_outputs =
            subscription_outputs_with_occurrence_sidecar(&snapshot, &root_occurrence_ids)?;
        let state_snapshot = relation_snapshot_with_delta_slack(&snapshot);
        let mut snapshot_index = RelationSnapshotIndex::from_snapshot(&state_snapshot);
        snapshot_index.roots = root_occurrence_ids
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, occurrence)| (occurrence, index))
            .collect();
        let state = Rc::new(RefCell::new(SubscriptionState {
            terminal_rows,
            kind: SubscriptionKind::Prepared {
                shape: state_shape,
                binding: state_binding,
                maintained_subscription,
            },
            groove_runtime_token: self.node.node.borrow().groove_runtime_token(),
            local_subscription_cleanup,
            propagates_upstream,
            author,
            authorization_mode,
            read_tier,
            remote_read_tier,
            requires_authority_receipt,
            remote_propagate_upstream,
            read_view: opts.read_view.clone(),
            snapshot: state_snapshot,
            snapshot_index,
            snapshot_source: SubscriptionSnapshotSource::LocalMaintained,
            settled,
            sender,
        }));
        state
            .borrow()
            .sender
            .unbounded_send(SubscriptionEvent::Delta {
                reset: true,
                publishable: !suppress_provisional_opening,
                added: initial_outputs,
                updated: Vec::new(),
                removed: Vec::new(),
                terminal_operations: Vec::new(),
                terminal_layout: None,
                settled,
                tier: read_tier,
            })
            .map_err(|_| Error::new(ErrorCode::Protocol, "subscription receiver closed"))?;
        self.node
            .subscriptions
            .borrow_mut()
            .push(Rc::downgrade(&state));
        let cleanup = if upstream_subscription_handles.is_empty() {
            local_cleanup.take()
        } else {
            let owner = Rc::downgrade(&state);
            register_upstream_subscription_owner(
                &self.node.upstream_subscription_owners,
                &upstream_subscription_handles,
                &state,
            );
            let upstream_cleanup =
                self.upstream_subscription_cleanup(upstream_subscription_handles, owner);
            let local_cleanup = local_cleanup.take();
            Box::new(move || {
                local_cleanup();
                upstream_cleanup();
            })
        };
        Ok(SubscriptionStream {
            receiver,
            _state: state,
            cleanup: Some(cleanup),
        })
    }

    fn validate_prepared_shape_for_registration(
        &self,
        prepared: &PreparedQuery,
    ) -> Result<(), Error> {
        let ast = ShapeAst::from_validated(&prepared.shape);
        let validation = {
            let node = self.node.node.borrow();
            validate_shape_ast_for_registration(&node, prepared.shape.shape_id(), &ast)
        };
        validation.map(|_| ()).map_err(Error::from)
    }

    async fn open_relation_subscription(
        &self,
        query: &RelationQuery,
        opts: ReadOpts,
        author: AuthorId,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<SubscriptionStream, Error> {
        ensure_supported_subscription_read_opts(&opts)?;
        let query = relation_query_to_query(query)?;
        let prepared = self.prepare_query(&query)?;
        self.open_subscription(&prepared, opts, author, authorization_mode)
            .await
    }

    fn open_subscription_upstream_coverage(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        opts: RegisterShapeOptions,
        identity: AuthorId,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<OpenedUpstreamCoverage, Error> {
        self.node
            .node
            .borrow_mut()
            .ensure_peer_maintained_subscription_view_supported(
                shape,
                binding,
                opts.tier,
                identity,
                &opts.read_view,
                authorization_mode,
            )?;
        let coverage = coverage_key(shape, binding, opts.clone());
        if self
            .node
            .upstream_coverage_refcounts
            .borrow()
            .contains_key(&coverage)
        {
            if let Some(subscription) = self
                .node
                .latest_coverage_subscriptions
                .borrow()
                .get(&coverage)
                .copied()
            {
                *self
                    .node
                    .upstream_coverage_refcounts
                    .borrow_mut()
                    .entry(coverage.clone())
                    .or_insert(0) += 1;
                let awaits_initial_authority_response = self
                    .node
                    .awaiting_initial_authority_coverage
                    .borrow()
                    .contains(&coverage);
                return Ok(OpenedUpstreamCoverage {
                    handles: vec![UpstreamCoverageHandle {
                        coverage,
                        subscription,
                    }],
                    awaits_initial_authority_response,
                });
            }
        }
        let subscription =
            self.attach_query_shape_binding_with_opts(shape, binding, opts, identity)?;
        *self
            .node
            .upstream_coverage_refcounts
            .borrow_mut()
            .entry(coverage.clone())
            .or_insert(0) += 1;
        let has_live_upstream =
            self.node.connections.borrow().iter().any(|connection| {
                matches!(&connection.borrow().link, ConnectionLink::Upstream { .. })
            });
        if has_live_upstream {
            self.node
                .awaiting_initial_authority_coverage
                .borrow_mut()
                .insert(coverage.clone());
        }
        Ok(OpenedUpstreamCoverage {
            handles: vec![UpstreamCoverageHandle {
                coverage,
                subscription,
            }],
            awaits_initial_authority_response: has_live_upstream,
        })
    }

    fn upstream_subscription_cleanup(
        &self,
        upstream_subscriptions: Vec<UpstreamCoverageHandle>,
        owner: Weak<RefCell<SubscriptionState>>,
    ) -> Box<dyn FnOnce()> {
        let node = Rc::clone(&self.node.node);
        let latest_coverage_subscriptions = Rc::clone(&self.node.latest_coverage_subscriptions);
        let upstream_coverage_refcounts = Rc::clone(&self.node.upstream_coverage_refcounts);
        let awaiting_initial_authority_coverage =
            Rc::clone(&self.node.awaiting_initial_authority_coverage);
        let upstream_subscription_owners = Rc::clone(&self.node.upstream_subscription_owners);
        let pending_upstream_subscriptions = Rc::clone(&self.node.upstream_subscriptions);
        let scheduler = Rc::clone(&self.node.scheduler);
        Box::new(move || {
            for handle in upstream_subscriptions {
                unregister_upstream_subscription_owner(
                    &upstream_subscription_owners,
                    handle.subscription,
                    &owner,
                );
                let mut refcounts = upstream_coverage_refcounts.borrow_mut();
                let Some(count) = refcounts.get_mut(&handle.coverage) else {
                    continue;
                };
                *count = count.saturating_sub(1);
                if *count > 0 {
                    continue;
                }
                refcounts.remove(&handle.coverage);
                awaiting_initial_authority_coverage
                    .borrow_mut()
                    .remove(&handle.coverage);
                drop(refcounts);
                let upstream_subscription = handle.subscription;
                node.borrow_mut().apply_unsubscribe(upstream_subscription);
                latest_coverage_subscriptions
                    .borrow_mut()
                    .retain(|coverage, subscription| {
                        coverage != &handle.coverage && *subscription != upstream_subscription
                    });
                pending_upstream_subscriptions
                    .borrow_mut()
                    .push(PendingUpstreamCommand::Unsubscribe(upstream_subscription));
            }
            schedule_tick_in(&scheduler, TickUrgency::Immediate);
        })
    }

    /// Insert a row locally, generating a uuidv7-shaped row id.
    ///
    /// The generated id is available from [`WriteHandle::row_uuid`].
    ///
    /// ```rust
    /// # use jazz::db::doctest_support::{block_on, open_todos_db};
    /// # use jazz::tx::DurabilityTier;
    /// let db = block_on(open_todos_db())?;
    /// let write = db.insert("todos", jazz::row! { title: "new todo", done: false })?;
    /// let row = write.row_uuid();
    /// block_on(write.wait(DurabilityTier::Local))?;
    ///
    /// let todos = db.prepare_query(&db.table("todos"))?;
    /// assert_eq!(db.read(&todos)?.len(), 1);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn insert(&self, table: &str, cells: RowCells) -> Result<WriteHandle<S>, Error> {
        let row = self.row_id_source.borrow_mut().next_row_id();
        self.write_mergeable(
            self.identity.author,
            None,
            table,
            row,
            cells,
            Vec::new(),
            None,
        )
    }

    /// Insert a row while attributing provenance to `made_by`.
    ///
    /// The Db's authenticated identity remains the write-policy subject. Client
    /// facades can only write as themselves; trusted-backend attribution is a
    /// serving-node concern on inbound commit-unit ingestion.
    pub fn insert_attributed(
        &self,
        made_by: AuthorId,
        table: &str,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        let row = self.row_id_source.borrow_mut().next_row_id();
        self.write_mergeable_as_session_subject(made_by, table, row, cells, Vec::new(), None)
    }

    /// Insert a row with a caller-supplied id.
    ///
    /// This is a niche path for imports from legacy systems or other cases
    /// where row identity already exists. New local rows should generally use
    /// [`Db::insert`] so the database generates the id.
    pub fn insert_with_id(
        &self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_absent(table, row, self.identity.author)?;
        self.write_mergeable(
            self.identity.author,
            None,
            table,
            row,
            cells,
            Vec::new(),
            None,
        )
    }

    /// Insert a caller-id row while attributing provenance to `made_by`.
    ///
    /// See [`Db::insert_attributed`] for the security boundary.
    pub fn insert_with_id_attributed(
        &self,
        made_by: AuthorId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_absent(table, row, self.identity.author)?;
        self.write_mergeable_as_session_subject(made_by, table, row, cells, Vec::new(), None)
    }

    /// Insert a row while evaluating write policy as `identity`.
    pub fn insert_for_identity(
        &self,
        identity: AuthorId,
        table: &str,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        let row = self.row_id_source.borrow_mut().next_row_id();
        self.insert_with_id_for_identity(identity, table, row, cells)
    }

    /// Insert a caller-id row with an explicit millisecond provenance time.
    pub fn insert_with_id_at_ms(
        &self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        now_ms: u64,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_absent(table, row, self.identity.author)?;
        self.write_mergeable_at_ms(
            self.identity.author,
            None,
            table,
            row,
            cells,
            Vec::new(),
            None,
            now_ms,
        )
    }

    /// Insert a caller-id row while evaluating write policy as `identity`.
    ///
    /// This is a trusted serving-node API for terminated backend/request
    /// sessions. It records provenance as `identity` and evaluates policy as
    /// the same identity, without changing the Db's own authority.
    pub fn insert_with_id_for_identity(
        &self,
        identity: AuthorId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_absent(table, row, identity)?;
        let cells = self.apply_insert_defaults(table, cells)?;
        // Client writes are admitted structurally and staged optimistically.
        // A trusted serving authority evaluates policy and returns the fate.
        self.write_mergeable(
            identity,
            Some(identity),
            table,
            row,
            cells,
            Vec::new(),
            None,
        )
    }

    /// Insert a caller-id row for `identity` with an explicit millisecond provenance time.
    pub fn insert_with_id_for_identity_at_ms(
        &self,
        identity: AuthorId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        now_ms: u64,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_absent(table, row, identity)?;
        let cells = self.apply_insert_defaults(table, cells)?;
        // See `insert_with_id_for_identity`: policy fate belongs to the
        // trusted serving authority, not this local client admission path.
        self.write_mergeable_at_ms(
            identity,
            Some(identity),
            table,
            row,
            cells,
            Vec::new(),
            None,
            now_ms,
        )
    }

    /// Advise whether an insert may be allowed.
    ///
    /// A `Db` is ordinarily a client-local replica, whose policy evidence may
    /// be incomplete. It therefore never turns a local policy evaluation into
    /// an allow/deny result. Use an explicitly trusted serving authority for a
    /// final decision.
    pub fn can_insert(&self, _table: &str, _cells: RowCells) -> Result<PermissionAdvice, Error> {
        Ok(PermissionAdvice::Unknown)
    }

    /// Evaluate an insert for a test-only serving-path probe without writing.
    #[cfg(test)]
    pub(crate) fn authorize_insert_for_identity(
        &self,
        table: &str,
        cells: RowCells,
        identity: AuthorId,
    ) -> Result<PermissionAdvice, Error> {
        let cells = self.apply_insert_defaults(table, cells)?;
        self.node
            .node
            .borrow_mut()
            .dry_run_mergeable_write_allows_for_view(
                &self.schema,
                MergeableCommit::new(table, RowUuid::from_bytes([0; 16]), 0)
                    .made_by(identity)
                    .permission_subject(identity)
                    .cells(cells),
            )
            .map(|allowed| {
                if allowed {
                    PermissionAdvice::Allowed
                } else {
                    PermissionAdvice::Denied
                }
            })
            .map_err(Into::into)
    }

    /// Update a row locally; omitted fields keep their current local value.
    ///
    /// ```rust
    /// # use std::collections::BTreeMap;
    /// # use jazz::db::doctest_support::{block_on, open_todos_db, todo_cells};
    /// # use jazz::ids::RowUuid;
    /// # use jazz::groove::records::Value;
    /// let db = block_on(open_todos_db())?;
    /// let todo = RowUuid::from_bytes([1; 16]);
    /// db.insert_with_id("todos", todo, todo_cells("draft", false))?;
    ///
    /// db.update(
    ///     "todos",
    ///     todo,
    ///     BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
    /// )?;
    /// let todos = db.prepare_query(&db.table("todos"))?;
    /// assert_eq!(db.read(&todos)?.len(), 1);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn update(
        &self,
        table: &str,
        row: RowUuid,
        patch: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        if patch.is_empty() {
            return self.no_op_update_handle_for_client(table, row, self.identity.author);
        }
        let (cells, parent, authored_columns) = self.merge_existing_cells(table, row, patch)?;
        self.write_mergeable_with_authored_columns(
            self.identity.author,
            None,
            table,
            row,
            cells,
            parent.into_iter().collect(),
            None,
            authored_columns,
        )
    }

    /// Update a row with an explicit millisecond provenance time.
    pub fn update_at_ms(
        &self,
        table: &str,
        row: RowUuid,
        patch: RowCells,
        now_ms: u64,
    ) -> Result<WriteHandle<S>, Error> {
        if patch.is_empty() {
            return self.no_op_update_handle_for_client(table, row, self.identity.author);
        }
        let (cells, parent, authored_columns) = self.merge_existing_cells(table, row, patch)?;
        self.write_mergeable_at_ms_with_authorship(
            self.identity.author,
            None,
            table,
            row,
            cells,
            parent.into_iter().collect(),
            None,
            Some(authored_columns),
            now_ms,
        )
    }

    /// Update a row while attributing provenance to `made_by`.
    ///
    /// See [`Db::insert_attributed`] for the security boundary.
    pub fn update_attributed(
        &self,
        made_by: AuthorId,
        table: &str,
        row: RowUuid,
        patch: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        self.check_attribution_allowed(made_by)?;
        if patch.is_empty() {
            return self.no_op_update_handle_for_client(table, row, self.identity.author);
        }
        let (cells, parent, authored_columns) = self.merge_existing_cells(table, row, patch)?;
        self.write_mergeable_as_session_subject_with_authored_columns(
            made_by,
            table,
            row,
            cells,
            parent.into_iter().collect(),
            None,
            authored_columns,
        )
    }

    /// Update a row while evaluating write policy as `identity`.
    pub fn update_for_identity(
        &self,
        identity: AuthorId,
        table: &str,
        row: RowUuid,
        patch: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        if patch.is_empty() {
            return self.no_op_update_handle_for_identity(table, row, identity);
        }
        let (cells, parent, authored_columns) =
            self.merge_existing_cells_for_identity(table, row, patch, identity)?;
        let parents = parent.into_iter().collect::<Vec<_>>();
        self.write_mergeable_with_authored_columns(
            identity,
            Some(identity),
            table,
            row,
            cells,
            parents,
            None,
            authored_columns,
        )
    }

    /// Update a row for `identity` with an explicit millisecond provenance time.
    pub fn update_for_identity_at_ms(
        &self,
        identity: AuthorId,
        table: &str,
        row: RowUuid,
        patch: RowCells,
        now_ms: u64,
    ) -> Result<WriteHandle<S>, Error> {
        if patch.is_empty() {
            return self.no_op_update_handle_for_identity(table, row, identity);
        }
        let (cells, parent, authored_columns) =
            self.merge_existing_cells_for_identity(table, row, patch, identity)?;
        let parents = parent.into_iter().collect::<Vec<_>>();
        self.write_mergeable_at_ms_with_authorship(
            identity,
            Some(identity),
            table,
            row,
            cells,
            parents,
            None,
            Some(authored_columns),
            now_ms,
        )
    }

    /// Upsert a row locally.
    ///
    /// This explicit-id path is primarily for importing rows from legacy
    /// systems. New local rows should generally use [`Db::insert`] and then
    /// update the returned [`WriteHandle::row_uuid`] when needed.
    ///
    /// ```rust
    /// # use std::collections::BTreeMap;
    /// # use jazz::db::doctest_support::{block_on, open_todos_db, todo_cells};
    /// # use jazz::ids::RowUuid;
    /// # use jazz::groove::records::Value;
    /// let db = block_on(open_todos_db())?;
    /// let todo = RowUuid::from_bytes([1; 16]);
    ///
    /// db.upsert("todos", todo, todo_cells("created", false))?;
    /// db.upsert(
    ///     "todos",
    ///     todo,
    ///     BTreeMap::from([("title".to_owned(), Value::String("renamed".to_owned()))]),
    /// )?;
    /// let todos = db.prepare_query(&db.table("todos"))?;
    /// assert_eq!(db.one(&todos)?.unwrap().row_uuid(), todo);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn upsert(
        &self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_not_deleted(table, row)?;
        let (cells, parents, authored_columns) = if self
            .upsert_target_for_client_identity(table, row, self.identity.author)?
            .is_some()
        {
            let (cells, parent, authored_columns) = self.merge_existing_cells(table, row, cells)?;
            (cells, parent.into_iter().collect(), Some(authored_columns))
        } else {
            (cells, Vec::new(), None)
        };
        self.write_mergeable_at_ms_with_authorship(
            self.identity.author,
            None,
            table,
            row,
            cells,
            parents,
            None,
            authored_columns,
            self.next_now_ms(),
        )
    }

    /// Upsert a row with an explicit millisecond provenance time.
    pub fn upsert_at_ms(
        &self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        now_ms: u64,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_not_deleted(table, row)?;
        let (cells, parents, authored_columns) = if self
            .upsert_target_for_client_identity(table, row, self.identity.author)?
            .is_some()
        {
            let (cells, parent, authored_columns) = self.merge_existing_cells(table, row, cells)?;
            (cells, parent.into_iter().collect(), Some(authored_columns))
        } else {
            (cells, Vec::new(), None)
        };
        self.write_mergeable_at_ms_with_authorship(
            self.identity.author,
            None,
            table,
            row,
            cells,
            parents,
            None,
            authored_columns,
            now_ms,
        )
    }

    /// Upsert a row while evaluating write policy as `identity`.
    pub fn upsert_for_identity(
        &self,
        identity: AuthorId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_not_deleted(table, row)?;
        let (cells, parents, authored_columns) = if self
            .upsert_target_for_trusted_identity(table, row, identity)?
            .is_some()
        {
            let (cells, parent, authored_columns) =
                self.merge_existing_cells_for_identity(table, row, cells, identity)?;
            (cells, parent.into_iter().collect(), Some(authored_columns))
        } else {
            (cells, Vec::new(), None)
        };
        self.write_mergeable_at_ms_with_authorship(
            identity,
            Some(identity),
            table,
            row,
            cells,
            parents,
            None,
            authored_columns,
            self.next_now_ms(),
        )
    }

    /// Upsert a row for `identity` with an explicit millisecond provenance time.
    pub fn upsert_for_identity_at_ms(
        &self,
        identity: AuthorId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        now_ms: u64,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_not_deleted(table, row)?;
        let (cells, parents, authored_columns) = if self
            .upsert_target_for_trusted_identity(table, row, identity)?
            .is_some()
        {
            let (cells, parent, authored_columns) =
                self.merge_existing_cells_for_identity(table, row, cells, identity)?;
            (cells, parent.into_iter().collect(), Some(authored_columns))
        } else {
            (cells, Vec::new(), None)
        };
        self.write_mergeable_at_ms_with_authorship(
            identity,
            Some(identity),
            table,
            row,
            cells,
            parents,
            None,
            authored_columns,
            now_ms,
        )
    }

    /// Soft-delete a row locally.
    ///
    /// ```rust
    /// # use jazz::db::doctest_support::{block_on, open_todos_db, todo_cells};
    /// # use jazz::ids::RowUuid;
    /// let db = block_on(open_todos_db())?;
    /// let todo = RowUuid::from_bytes([1; 16]);
    /// db.insert_with_id("todos", todo, todo_cells("remove me", false))?;
    ///
    /// db.delete("todos", todo)?;
    /// let todos = db.prepare_query(&db.table("todos"))?;
    /// assert!(db.read(&todos)?.is_empty());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn delete(&self, table: &str, row: RowUuid) -> Result<WriteHandle<S>, Error> {
        self.delete_at_ms_option(table, row, None)
    }

    /// Soft-delete a row with explicit millisecond provenance time.
    pub fn delete_at_ms(
        &self,
        table: &str,
        row: RowUuid,
        now_ms: u64,
    ) -> Result<WriteHandle<S>, Error> {
        self.delete_at_ms_option(table, row, Some(now_ms))
    }

    fn delete_at_ms_option(
        &self,
        table: &str,
        row: RowUuid,
        now_ms: Option<u64>,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_not_deleted(table, row)?;
        let (parents, _) = self.row_layer_parents(table, row)?;
        match now_ms {
            Some(now_ms) => self.write_mergeable_at_ms(
                self.identity.author,
                None,
                table,
                row,
                BTreeMap::new(),
                parents,
                Some(DeletionEvent::Deleted),
                now_ms,
            ),
            None => self.write_mergeable(
                self.identity.author,
                None,
                table,
                row,
                BTreeMap::new(),
                parents,
                Some(DeletionEvent::Deleted),
            ),
        }
    }

    /// Soft-delete a row while attributing provenance to `made_by`.
    ///
    /// See [`Db::insert_attributed`] for the security boundary.
    pub fn delete_attributed(
        &self,
        made_by: AuthorId,
        table: &str,
        row: RowUuid,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_not_deleted(table, row)?;
        let (parents, _) = self.row_layer_parents(table, row)?;
        self.write_mergeable_as_session_subject(
            made_by,
            table,
            row,
            BTreeMap::new(),
            parents,
            Some(DeletionEvent::Deleted),
        )
    }

    /// Soft-delete a row while evaluating write policy as `identity`.
    pub fn delete_for_identity(
        &self,
        identity: AuthorId,
        table: &str,
        row: RowUuid,
    ) -> Result<WriteHandle<S>, Error> {
        self.delete_for_identity_at_ms_option(identity, table, row, None)
    }

    /// Soft-delete a row while evaluating write policy as `identity`, with explicit time.
    pub fn delete_for_identity_at_ms(
        &self,
        identity: AuthorId,
        table: &str,
        row: RowUuid,
        now_ms: u64,
    ) -> Result<WriteHandle<S>, Error> {
        self.delete_for_identity_at_ms_option(identity, table, row, Some(now_ms))
    }

    fn delete_for_identity_at_ms_option(
        &self,
        identity: AuthorId,
        table: &str,
        row: RowUuid,
        now_ms: Option<u64>,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_not_deleted(table, row)?;
        let (parents, _) = self.row_layer_parents(table, row)?;
        match now_ms {
            Some(now_ms) => self.write_mergeable_at_ms(
                identity,
                Some(identity),
                table,
                row,
                BTreeMap::new(),
                parents,
                Some(DeletionEvent::Deleted),
                now_ms,
            ),
            None => self.write_mergeable(
                identity,
                Some(identity),
                table,
                row,
                BTreeMap::new(),
                parents,
                Some(DeletionEvent::Deleted),
            ),
        }
    }

    /// Advise whether a read may be allowed. Client-local replicas return
    /// `Unknown` rather than using locally available rows as policy evidence.
    pub fn can_read(&self, _table: &str, _row: RowUuid) -> Result<PermissionAdvice, Error> {
        Ok(PermissionAdvice::Unknown)
    }

    /// Evaluate a read for the serving path without disclosing data.
    pub(crate) fn authorize_read_for_identity(
        &self,
        table: &str,
        row: RowUuid,
        author: AuthorId,
    ) -> Result<PermissionAdvice, Error> {
        self.table_schema(table)?;
        self.node
            .node
            .borrow_mut()
            .dry_run_read_current_allows(table, row, author)
            .map(|allowed| {
                if allowed {
                    PermissionAdvice::Allowed
                } else {
                    PermissionAdvice::Denied
                }
            })
            .map_err(Into::into)
    }

    /// Advise whether an update may be allowed. Client-local replicas return
    /// `Unknown` rather than using locally available rows as policy evidence.
    pub fn can_update(&self, _table: &str, _row: RowUuid) -> Result<PermissionAdvice, Error> {
        Ok(PermissionAdvice::Unknown)
    }

    /// Attach process-local auth claims for `identity`.
    pub fn set_identity_claims(&self, identity: AuthorId, claims: BTreeMap<String, Value>) {
        let changed = {
            let mut node = self.node.node.borrow_mut();
            let previous_revision = node.session_claim_revision(identity);
            node.set_session_claims(identity, claims);
            node.session_claim_revision(identity) != previous_revision
        };
        if changed {
            self.node.schedule_tick(TickUrgency::Deferred);
        }
    }

    /// Advise whether a delete may be allowed. Client-local replicas return
    /// `Unknown` rather than using locally available rows as policy evidence.
    pub fn can_delete(&self, _table: &str, _row: RowUuid) -> Result<PermissionAdvice, Error> {
        Ok(PermissionAdvice::Unknown)
    }

    /// Evaluate a delete for a test-only serving-path probe without writing.
    #[cfg(test)]
    pub(crate) fn authorize_delete_for_identity(
        &self,
        table: &str,
        row: RowUuid,
        author: AuthorId,
    ) -> Result<PermissionAdvice, Error> {
        self.table_schema(table)?;
        self.node
            .node
            .borrow_mut()
            .dry_run_delete_current_allows(table, row, author)
            .map(|allowed| {
                if allowed {
                    PermissionAdvice::Allowed
                } else {
                    PermissionAdvice::Denied
                }
            })
            .map_err(Into::into)
    }

    /// Build a mergeable transaction that commits multiple writes under one id.
    pub fn mergeable_tx(&self) -> Result<MergeableTx<'_, S>, Error> {
        let tx_id = OpenBatchId::new();
        self.begin_mergeable(tx_id)?;
        Ok(MergeableTx {
            db: self,
            tx_id,
            committed: false,
        })
    }

    /// Run `callback` in a mergeable transaction and commit all staged writes as one transaction.
    ///
    /// If `callback` returns an error, the transaction is dropped without committing. Reads and
    /// writes through the [`MergeableTx`] observe earlier writes staged in the same callback.
    pub fn transaction<T>(
        &self,
        callback: impl FnOnce(&mut MergeableTx<'_, S>) -> Result<T, Error>,
    ) -> Result<(T, TxId), Error> {
        let mut tx = self.mergeable_tx()?;
        let value = callback(&mut tx)?;
        let tx_id = tx.commit()?;
        Ok((value, tx_id))
    }

    /// Build a mergeable transaction authored and permission-checked as `author`.
    pub fn mergeable_tx_for_identity(&self, author: AuthorId) -> Result<MergeableTx<'_, S>, Error> {
        let tx_id = OpenBatchId::new();
        self.begin_mergeable_for_identity(tx_id, author)?;
        Ok(MergeableTx {
            db: self,
            tx_id,
            committed: false,
        })
    }

    /// Run `callback` in a mergeable transaction authored and permission-checked as `author`.
    ///
    /// If `callback` returns an error, the transaction is dropped without committing.
    pub fn transaction_for_identity<T>(
        &self,
        author: AuthorId,
        callback: impl FnOnce(&mut MergeableTx<'_, S>) -> Result<T, Error>,
    ) -> Result<(T, TxId), Error> {
        let mut tx = self.mergeable_tx_for_identity(author)?;
        let value = callback(&mut tx)?;
        let tx_id = tx.commit()?;
        Ok((value, tx_id))
    }

    /// Publish an immutable schema-version payload through the catalogue lane.
    pub fn publish_schema(&self, schema: SchemaVersion) -> Result<Vec<SyncMessage>, Error> {
        self.check_catalogue_admin()?;
        self.node
            .node
            .borrow_mut()
            .apply_trusted_catalogue_message(SyncMessage::PublishSchema {
                author: self.identity.author,
                schema: Box::new(schema),
            })
            .map_err(Into::into)
    }

    /// Atomically publish a non-genesis schema and its lineage-defining lens.
    pub fn publish_schema_with_lens(
        &self,
        catalogue_seq: u64,
        publication: SchemaLineagePublication,
    ) -> Result<Vec<SyncMessage>, Error> {
        self.check_catalogue_admin()?;
        self.node
            .node
            .borrow_mut()
            .apply_trusted_catalogue_message(SyncMessage::PublishSchemaWithLens {
                author: self.identity.author,
                catalogue_seq,
                publication: Box::new(publication),
            })
            .map_err(Into::into)
    }

    /// Publish an immutable migration lens through the catalogue lane.
    pub fn publish_lens(&self, lens: MigrationLens) -> Result<Vec<SyncMessage>, Error> {
        self.check_catalogue_admin()?;
        self.node
            .node
            .borrow_mut()
            .apply_trusted_catalogue_message(SyncMessage::PublishLens {
                author: self.identity.author,
                lens,
            })
            .map_err(Into::into)
    }

    /// Set the current write-schema pointer through the catalogue lane.
    pub fn set_current_write_schema(
        &self,
        pointer: CurrentWriteSchema,
    ) -> Result<Vec<SyncMessage>, Error> {
        self.check_catalogue_admin()?;
        self.node
            .node
            .borrow_mut()
            .apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
                author: self.identity.author,
                pointer,
            })
            .map_err(Into::into)
    }

    /// Set whether this authority may settle session-scoped reads and writes.
    /// Enabling it rehydrates all live subscriber views.
    pub fn set_permissions_ready(&self, ready: bool) -> Result<(), Error> {
        self.node.set_permissions_ready(ready)
    }

    /// Return the current write-schema pointer known to this database.
    pub fn current_write_schema(&self) -> Result<CurrentWriteSchema, Error> {
        self.node
            .node
            .borrow()
            .current_write_schema()
            .map_err(Into::into)
    }

    /// Return a published schema-version payload known to this database.
    pub fn catalogue_schema(&self, schema: SchemaVersionId) -> Option<JazzSchema> {
        self.node
            .node
            .borrow()
            .catalogue_schemas()
            .get(&schema)
            .map(|schema| schema.schema.clone())
    }

    /// Highest contiguously activated authoritative catalogue position.
    pub fn active_catalogue_seq(&self) -> u64 {
        self.node.node.borrow().active_catalogue_seq()
    }

    /// Return a published migration lens known to this database.
    pub fn catalogue_lens(&self, lens: crate::ids::MigrationLensId) -> Option<MigrationLens> {
        self.node
            .node
            .borrow()
            .catalogue_lenses()
            .get(&lens)
            .cloned()
    }

    /// Open a mergeable transaction and return its id.
    ///
    /// The caller owns this transaction's lifetime and must commit it with
    /// [`Db::commit_mergeable_handle`] or abandon it with
    /// [`Db::abandon_transaction_handle`]. Perform its writes through a
    /// [`MergeableTxRef`], which can be reconstructed from this id for each
    /// foreign-function call. Rust callers that want RAII should use
    /// [`Db::mergeable_tx`] instead.
    pub fn begin_mergeable(&self, id: OpenBatchId) -> Result<(), Error> {
        self.node
            .node
            .borrow_mut()
            .open_mergeable(id, self.identity.author, None)
            .map_err(Into::into)
    }

    /// Open a mergeable transaction authored and permission-checked as `author`.
    ///
    /// See [`Db::begin_mergeable`] for ownership and operation-handle guidance.
    pub fn begin_mergeable_for_identity(
        &self,
        id: OpenBatchId,
        author: AuthorId,
    ) -> Result<(), Error> {
        self.node
            .node
            .borrow_mut()
            .open_mergeable(id, author, Some(author))
            .map_err(Into::into)
    }

    /// Return a non-owning operations handle for an already-open mergeable transaction.
    ///
    /// This handle never closes the transaction when dropped, so it is suitable
    /// for a single call in a binding that retains `tx_id` between calls. Its
    /// CRUD API is defined by [`MergeableTxOps`] and is shared with the owning
    /// [`MergeableTx`] handle.
    pub fn mergeable_tx_ref(&self, tx_id: OpenBatchId) -> MergeableTxRef<'_, S> {
        MergeableTxRef { db: self, tx_id }
    }

    fn stage_mergeable_insert(
        &self,
        tx_id: OpenBatchId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        let now_ms = Some(now_ms.unwrap_or_else(|| self.next_now_ms()));
        let cells = self.apply_insert_defaults(table, cells)?;
        self.node
            .node
            .borrow_mut()
            .tx_write_mergeable_in_schema(
                tx_id,
                self.schema_version_id,
                table,
                row,
                cells,
                None,
                Vec::new(),
                now_ms,
                false,
            )
            .map_err(Into::into)
    }

    fn stage_mergeable_update(
        &self,
        tx_id: OpenBatchId,
        table: &str,
        row: RowUuid,
        patch: RowCells,
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        let now_ms = Some(now_ms.unwrap_or_else(|| self.next_now_ms()));
        self.node
            .node
            .borrow_mut()
            .tx_patch_mergeable_in_schema(tx_id, self.schema_version_id, table, row, patch, now_ms)
            .map_err(Into::into)
    }

    fn stage_mergeable_delete(
        &self,
        tx_id: OpenBatchId,
        table: &str,
        row: RowUuid,
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        let now_ms = Some(now_ms.unwrap_or_else(|| self.next_now_ms()));
        self.node
            .node
            .borrow_mut()
            .tx_write_mergeable_in_schema(
                tx_id,
                self.schema_version_id,
                table,
                row,
                BTreeMap::new(),
                Some(DeletionEvent::Deleted),
                Vec::new(),
                now_ms,
                false,
            )
            .map_err(Into::into)
    }

    fn stage_mergeable_restore(
        &self,
        tx_id: OpenBatchId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        let now_ms = Some(now_ms.unwrap_or_else(|| self.next_now_ms()));
        let cells = self.apply_insert_defaults(table, cells)?;
        let mut node = self.node.node.borrow_mut();
        let content_parents = node
            .local_content_winner_tx_id(table, row)?
            .into_iter()
            .collect();
        let deletion_parents = node
            .local_deletion_winner_tx_id(table, row)?
            .into_iter()
            .collect();
        node.tx_write_mergeable_in_schema(
            tx_id,
            self.schema_version_id,
            table,
            row,
            cells,
            None,
            content_parents,
            now_ms,
            true,
        )?;
        node.tx_write_mergeable_in_schema(
            tx_id,
            self.schema_version_id,
            table,
            row,
            BTreeMap::new(),
            Some(DeletionEvent::Restored),
            deletion_parents,
            now_ms,
            true,
        )?;
        Ok(())
    }

    /// Commit an owned mergeable transaction handle.
    pub fn commit_mergeable_handle(&self, open_tx_id: OpenBatchId) -> Result<TxId, Error> {
        let tx_id = self
            .node
            .node
            .borrow_mut()
            .commit_mergeable_open(open_tx_id, || self.next_now_ms())?;
        self.finalize_local_commit(tx_id)?;
        self.refresh_subscriptions()?;
        Ok(tx_id)
    }

    /// Abandon an owned open transaction handle.
    pub fn abandon_transaction_handle(&self, open_tx_id: OpenBatchId) -> Result<(), Error> {
        self.node
            .node
            .borrow_mut()
            .abandon_tx(open_tx_id)
            .map_err(Into::into)
    }

    /// Open an exclusive transaction over the current local snapshot.
    ///
    /// This is the owning, RAII flavour. It abandons an uncommitted transaction
    /// on drop. Use [`Db::exclusive_tx_ref`] only when another layer retains the
    /// `OpenBatchId` and owns that lifetime explicitly.
    pub fn exclusive_tx(&self) -> Result<ExclusiveTx<'_, S>, Error> {
        let tx_id = OpenBatchId::new();
        self.open_exclusive_handle(tx_id)?;
        Ok(ExclusiveTx {
            db: self,
            tx_id,
            committed: false,
        })
    }

    /// Open an exclusive transaction and return its id.
    ///
    /// The caller owns this transaction's lifetime and must commit it with
    /// [`Db::commit_exclusive_handle`] or abandon it with
    /// [`Db::abandon_exclusive_handle`]. Perform its operations through an
    /// [`ExclusiveTxRef`]. Rust callers that want RAII should use
    /// [`Db::exclusive_tx`] instead.
    pub fn begin_exclusive(&self, id: OpenBatchId) -> Result<(), Error> {
        self.open_exclusive_handle(id)
    }

    /// Return a non-owning operations handle for an already-open exclusive transaction.
    ///
    /// This handle never closes the transaction when dropped, so it is suitable
    /// for a single call in a binding that retains `tx_id` between calls. Its
    /// CRUD API is defined by [`ExclusiveTxOps`] and is shared with the owning
    /// [`ExclusiveTx`] handle.
    pub fn exclusive_tx_ref(&self, tx_id: OpenBatchId) -> ExclusiveTxRef<'_, S> {
        ExclusiveTxRef { db: self, tx_id }
    }

    fn exclusive_read(
        &self,
        tx_id: OpenBatchId,
        table: &str,
        row: RowUuid,
    ) -> Result<Option<RowCells>, Error> {
        self.node
            .node
            .borrow_mut()
            .tx_read_in_schema(tx_id, self.schema_version_id, table, row)
            .map_err(Into::into)
    }

    fn transaction_all(
        &self,
        tx_id: OpenBatchId,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.transaction_all_in_authorization_mode(
            tx_id,
            prepared,
            self.identity.author,
            opts,
            QueryAuthorizationMode::ClientLocal,
        )
    }

    pub(crate) fn transaction_all_for_identity(
        &self,
        tx_id: OpenBatchId,
        prepared: &PreparedQuery,
        author: AuthorId,
        opts: ReadOpts,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.transaction_all_in_authorization_mode(
            tx_id,
            prepared,
            author,
            opts,
            QueryAuthorizationMode::TrustedServing,
        )
    }

    fn transaction_all_in_authorization_mode(
        &self,
        tx_id: OpenBatchId,
        prepared: &PreparedQuery,
        author: AuthorId,
        opts: ReadOpts,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<Vec<CurrentRow>, Error> {
        ensure_default_read_view(&opts)?;
        let mut node = self.node.node.borrow_mut();
        match authorization_mode {
            QueryAuthorizationMode::ClientLocal => node
                .tx_query_with_options(
                    tx_id,
                    &prepared.shape,
                    &prepared.binding,
                    opts.include_deleted,
                )
                .map_err(Into::into),
            QueryAuthorizationMode::TrustedServing => node
                .tx_query_for_identity_with_options(
                    tx_id,
                    &prepared.shape,
                    &prepared.binding,
                    author,
                    opts.include_deleted,
                )
                .map_err(Into::into),
        }
    }

    fn stage_exclusive_insert(
        &self,
        tx_id: OpenBatchId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<(), Error> {
        let now_ms = self.next_now_ms();
        let cells = self.apply_insert_defaults(table, cells)?;
        self.node
            .node
            .borrow_mut()
            .tx_write_in_schema_at_ms(
                tx_id,
                self.schema_version_id,
                table,
                row,
                cells,
                None,
                Some(now_ms),
            )
            .map_err(Into::into)
    }

    fn stage_exclusive_delete(
        &self,
        tx_id: OpenBatchId,
        table: &str,
        row: RowUuid,
    ) -> Result<(), Error> {
        let now_ms = self.next_now_ms();
        self.node
            .node
            .borrow_mut()
            .tx_write_in_schema_at_ms(
                tx_id,
                self.schema_version_id,
                table,
                row,
                BTreeMap::<String, Value>::new(),
                Some(DeletionEvent::Deleted),
                Some(now_ms),
            )
            .map_err(Into::into)
    }

    fn stage_exclusive_restore(
        &self,
        tx_id: OpenBatchId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<(), Error> {
        let now_ms = self.next_now_ms();
        let cells = self.apply_insert_defaults(table, cells)?;
        let mut node = self.node.node.borrow_mut();
        // Restore needs one content version and one deletion-register version:
        // `tx_write` rejects a version carrying both. The layers have separate
        // winners and parent chains; see `restore`'s `local_*_winner_tx_id` pair.
        // Keep this staged form aligned with the committed restore path.
        node.tx_write_in_schema_at_ms(
            tx_id,
            self.schema_version_id,
            table,
            row,
            cells,
            None,
            Some(now_ms),
        )?;
        node.tx_write_in_schema_at_ms(
            tx_id,
            self.schema_version_id,
            table,
            row,
            BTreeMap::<String, Value>::new(),
            Some(DeletionEvent::Restored),
            Some(now_ms),
        )?;
        Ok(())
    }

    /// Commit an owned exclusive transaction handle.
    pub fn commit_exclusive_handle(&self, open_tx_id: OpenBatchId) -> Result<TxId, Error> {
        let (tx_id, unit) = self.node.node.borrow_mut().commit_exclusive(
            open_tx_id,
            self.identity.author,
            self.next_now_ms(),
        )?;
        self.finalize_local_exclusive_unit(tx_id, unit)?;
        self.refresh_subscriptions()?;
        Ok(tx_id)
    }

    /// Abandon an owned exclusive transaction handle.
    pub fn abandon_exclusive_handle(&self, open_tx_id: OpenBatchId) -> Result<(), Error> {
        self.abandon_transaction_handle(open_tx_id)
    }

    pub(crate) fn open_exclusive_handle(&self, id: OpenBatchId) -> Result<(), Error> {
        self.node
            .node
            .borrow_mut()
            .open_exclusive_for_identity(id, self.identity.author)
            .map_err(Into::into)
    }

    /// Restore a row locally, applying defaults for omitted columns.
    ///
    /// ```rust
    /// # use jazz::db::doctest_support::{block_on, open_todos_db, todo_cells};
    /// # use jazz::ids::RowUuid;
    /// let db = block_on(open_todos_db())?;
    /// let todo = RowUuid::from_bytes([1; 16]);
    /// db.insert_with_id("todos", todo, todo_cells("archived", false))?;
    /// db.delete("todos", todo)?;
    ///
    /// db.restore("todos", todo, todo_cells("restored", false))?;
    /// let todos = db.prepare_query(&db.table("todos"))?;
    /// assert_eq!(db.one(&todos)?.unwrap().row_uuid(), todo);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn restore(
        &self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        let cells = self.apply_insert_defaults(table, cells)?;
        self.ensure_row_deleted(table, row, self.identity.author)?;
        let (content_parents, deletion_parents) = {
            let mut node = self.node.node.borrow_mut();
            let content_parents = node
                .local_content_winner_tx_id(table, row)?
                .into_iter()
                .collect::<Vec<_>>();
            let deletion_parents = node
                .local_deletion_winner_tx_id(table, row)?
                .into_iter()
                .collect::<Vec<_>>();
            (content_parents, deletion_parents)
        };
        let tx_id = self
            .node
            .node
            .borrow_mut()
            .commit_mergeable_many_in_schema(
                self.schema_version_id,
                vec![
                    MergeableCommit::new(table, row, self.next_now_ms())
                        .made_by(self.identity.author)
                        .parents(content_parents)
                        .cells(cells),
                    MergeableCommit::new(table, row, self.next_now_ms())
                        .made_by(self.identity.author)
                        .parents(deletion_parents)
                        .cells(BTreeMap::<String, Value>::new())
                        .deletion(DeletionEvent::Restored),
                ],
            )?;
        let local_tier = self.finalize_local_commit(tx_id)?;
        self.refresh_subscriptions()?;
        Ok(WriteHandle {
            node: Rc::downgrade(&self.node.node),
            row_uuid: row,
            tx_id,
            local_tier,
        })
    }

    /// Restore a row while evaluating write policy as `identity`.
    pub fn restore_for_identity(
        &self,
        identity: AuthorId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<S>, Error> {
        let cells = self.apply_insert_defaults(table, cells)?;
        self.ensure_row_deleted(table, row, identity)?;
        let (content_parents, deletion_parents) = {
            let mut node = self.node.node.borrow_mut();
            let content_parents = node
                .local_content_winner_tx_id(table, row)?
                .into_iter()
                .collect::<Vec<_>>();
            let deletion_parents = node
                .local_deletion_winner_tx_id(table, row)?
                .into_iter()
                .collect::<Vec<_>>();
            (content_parents, deletion_parents)
        };
        let tx_id = self
            .node
            .node
            .borrow_mut()
            .commit_mergeable_many_in_schema(
                self.schema_version_id,
                vec![
                    MergeableCommit::new(table, row, self.next_now_ms())
                        .made_by(identity)
                        .permission_subject(identity)
                        .parents(content_parents)
                        .cells(cells),
                    MergeableCommit::new(table, row, self.next_now_ms())
                        .made_by(identity)
                        .permission_subject(identity)
                        .parents(deletion_parents)
                        .cells(BTreeMap::<String, Value>::new())
                        .deletion(DeletionEvent::Restored),
                ],
            )?;
        let local_tier = self.finalize_local_commit(tx_id)?;
        self.refresh_subscriptions()?;
        Ok(WriteHandle {
            node: Rc::downgrade(&self.node.node),
            row_uuid: row,
            tx_id,
            local_tier,
        })
    }

    fn write_mergeable_as_session_subject(
        &self,
        made_by: AuthorId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        parents: Vec<TxId>,
        deletion: Option<DeletionEvent>,
    ) -> Result<WriteHandle<S>, Error> {
        self.check_attribution_allowed(made_by)?;
        self.write_mergeable(
            made_by,
            Some(self.identity.author),
            table,
            row,
            cells,
            parents,
            deletion,
        )
    }

    fn write_mergeable_as_session_subject_with_authored_columns(
        &self,
        made_by: AuthorId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        parents: Vec<TxId>,
        deletion: Option<DeletionEvent>,
        authored_columns: BTreeSet<String>,
    ) -> Result<WriteHandle<S>, Error> {
        self.check_attribution_allowed(made_by)?;
        self.write_mergeable_with_authored_columns(
            made_by,
            Some(self.identity.author),
            table,
            row,
            cells,
            parents,
            deletion,
            authored_columns,
        )
    }

    /// Restore a row with an explicit millisecond provenance time.
    pub fn restore_at_ms(
        &self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        now_ms: u64,
    ) -> Result<WriteHandle<S>, Error> {
        let cells = self.apply_insert_defaults(table, cells)?;
        self.ensure_row_deleted(table, row, self.identity.author)?;
        let (content_parents, deletion_parents) = self.row_layer_parents(table, row)?;
        let tx_id = self
            .node
            .node
            .borrow_mut()
            .commit_mergeable_many_in_schema(
                self.schema_version_id,
                vec![
                    MergeableCommit::new(table, row, now_ms)
                        .made_by(self.identity.author)
                        .parents(content_parents)
                        .cells(cells),
                    MergeableCommit::new(table, row, now_ms)
                        .made_by(self.identity.author)
                        .parents(deletion_parents)
                        .cells(BTreeMap::<String, Value>::new())
                        .deletion(DeletionEvent::Restored),
                ],
            )?;
        let local_tier = self.finalize_local_commit(tx_id)?;
        self.refresh_subscriptions()?;
        Ok(WriteHandle {
            node: Rc::downgrade(&self.node.node),
            row_uuid: row,
            tx_id,
            local_tier,
        })
    }

    /// Restore a row for `identity` with an explicit millisecond provenance time.
    pub fn restore_for_identity_at_ms(
        &self,
        identity: AuthorId,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        now_ms: u64,
    ) -> Result<WriteHandle<S>, Error> {
        let cells = self.apply_insert_defaults(table, cells)?;
        self.ensure_row_deleted(table, row, identity)?;
        let (content_parents, deletion_parents) = self.row_layer_parents(table, row)?;
        let tx_id = self
            .node
            .node
            .borrow_mut()
            .commit_mergeable_many_in_schema(
                self.schema_version_id,
                vec![
                    MergeableCommit::new(table, row, now_ms)
                        .made_by(identity)
                        .permission_subject(identity)
                        .parents(content_parents)
                        .cells(cells),
                    MergeableCommit::new(table, row, now_ms)
                        .made_by(identity)
                        .permission_subject(identity)
                        .parents(deletion_parents)
                        .cells(BTreeMap::<String, Value>::new())
                        .deletion(DeletionEvent::Restored),
                ],
            )?;
        let local_tier = self.finalize_local_commit(tx_id)?;
        self.refresh_subscriptions()?;
        Ok(WriteHandle {
            node: Rc::downgrade(&self.node.node),
            row_uuid: row,
            tx_id,
            local_tier,
        })
    }

    fn write_mergeable(
        &self,
        made_by: AuthorId,
        permission_subject: Option<AuthorId>,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        parents: Vec<TxId>,
        deletion: Option<DeletionEvent>,
    ) -> Result<WriteHandle<S>, Error> {
        self.write_mergeable_at_ms(
            made_by,
            permission_subject,
            table,
            row,
            cells,
            parents,
            deletion,
            self.next_now_ms(),
        )
    }

    fn write_mergeable_at_ms(
        &self,
        made_by: AuthorId,
        permission_subject: Option<AuthorId>,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        parents: Vec<TxId>,
        deletion: Option<DeletionEvent>,
        now_ms: u64,
    ) -> Result<WriteHandle<S>, Error> {
        self.write_mergeable_at_ms_with_authorship(
            made_by,
            permission_subject,
            table,
            row,
            cells,
            parents,
            deletion,
            None,
            now_ms,
        )
    }

    fn write_mergeable_with_authored_columns(
        &self,
        made_by: AuthorId,
        permission_subject: Option<AuthorId>,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        parents: Vec<TxId>,
        deletion: Option<DeletionEvent>,
        authored_columns: BTreeSet<String>,
    ) -> Result<WriteHandle<S>, Error> {
        self.write_mergeable_at_ms_with_authorship(
            made_by,
            permission_subject,
            table,
            row,
            cells,
            parents,
            deletion,
            Some(authored_columns),
            self.next_now_ms(),
        )
    }

    fn write_mergeable_at_ms_with_authorship(
        &self,
        made_by: AuthorId,
        permission_subject: Option<AuthorId>,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        parents: Vec<TxId>,
        deletion: Option<DeletionEvent>,
        authored_columns: Option<BTreeSet<String>>,
        now_ms: u64,
    ) -> Result<WriteHandle<S>, Error> {
        let operation = if deletion == Some(DeletionEvent::Deleted) {
            "DELETE"
        } else if parents.is_empty() {
            "INSERT"
        } else {
            "UPDATE"
        };
        let cells = if operation == "INSERT" {
            self.apply_insert_defaults(table, cells)?
        } else {
            cells
        };
        let mut commit = MergeableCommit::new(table, row, now_ms)
            .made_by(made_by)
            .parents(parents)
            .cells(cells);
        if let Some(authored_columns) = authored_columns {
            commit = commit.authored_columns(authored_columns);
        }
        if let Some(subject) = permission_subject {
            commit = commit.permission_subject(subject);
        }
        if let Some(deletion) = deletion {
            commit = commit.deletion(deletion);
        }
        // Db is an untrusted client: structurally valid writes are staged and
        // sent optimistically. A serving authority assigns the policy fate.
        let tx_id = self
            .node
            .node
            .borrow_mut()
            .commit_mergeable_in_schema(self.schema_version_id, commit)?;
        let local_tier = self.finalize_local_commit(tx_id)?;
        self.refresh_subscriptions()?;
        Ok(WriteHandle {
            node: Rc::downgrade(&self.node.node),
            row_uuid: row,
            tx_id,
            local_tier,
        })
    }

    fn check_attribution_allowed(&self, made_by: AuthorId) -> Result<(), Error> {
        if made_by == self.identity.author {
            return Ok(());
        }
        Err(Error::new(
            ErrorCode::WriteRejected,
            "attribution requires a trusted serving node",
        ))
    }

    fn check_catalogue_admin(&self) -> Result<(), Error> {
        if self.identity.author == AuthorId::SYSTEM {
            return Ok(());
        }
        Err(Error::new(
            ErrorCode::Protocol,
            "catalogue updates require a serving Node",
        ))
    }

    /// Finalize a locally-committed exclusive transaction. A `Core` authority
    /// validates and accepts/rejects it now, using the in-memory commit unit
    /// (which still carries `base_snapshot` and the read sets); other roles
    /// queue it for upstream, leaving it Pending/Local.
    fn finalize_local_exclusive_unit(
        &self,
        tx_id: TxId,
        unit: SyncMessage,
    ) -> Result<DurabilityTier, Error> {
        self.node.queue_pending_upload(tx_id, Some(unit));
        Ok(self.node.node.borrow().authored_commit_durability())
    }

    /// Client writes stay pending at this runtime's authored durability until
    /// peer durability or fate updates arrive over a connection.
    fn finalize_local_commit(&self, tx_id: TxId) -> Result<DurabilityTier, Error> {
        self.node.queue_pending_upload(tx_id, None);
        Ok(self.node.node.borrow().authored_commit_durability())
    }

    fn next_now_ms(&self) -> u64 {
        let next = self.next_now_ms.get();
        self.next_now_ms.set(next + 1);
        next
    }

    fn current_write_schema_for_query(&self) -> Result<(JazzSchema, SchemaVersionId), Error> {
        if self.schema_view_is_fixed {
            return Ok((self.schema.clone(), self.schema_version_id));
        }
        let node = self.node.node.borrow();
        let current = node.current_write_schema().map_err(Error::from)?;
        if current.schema == self.schema_version_id {
            return Ok((self.schema.clone(), self.schema_version_id));
        }
        node.catalogue_schemas()
            .get(&current.schema)
            .map(|schema| (schema.schema.clone(), current.schema))
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::Schema,
                    format!(
                        "current write schema {:?} is missing from catalogue",
                        current.schema
                    ),
                )
            })
    }

    fn table_schema(&self, table: &str) -> Result<&TableSchema, Error> {
        self.schema
            .tables
            .iter()
            .find(|candidate| candidate.name == table)
            .ok_or_else(|| Error::new(ErrorCode::Schema, format!("unknown table {table}")))
    }

    fn apply_insert_defaults(&self, table: &str, mut cells: RowCells) -> Result<RowCells, Error> {
        let table_schema = self.table_schema(table)?;
        for column in &table_schema.columns {
            if !cells.contains_key(&column.name) {
                if let Some(default) = &column.default {
                    cells.insert(
                        column.name.clone(),
                        default_cell_for_column_type(&column.column_type, default),
                    );
                }
            }
        }
        Ok(cells)
    }

    fn upsert_target_for_client_identity(
        &self,
        table: &str,
        row: RowUuid,
        identity: AuthorId,
    ) -> Result<Option<CurrentRow>, Error> {
        let target = self.local_row_for_client_identity(table, row, identity)?;
        if target.is_some() {
            return Ok(target);
        }
        // A policy-filtered point read cannot by itself distinguish an absent
        // row from an existing row hidden from this identity. Upsert needs
        // exactly that distinction: a genuinely absent target follows INSERT
        // policy and does not require read permission, while merging into an
        // existing target must not expose or copy hidden cells.
        if self.local_current_row(table, row)?.is_none() {
            return Ok(None);
        }
        if identity == AuthorId::SYSTEM || self.table_schema(table)?.read_policy.is_none() {
            return Ok(None);
        }
        Err(read_for_write_denied("UPSERT", table))
    }

    fn upsert_target_for_trusted_identity(
        &self,
        table: &str,
        row: RowUuid,
        identity: AuthorId,
    ) -> Result<Option<CurrentRow>, Error> {
        let target = self.local_row_for_trusted_identity(table, row, identity)?;
        if target.is_some() {
            return Ok(target);
        }
        // Trusted serving evaluates the identity's real read policy before
        // merging an existing row. A hidden existing row must not be treated
        // as an insert target.
        if self.local_current_row(table, row)?.is_none() {
            return Ok(None);
        }
        if identity == AuthorId::SYSTEM || self.table_schema(table)?.read_policy.is_none() {
            return Ok(None);
        }
        Err(read_for_write_denied("UPSERT", table))
    }

    /// Read one locally-current row by primary key without evaluating a table
    /// query. This backend-scoped helper is used by import/upsert bridges that
    /// already operate with database authority and need an O(row) existence
    /// check before staging a write.
    pub fn local_current_row(
        &self,
        table: &str,
        row: RowUuid,
    ) -> Result<Option<CurrentRow>, Error> {
        self.table_schema(table)?;
        Ok(self.node.node.borrow_mut().local_current_row(table, row)?)
    }

    fn ensure_row_absent(
        &self,
        table: &str,
        row: RowUuid,
        _identity: AuthorId,
    ) -> Result<(), Error> {
        self.table_schema(table)?;
        let (content_parent, deletion_parent) = {
            let mut node = self.node.node.borrow_mut();
            (
                node.local_content_winner_tx_id(table, row)?,
                node.local_deletion_winner_tx_id(table, row)?,
            )
        };
        if deletion_parent.is_some() {
            return Err(row_already_deleted(row));
        }
        if content_parent.is_some() {
            return Err(Error::new(
                ErrorCode::WriteRejected,
                format!("encoding error: object already exists: {}", row.0),
            ));
        }
        Ok(())
    }

    fn ensure_row_deleted(
        &self,
        table: &str,
        row: RowUuid,
        _identity: AuthorId,
    ) -> Result<(), Error> {
        self.table_schema(table)?;
        let deleted = self
            .node
            .node
            .borrow_mut()
            .local_deletion_winner_tx_id(table, row)?
            .is_some();
        if deleted {
            Ok(())
        } else {
            Err(Error::new(
                ErrorCode::WriteRejected,
                format!("row not deleted: {}", row.0),
            ))
        }
    }

    fn ensure_row_not_deleted(&self, table: &str, row: RowUuid) -> Result<(), Error> {
        self.table_schema(table)?;
        let deleted = self
            .node
            .node
            .borrow_mut()
            .local_deletion_winner_tx_id(table, row)?
            .is_some();
        if deleted {
            Err(row_already_deleted(row))
        } else {
            Ok(())
        }
    }

    fn row_layer_parents(
        &self,
        table: &str,
        row: RowUuid,
    ) -> Result<(Vec<TxId>, Vec<TxId>), Error> {
        let mut node = self.node.node.borrow_mut();
        let content_parents = node
            .local_content_winner_tx_id(table, row)?
            .into_iter()
            .collect::<Vec<_>>();
        let deletion_parents = node
            .local_deletion_winner_tx_id(table, row)?
            .into_iter()
            .collect::<Vec<_>>();
        Ok((content_parents, deletion_parents))
    }

    fn local_row_for_client_identity(
        &self,
        table: &str,
        row: RowUuid,
        identity: AuthorId,
    ) -> Result<Option<CurrentRow>, Error> {
        let query = self.prepare_query(&Query::from(table))?;
        Ok(self
            .node
            .node
            .borrow_mut()
            .query_rows_for_client(
                &query.shape,
                &query.binding,
                DurabilityTier::Local,
                identity,
            )?
            .into_iter()
            .find(|candidate| candidate.row_uuid() == row))
    }

    fn local_row_for_trusted_identity(
        &self,
        table: &str,
        row: RowUuid,
        identity: AuthorId,
    ) -> Result<Option<CurrentRow>, Error> {
        let query = self.prepare_query(&Query::from(table))?;
        Ok(self
            .node
            .node
            .borrow_mut()
            .query_rows_with_prepared_plan_for_identity(
                &query.shape,
                &query.binding,
                DurabilityTier::Local,
                None,
                identity,
            )?
            .into_iter()
            .find(|candidate| candidate.row_uuid() == row))
    }

    fn no_op_update_handle_for_client(
        &self,
        table: &str,
        row: RowUuid,
        identity: AuthorId,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_not_deleted(table, row)?;
        let existing = self
            .local_row_for_client_identity(table, row, identity)?
            .ok_or_else(|| read_for_write_denied("partial UPDATE", table))?;
        let tx_id = self
            .node
            .node
            .borrow_mut()
            .current_row_tx_id(&existing)
            .ok_or_else(|| Error::new(ErrorCode::NotObserved, "current row has no transaction"))?;
        let local_tier = self.write_state(tx_id)?.durability;
        Ok(WriteHandle {
            node: Rc::downgrade(&self.node.node),
            row_uuid: row,
            tx_id,
            local_tier,
        })
    }

    fn no_op_update_handle_for_identity(
        &self,
        table: &str,
        row: RowUuid,
        identity: AuthorId,
    ) -> Result<WriteHandle<S>, Error> {
        self.ensure_row_not_deleted(table, row)?;
        let existing = self
            .local_row_for_trusted_identity(table, row, identity)?
            .ok_or_else(|| read_for_write_denied("partial UPDATE", table))?;
        let tx_id = self
            .node
            .node
            .borrow_mut()
            .current_row_tx_id(&existing)
            .ok_or_else(|| Error::new(ErrorCode::NotObserved, "current row has no transaction"))?;
        let local_tier = self.write_state(tx_id)?.durability;
        Ok(WriteHandle {
            node: Rc::downgrade(&self.node.node),
            row_uuid: row,
            tx_id,
            local_tier,
        })
    }

    fn merge_existing_cells(
        &self,
        table: &str,
        row: RowUuid,
        patch: RowCells,
    ) -> Result<(RowCells, Option<TxId>, BTreeSet<String>), Error> {
        self.merge_existing_cells_for_client_identity(table, row, patch, self.identity.author)
    }

    fn merge_existing_cells_for_client_identity(
        &self,
        table: &str,
        row: RowUuid,
        patch: RowCells,
        identity: AuthorId,
    ) -> Result<(RowCells, Option<TxId>, BTreeSet<String>), Error> {
        let table_schema = self.table_schema(table)?;
        self.ensure_row_not_deleted(table, row)?;
        if table_schema
            .columns
            .iter()
            .all(|column| patch.contains_key(&column.name))
        {
            // A full-row write does not observe user data. Its causal parent is
            // storage bookkeeping, so obtain only that parent with system
            // authority rather than evaluating the writer's read policy.
            let parent = self
                .local_current_row(table, row)?
                .as_ref()
                .and_then(|existing| self.node.node.borrow_mut().current_row_tx_id(existing));
            let authored_columns = patch.keys().cloned().collect();
            return Ok((patch, parent, authored_columns));
        }
        let mut cells = BTreeMap::new();
        let existing = self
            .local_row_for_client_identity(table, row, identity)?
            .ok_or_else(|| read_for_write_denied("partial UPDATE", table))?;
        for column in &table_schema.columns {
            if let Some(value) = existing.cell(table_schema, &column.name) {
                cells.insert(
                    column.name.clone(),
                    default_cell_for_column_type(&column.column_type, &value),
                );
            }
        }
        let parent = self.node.node.borrow_mut().current_row_tx_id(&existing);
        let authored_columns = patch.keys().cloned().collect();
        cells.extend(patch);
        Ok((cells, parent, authored_columns))
    }

    fn merge_existing_cells_for_identity(
        &self,
        table: &str,
        row: RowUuid,
        patch: RowCells,
        identity: AuthorId,
    ) -> Result<(RowCells, Option<TxId>, BTreeSet<String>), Error> {
        let table_schema = self.table_schema(table)?;
        self.ensure_row_not_deleted(table, row)?;
        if table_schema
            .columns
            .iter()
            .all(|column| patch.contains_key(&column.name))
        {
            let parent = self
                .local_current_row(table, row)?
                .as_ref()
                .and_then(|existing| self.node.node.borrow_mut().current_row_tx_id(existing));
            let authored_columns = patch.keys().cloned().collect();
            return Ok((patch, parent, authored_columns));
        }
        if self.authorize_read_for_identity(table, row, identity)? != PermissionAdvice::Allowed {
            return Err(read_for_write_denied("partial UPDATE", table));
        }
        let mut cells = BTreeMap::new();
        let existing = self
            .local_row_for_trusted_identity(table, row, identity)?
            .ok_or_else(|| read_for_write_denied("partial UPDATE", table))?;
        for column in &table_schema.columns {
            if let Some(value) = existing.cell(table_schema, &column.name) {
                cells.insert(
                    column.name.clone(),
                    default_cell_for_column_type(&column.column_type, &value),
                );
            }
        }
        let parent = self.node.node.borrow_mut().current_row_tx_id(&existing);
        let authored_columns = patch.keys().cloned().collect();
        cells.extend(patch);
        Ok((cells, parent, authored_columns))
    }

    /// Attach this `Db` to an upstream peer over a binding-supplied transport.
    ///
    /// The returned [`PeerConnection`] carries this Db's subscriptions upstream
    /// under this Db's own identity and applies the updates that come back.
    /// An unfated commit unit is interpreted from this receiving Db's role: an
    /// ordinary Local Db records it as Pending/Local, while the structurally
    /// separate history-complete path remains the Core authority.
    /// The binding drives it by calling [`PeerConnection::tick`] (or
    /// [`Db::tick`]) whenever it has staged inbound bytes or wants to flush.
    pub fn connect_upstream(
        &self,
        transport: Box<dyn Transport>,
    ) -> Rc<RefCell<PeerConnection<S>>> {
        self.node.connect_upstream(transport)
    }

    /// Install or clear the scheduler used to wake this database's live peer
    /// connections when local writes, subscription registrations, or transport
    /// events create sync work.
    pub fn set_tick_scheduler(&self, scheduler: Option<Rc<dyn TickScheduler>>) {
        self.node.set_scheduler(scheduler);
    }

    /// Configure automatic edge-cache byte-budget eviction.
    ///
    /// `None` disables automatic eviction and preserves the historical manual
    /// `evict_cold` behavior.
    pub fn set_edge_cache_budget(&self, budget: Option<EdgeCacheBudget>) {
        self.node.set_edge_cache_budget(budget);
    }

    /// Ask the installed scheduler to service pending peer-connection work.
    pub fn schedule_tick(&self, urgency: TickUrgency) {
        self.node.schedule_tick(urgency);
    }

    /// Request a one-shot permission decision from the authenticated upstream
    /// serving authority. Dropping the returned future cancels local delivery;
    /// late or replayed responses are ignored by request id.
    pub fn request_permission_advice(
        &self,
        action: PermissionAdviceAction,
    ) -> PermissionAdviceFuture {
        self.node.request_permission_advice(action)
    }

    /// Resolve outstanding permission preflights as `Unknown` and suppress
    /// requests that have not reached the transport yet.
    pub fn cancel_permission_advice_request(&self, request_id: PermissionAdviceRequestId) {
        self.node.cancel_permission_advice_request(request_id);
    }

    /// Accept a subscriber connection served under `identity`.
    ///
    /// The accepting Db owns the ingestion semantics. A Local Db persists
    /// unfated commits as Pending/Local and forwards them upstream; a
    /// history-complete Db applies Core authority semantics.
    pub fn accept_subscriber(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
    ) -> Rc<RefCell<PeerConnection<S>>> {
        self.node.accept_subscriber(transport, identity)
    }

    /// Accept a subscriber connection served under `identity` with auth claims.
    pub fn accept_subscriber_with_claims(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        claims: BTreeMap<String, Value>,
    ) -> Rc<RefCell<PeerConnection<S>>> {
        self.node
            .accept_subscriber_with_claims(transport, identity, claims)
    }

    /// Accept a subscriber connection with explicit auth claims and upload trust mode.
    pub fn accept_subscriber_with_claims_and_trust(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        claims: BTreeMap<String, Value>,
        trust: CommitUnitTrust,
    ) -> Rc<RefCell<PeerConnection<S>>> {
        self.node
            .accept_subscriber_with_claims_and_trust(transport, identity, claims, trust)
    }

    /// Accept an edge-terminated subscriber with session claims.
    pub fn accept_edge_subscriber_with_claims(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        claims: BTreeMap<String, Value>,
    ) -> Rc<RefCell<PeerConnection<S>>> {
        self.node
            .accept_edge_subscriber_with_claims(transport, identity, claims)
    }

    /// Accept a subscriber whose host shell is wired as an edge fate authority.
    pub fn accept_edge_authority_subscriber_with_claims(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        claims: BTreeMap<String, Value>,
    ) -> Rc<RefCell<PeerConnection<S>>> {
        self.node
            .accept_edge_authority_subscriber_with_claims(transport, identity, claims)
    }

    /// Accept a reconnecting subscriber, resuming from a previous cursor.
    pub fn accept_subscriber_with_resume(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        cursor: ResumeCursor,
    ) -> Rc<RefCell<PeerConnection<S>>> {
        self.node
            .accept_subscriber_with_resume(transport, identity, cursor)
    }

    /// Detach a previously attached peer connection from this database.
    pub fn detach_connection(&self, connection: &Rc<RefCell<PeerConnection<S>>>) -> bool {
        self.node.detach_connection(connection)
    }

    /// Service every connection once (a convenience over
    /// [`PeerConnection::tick`] for the common single-upstream client).
    pub fn tick(&self) -> Result<(), Error> {
        self.node.tick().map(|_| ())
    }

    /// Service every connection once and return binding-observable wake counts.
    pub fn tick_stats(&self) -> Result<DbTickStats, Error> {
        self.node.tick()
    }

    fn refresh_subscriptions(&self) -> Result<usize, Error> {
        let refreshed = self.node.refresh_subscriptions()?;
        if refreshed > 0 {
            self.node.mark_subscriber_connections_dirty();
        }
        Ok(refreshed)
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only history-class byte estimate. This is intentionally the
    /// cheap physical-class counter, not a logical table-prefix scan.
    pub fn history_class_bytes_for_test(&self) -> Result<Option<u64>, Error> {
        Ok(self.node.node.borrow().history_class_bytes_for_test()?)
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only encoded storage byte estimate across Jazz physical
    /// classes.
    pub fn encoded_storage_bytes_for_test(&self) -> Result<u64, Error> {
        Ok(self.node.node.borrow().encoded_storage_bytes_for_test()?)
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only durability boundary for harnesses that reopen the same
    /// storage path immediately after a synthetic lifecycle transition.
    pub fn flush_for_test(&self) -> Result<(), Error> {
        Ok(self.node.node.borrow_mut().flush_query_runtime()?)
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only reset for logical storage-read attribution.
    pub fn reset_storage_read_metrics_for_test(&self) {
        self.node.node.borrow().reset_storage_read_metrics();
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only drain for logical storage-read attribution.
    pub fn take_storage_read_metrics_for_test(&self) -> groove::db::StorageReadMetrics {
        self.node.node.borrow().take_storage_read_metrics()
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only snapshot of sync-path counters.
    pub fn sync_metrics_for_test(&self) -> crate::node::SyncMetrics {
        self.node.node.borrow().sync_metrics().clone()
    }

    #[cfg(any(test, feature = "testing"))]
    /// Test/bench-only runtime diagnostics used by performance receipts.
    pub fn runtime_stats_for_test(&self) -> groove::ivm::RuntimeStats {
        self.node.node.borrow().runtime_stats_for_test()
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only maintained subscription sizing diagnostics used by
    /// warm-cache performance receipts.
    pub fn maintained_subscription_size_receipts_for_test(
        &self,
    ) -> Vec<MaintainedSubscriptionSizeReceipt> {
        self.node
            .subscriptions
            .borrow()
            .iter()
            .filter_map(Weak::upgrade)
            .filter_map(|state| {
                let state = state.borrow();
                let SubscriptionKind::Prepared {
                    shape,
                    binding,
                    maintained_subscription,
                } = &state.kind;
                let maintained_subscription = maintained_subscription.as_ref()?;
                let snapshot = &state.snapshot;
                let snapshot_bytes = encode_relation_snapshot_for_size(snapshot)
                    .map(|bytes| bytes.len())
                    .unwrap_or_default();
                let reset_frame_bytes = encode_subscription_reset_frame_for_size(
                    state.read_tier,
                    state.settled,
                    snapshot,
                )
                .map(|bytes| bytes.len())
                .unwrap_or_default();
                Some(MaintainedSubscriptionSizeReceipt {
                    name: shape.query().table.clone(),
                    shape_id: shape.shape_id().0,
                    binding_id: binding.binding_id().0,
                    rows: snapshot.rows.len(),
                    root_rows: snapshot.root_count,
                    relation_edges: snapshot.edges.len(),
                    footprint: DbMaintainedSubscriptionFootprint::from_local(
                        maintained_subscription.footprint(),
                    ),
                    snapshot_bytes,
                    reset_frame_bytes,
                    validation_tuple_estimate_bytes: validation_tuple_estimate_bytes(
                        shape,
                        binding,
                        state.author,
                        state.read_tier,
                        &state.read_view,
                    ),
                })
            })
            .collect()
    }
}

fn schema_policy_metadata_matches(left: &JazzSchema, right: &JazzSchema) -> bool {
    left.branch_read_policy == right.branch_read_policy
        && left.branch_write_policy == right.branch_write_policy
        && left.tables.len() == right.tables.len()
        && left.tables.iter().all(|left_table| {
            right.tables.iter().any(|right_table| {
                left_table.name == right_table.name
                    && left_table.read_policy == right_table.read_policy
                    && left_table.write_policies == right_table.write_policies
            })
        })
}

fn schema_index_metadata_matches(left: &JazzSchema, right: &JazzSchema) -> bool {
    left.tables.len() == right.tables.len()
        && left.tables.iter().all(|left_table| {
            right.tables.iter().any(|right_table| {
                left_table.name == right_table.name
                    && left_table.indexed_columns == right_table.indexed_columns
            })
        })
}

#[cfg(feature = "testing")]
#[derive(Clone, Debug, PartialEq, Eq)]
/// Test/bench-only sizing receipt for one active maintained subscription.
pub struct MaintainedSubscriptionSizeReceipt {
    /// Debug label for the subscription, currently the root query table.
    pub name: String,
    /// Stable query shape id.
    pub shape_id: uuid::Uuid,
    /// Stable binding id.
    pub binding_id: uuid::Uuid,
    /// Materialized snapshot row count, including related rows.
    pub rows: usize,
    /// Materialized root row count.
    pub root_rows: usize,
    /// Materialized relation/include edge count.
    pub relation_edges: usize,
    /// Approximate maintained-view and local control-state footprint.
    pub footprint: DbMaintainedSubscriptionFootprint,
    /// Postcard bytes for the materialized relation snapshot shape used by native runtimes.
    pub snapshot_bytes: usize,
    /// Postcard bytes for the native reset delta row payload.
    pub reset_frame_bytes: usize,
    /// Estimated validation tuple bytes for a future warm-cache key.
    pub validation_tuple_estimate_bytes: usize,
}

#[cfg(feature = "testing")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
/// Test/bench-only approximate heap footprint for a maintained subscription.
pub struct DbMaintainedSubscriptionFootprint {
    /// Active result-current rows in the maintained index.
    pub result_rows: usize,
    /// Result weight map entries, including non-positive transient entries.
    pub result_weights: usize,
    /// Result payload map entries retained for projected/synthetic output.
    pub result_payloads: usize,
    /// Active readable version identities retained by full record identity.
    pub version_identities: usize,
    /// Entries reachable through the version-by-transaction index.
    pub version_tx_entries: usize,
    /// Active replacement winner entries across content and deletion maps.
    pub replacement_entries: usize,
    /// Approximate heap bytes retained by result_weights.
    pub result_weights_bytes: usize,
    /// Approximate heap bytes retained by result_payloads.
    pub result_payloads_bytes: usize,
    /// Approximate heap bytes retained by WeightedVersionIndex.
    pub versions_bytes: usize,
    /// Approximate heap bytes retained by ReplacementIndex.
    pub replacements_bytes: usize,
    /// Approximate heap bytes retained by maintained-view indexes.
    pub maintained_heap_bytes: usize,
    /// Lowered terminal schema count.
    pub terminal_schemas: usize,
    /// Approximate heap bytes retained by terminal schemas.
    pub terminal_schemas_bytes: usize,
    /// Table schema count retained by the local subscription.
    pub tables: usize,
    /// Local result-set member count.
    pub result_set: usize,
    /// Local result payload count.
    pub local_result_payloads: usize,
    /// Local program fact count.
    pub program_facts: usize,
    /// Approximate heap bytes retained by local subscription control state.
    pub control_state_bytes: usize,
    /// Approximate maintained plus local control-state heap bytes.
    pub total_heap_bytes: usize,
}

#[cfg(feature = "testing")]
impl DbMaintainedSubscriptionFootprint {
    fn from_local(footprint: crate::node::LocalMaintainedViewSubscriptionFootprint) -> Self {
        Self {
            result_rows: footprint.maintained.result_rows,
            result_weights: footprint.maintained.result_weights,
            result_payloads: footprint.maintained.result_payloads,
            version_identities: footprint.maintained.version_identities,
            version_tx_entries: footprint.maintained.version_tx_entries,
            replacement_entries: footprint.maintained.replacement_entries,
            result_weights_bytes: footprint.maintained.result_weights_bytes,
            result_payloads_bytes: footprint.maintained.result_payloads_bytes,
            versions_bytes: footprint.maintained.versions_bytes,
            replacements_bytes: footprint.maintained.replacements_bytes,
            maintained_heap_bytes: footprint.maintained.total_heap_bytes,
            terminal_schemas: footprint.terminal_schemas.terminal_schemas,
            terminal_schemas_bytes: footprint.terminal_schemas.terminal_schemas_bytes,
            tables: footprint.tables,
            result_set: footprint.result_set,
            local_result_payloads: footprint.result_payloads,
            program_facts: footprint.program_facts,
            control_state_bytes: footprint.control_state_bytes,
            total_heap_bytes: footprint.total_heap_bytes,
        }
    }
}

#[cfg(feature = "testing")]
#[derive(serde::Serialize)]
struct SizeRelationSnapshot<'a> {
    root_count: u64,
    rows: Vec<SizeRowBatch<'a>>,
}

#[cfg(feature = "testing")]
#[derive(serde::Serialize)]
struct SizeSubscriptionDelta<'a> {
    added: Vec<SizeRowBatch<'a>>,
    updated: Vec<SizeRowBatch<'a>>,
    removed: Vec<SizeRemovedRow>,
}

#[cfg(feature = "testing")]
#[derive(serde::Serialize)]
struct SizeRowBatch<'a> {
    table: &'a str,
    descriptor: groove::records::RecordDescriptor,
    rows: Vec<SizeRow<'a>>,
}

#[cfg(feature = "testing")]
#[derive(serde::Serialize)]
struct SizeRow<'a> {
    row_id: RowUuid,
    deleted: bool,
    raw: &'a [u8],
}

#[cfg(feature = "testing")]
#[derive(serde::Serialize)]
struct SizeRemovedRow {
    table: String,
    row_id: RowUuid,
}

#[cfg(feature = "testing")]
fn encode_relation_snapshot_for_size(
    snapshot: &RelationSnapshot,
) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(&SizeRelationSnapshot {
        root_count: snapshot.root_count as u64,
        rows: size_row_batches(&snapshot.rows),
    })
}

#[cfg(feature = "testing")]
fn encode_subscription_reset_frame_for_size(
    _tier: DurabilityTier,
    _settled: bool,
    snapshot: &RelationSnapshot,
) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(&SizeSubscriptionDelta {
        added: size_row_batches(&snapshot.rows),
        updated: Vec::new(),
        removed: Vec::new(),
    })
}

#[cfg(feature = "testing")]
fn size_row_batches(rows: &[CurrentRow]) -> Vec<SizeRowBatch<'_>> {
    let mut batches = Vec::<SizeRowBatch<'_>>::new();
    for row in rows {
        let (descriptor, raw) = row.encoded_record();
        match batches.last_mut() {
            Some(batch) if batch.table == row.table() && batch.descriptor == *descriptor => {
                batch.rows.push(size_row(row, raw));
            }
            _ => batches.push(SizeRowBatch {
                table: row.table(),
                descriptor: *descriptor,
                rows: vec![size_row(row, raw)],
            }),
        }
    }
    batches
}

#[cfg(feature = "testing")]
fn size_row<'a>(row: &CurrentRow, raw: &'a [u8]) -> SizeRow<'a> {
    SizeRow {
        row_id: row.row_uuid(),
        deleted: row.is_deleted(),
        raw,
    }
}

#[cfg(feature = "testing")]
fn validation_tuple_estimate_bytes(
    shape: &ValidatedQuery,
    binding: &Binding,
    author: AuthorId,
    tier: DurabilityTier,
    read_view: &ReadViewSpec,
) -> usize {
    #[derive(serde::Serialize)]
    struct ValidationTuple<'a> {
        shape_id: uuid::Uuid,
        binding_id: uuid::Uuid,
        schema_version: SchemaVersionId,
        canonical_query: &'a [u8],
        canonical_binding: &'a [u8],
        author: AuthorId,
        tier: DurabilityTier,
        read_view: &'a ReadViewSpec,
    }

    postcard::to_allocvec(&ValidationTuple {
        shape_id: shape.shape_id().0,
        binding_id: binding.binding_id().0,
        schema_version: shape.schema_version(),
        canonical_query: shape.canonical_bytes(),
        canonical_binding: binding.canonical_bytes(),
        author,
        tier,
        read_view,
    })
    .map(|bytes| bytes.len())
    .unwrap_or_default()
}

/// Counts produced while servicing non-blocking database connection work.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DbTickStats {
    /// Number of live subscriptions that received a queued event.
    pub subscription_events: usize,
    /// Number of connection ticks that applied remote sync state locally.
    pub remote_sync_applied: usize,
}

mod node_runtime;
pub use node_runtime::{ConnectionSessionContext, Node, Transport};
use node_runtime::{register_upstream_subscription_owner, unregister_upstream_subscription_owner};
mod peer_connection;
use peer_connection::{ConnectionLink, schedule_tick_in};
pub use peer_connection::{PeerConnection, ResumeCursor};
mod config;
pub use config::{DbConfig, DbIdentity, ProductionRowIdSource, RowIdSource, SeededRowIdSource};

/// One-shot read options.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ReadOpts {
    /// Durability tier that gates the first result.
    pub tier: DurabilityTier,
    /// Whether own local updates are visible immediately.
    pub local_updates: LocalUpdates,
    /// Whether evaluation may propagate upstream.
    pub propagation: Propagation,
    /// Include current rows whose deletion winner is `Deleted`.
    pub include_deleted: bool,
    /// Semantic read view to evaluate against.
    pub read_view: ReadViewSpec,
}

impl Default for ReadOpts {
    fn default() -> Self {
        Self {
            tier: DurabilityTier::Local,
            local_updates: LocalUpdates::Immediate,
            propagation: Propagation::Full,
            include_deleted: false,
            read_view: ReadViewSpec::default(),
        }
    }
}

/// Own-write overlay policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum LocalUpdates {
    /// Include local writes immediately.
    Immediate,
    /// Defer local writes until the requested tier observes them.
    Deferred,
}

/// Read propagation policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum Propagation {
    /// Full propagation may be used by future remote paths.
    Full,
    /// Evaluate only against local knowledge.
    LocalOnly,
}

/// Public API error with stable machine-readable codes.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct Error {
    /// Stable error code.
    pub code: ErrorCode,
    /// Human-readable detail.
    pub message: String,
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.code {
            ErrorCode::TransactionConflict => {
                write!(formatter, "(transaction_conflict): {}", self.message)
            }
            _ => write!(formatter, "{:?}: {}", self.code, self.message),
        }
    }
}

impl std::error::Error for Error {}

impl Error {
    fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

fn row_already_deleted(row: RowUuid) -> Error {
    Error::new(
        ErrorCode::WriteRejected,
        format!("row already deleted: {}", row.0),
    )
}

fn read_for_write_denied(operation: &str, table: &str) -> Error {
    Error::new(
        ErrorCode::WriteRejected,
        format!(
            "read policy denied {operation} on table {table}: the operation requires read permission on the target row"
        ),
    )
}

/// Stable API error code.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ErrorCode {
    /// Schema validation failed.
    Schema,
    /// Query validation or binding failed.
    Query,
    /// Write was rejected.
    WriteRejected,
    /// An exclusive transaction's fixed snapshot was invalidated locally.
    TransactionConflict,
    /// Storage failed.
    Storage,
    /// Protocol or local node operation failed.
    Protocol,
    /// Local transport queue is full and the operation should be retried later.
    Backpressure,
    /// Requested observation is not locally available in this slice.
    NotObserved,
    /// Historical read must be evaluated by a complete-history server.
    HistoricalReadRequiresServer,
}

impl From<crate::node::Error> for Error {
    fn from(error: crate::node::Error) -> Self {
        let code = match &error {
            crate::node::Error::HistoricalReadRequiresServer => {
                ErrorCode::HistoricalReadRequiresServer
            }
            crate::node::Error::Storage(_) | crate::node::Error::Groove(_) => ErrorCode::Storage,
            crate::node::Error::Query(_) => ErrorCode::Query,
            crate::node::Error::TransactionConflict => ErrorCode::TransactionConflict,
            crate::node::Error::TableNotFound(_)
            | crate::node::Error::UnsupportedColumnType(_)
            | crate::node::Error::InvalidMergeableCommit(_) => ErrorCode::Schema,
            _ => ErrorCode::Protocol,
        };
        Self::new(code, error.to_string())
    }
}

impl From<QueryError> for Error {
    fn from(error: QueryError) -> Self {
        Self::new(ErrorCode::Query, error.to_string())
    }
}

#[doc(hidden)]
pub mod doctest_support {
    use std::collections::BTreeMap;
    use std::future::Future;

    use groove::records::Value;
    use groove::schema::{ColumnSchema, ColumnType};
    pub use groove::storage::MemoryStorage;

    use crate::db::{Db, DbConfig, DbIdentity, Error, RowCells, SeededRowIdSource};
    use crate::ids::{AuthorId, NodeUuid};
    use crate::schema::{JazzSchema, Policy, TableSchema};

    /// Poll a ready-immediate Db future in examples.
    pub fn block_on<F: Future>(future: F) -> F::Output {
        crate::db::block_on(future)
    }

    /// Example schema used by Db doctests.
    pub fn schema() -> JazzSchema {
        JazzSchema::new([TableSchema::new(
            "todos",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("done", ColumnType::Bool),
            ],
        )
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public())])
    }

    /// Open a fresh Db over in-memory storage.
    pub async fn open_todos_db() -> Result<Db<MemoryStorage>, Error> {
        let schema = schema();
        let cfs = schema.column_families();
        let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
        Db::open(DbConfig {
            schema,
            storage: MemoryStorage::new(&refs),
            identity: DbIdentity {
                node: NodeUuid::from_bytes([0x11; 16]),
                author: AuthorId::from_bytes([0xa1; 16]),
            },
            id_source: Some(Box::new(SeededRowIdSource::new(0x1111))),
        })
        .await
    }

    /// Todo row payload for examples.
    pub fn todo_cells(title: &str, done: bool) -> RowCells {
        BTreeMap::from([
            ("title".to_owned(), Value::String(title.to_owned())),
            ("done".to_owned(), Value::Bool(done)),
        ])
    }
}

fn effective_read_tier(opts: &ReadOpts) -> DurabilityTier {
    if opts.local_updates == LocalUpdates::Immediate {
        opts.tier.max(DurabilityTier::Local)
    } else {
        opts.tier
    }
}

fn upstream_register_shape_options(
    tier: DurabilityTier,
    read_view: ReadViewSpec,
    upstream_durability_floor: DurabilityTier,
    propagate_upstream: bool,
) -> RegisterShapeOptions {
    RegisterShapeOptions {
        tier: remote_subscription_tier(tier, upstream_durability_floor),
        read_view,
        propagate_upstream,
    }
}

fn remote_subscription_tier(
    tier: DurabilityTier,
    upstream_durability_floor: DurabilityTier,
) -> DurabilityTier {
    tier.max(upstream_durability_floor)
}

fn ensure_default_read_view(opts: &ReadOpts) -> Result<(), Error> {
    if opts.read_view.is_default() {
        return Ok(());
    }
    Err(Error::new(
        ErrorCode::Query,
        "non-default read_view is not supported yet; reads currently execute against the current/default view",
    ))
}

fn ensure_supported_read_view(opts: &ReadOpts) -> Result<(), Error> {
    match &opts.read_view.source {
        ReadViewSourceSpec::Current => Ok(()),
        ReadViewSourceSpec::Branch { .. }
            if opts.read_view.schema == Default::default()
                && opts.read_view.overlays.is_empty() =>
        {
            Ok(())
        }
        _ => ensure_default_read_view(opts),
    }
}

fn ensure_supported_subscription_read_opts(opts: &ReadOpts) -> Result<(), Error> {
    if opts.include_deleted {
        return Err(Error::new(
            ErrorCode::Query,
            "live subscriptions do not support include_deleted yet",
        ));
    }
    ensure_supported_read_view(opts)
}

fn ensure_supported_register_shape_read_view(opts: &RegisterShapeOptions) -> Result<(), Error> {
    let read_opts = ReadOpts {
        read_view: opts.read_view.clone(),
        ..ReadOpts::default()
    };
    ensure_supported_read_view(&read_opts)
}

fn ensure_supported_register_shape_options(
    opts: &RegisterShapeOptions,
    local_receiver: bool,
    peer_role: PeerRole,
) -> Result<(), Error> {
    ensure_supported_register_shape_read_view(opts)?;
    let supported = match (local_receiver, peer_role) {
        (true, _) | (false, PeerRole::Relay) => opts.tier >= DurabilityTier::Local,
        (false, PeerRole::ClientLink { .. }) => opts.tier == DurabilityTier::Global,
    };
    if !supported {
        let message = match (local_receiver, peer_role) {
            (true, _) => {
                "Local sync subscription serving requires at least local-tier registration"
            }
            (false, PeerRole::Relay) => {
                "relay sync subscription serving requires at least local-tier registration"
            }
            (false, PeerRole::ClientLink { .. }) => {
                "sync subscription serving supports only global-tier registration"
            }
        };
        return Err(Error::new(ErrorCode::Query, message));
    }
    Ok(())
}

fn validate_shape_ast_for_registration<S>(
    node: &NodeState<S>,
    shape_id: ShapeId,
    ast: &ShapeAst,
) -> Result<Option<ValidatedQuery>, crate::node::Error>
where
    S: OrderedKvStorage,
{
    node.validate_shape_ast_for_registration(shape_id, ast)
}

fn send_unsupported_shape_capability_rejection(
    transport: &mut dyn Transport,
    subscription: SubscriptionKey,
    detail: String,
) -> Result<(), TransportError> {
    send_subscription_rejection(
        transport,
        subscription,
        SubscribeRejectReason::UnsupportedShapeCapability { detail },
    )
}

fn reject_server_subscription_failure(
    transport: &mut dyn Transport,
    subscription: SubscriptionKey,
    error: &crate::node::Error,
) -> Result<(), TransportError> {
    // Keep the complete error on the serving process only. Subscription keys
    // provide a correlation handle without disclosing schema, policy, or
    // storage details to the peer.
    eprintln!(
        "jazz subscription rejected: shape={} binding={} read_view={} server_error={error}",
        subscription.shape_id.0, subscription.binding_id.0, subscription.read_view.id,
    );
    send_subscription_rejection(
        transport,
        subscription,
        SubscribeRejectReason::ServerFailure {
            code: server_failure_code(error),
        },
    )
}

fn send_subscription_rejection(
    transport: &mut dyn Transport,
    subscription: SubscriptionKey,
    reason: SubscribeRejectReason,
) -> Result<(), TransportError> {
    transport.send(SyncMessage::SubscribeRejected {
        subscription,
        reason,
    })
}

fn server_failure_code(error: &crate::node::Error) -> SubscribeServerFailureCode {
    match error {
        crate::node::Error::TableNotFound(_) => SubscribeServerFailureCode::TableNotFound,
        crate::node::Error::Query(error) if matches!(**error, QueryError::UnknownTable(_)) => {
            SubscribeServerFailureCode::TableNotFound
        }
        crate::node::Error::Query(_) => SubscribeServerFailureCode::QueryValidation,
        crate::node::Error::QueryLowering(_) => SubscribeServerFailureCode::QueryLowering,
        crate::node::Error::AuthorizationDenied => SubscribeServerFailureCode::PolicyEvaluation,
        crate::node::Error::InvalidStoredValue(_)
        | crate::node::Error::InvalidCatalogueUpdate(_) => {
            SubscribeServerFailureCode::SchemaResolution
        }
        _ => SubscribeServerFailureCode::Internal,
    }
}

fn is_server_shape_validation_failure(error: &crate::node::Error) -> bool {
    matches!(
        error,
        crate::node::Error::TableNotFound(_) | crate::node::Error::Query(_)
    )
}

fn register_shape_rejection_subscription(
    shape_id: ShapeId,
    read_view: ReadViewKey,
) -> SubscriptionKey {
    SubscriptionKey {
        shape_id,
        binding_id: BindingId(uuid::Uuid::nil()),
        read_view,
    }
}

fn coverage_key(
    shape: &ValidatedQuery,
    binding: &Binding,
    opts: RegisterShapeOptions,
) -> CoverageKey {
    CoverageKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        opts,
    }
}

fn subscriber_permissions_ready(permissions_ready: bool, trust: CommitUnitTrust) -> bool {
    trust == CommitUnitTrust::TrustedBackend || permissions_ready
}

/// Messages whose semantics assert downstream authority state must never be
/// accepted from a subscriber transport. Keep this admission check ahead of
/// `NodeState::apply_sync_message`: validation inside the node cannot recover
/// the direction or authenticated link role after dispatch.
fn subscriber_inbound_message_is_authority_only(
    message: &SyncMessage,
    trust: CommitUnitTrust,
) -> bool {
    matches!(
        message,
        SyncMessage::FateUpdate { .. }
            | SyncMessage::SubscribeRejected { .. }
            | SyncMessage::CatalogueAck(_)
            | SyncMessage::ViewUpdate { .. }
            | SyncMessage::RowVersionPayloads { .. }
            | SyncMessage::CatalogueSnapshot(_)
            | SyncMessage::PermissionAdviceResponse { .. }
            | SyncMessage::AuthorizationScopeReceipt { .. }
            | SyncMessage::AuthorizationScopeView { .. }
            | SyncMessage::AuthorizationScopeAggregateReceipt { .. }
            | SyncMessage::AuthorizationScopeUnavailable { .. }
            | SyncMessage::AuthorizationScopeDecision { .. }
    ) || (trust == CommitUnitTrust::Session && matches!(message, SyncMessage::SessionClaims { .. }))
}

fn subscriber_permission_subject(ingest: CommitUnitIngestContext) -> AuthorId {
    match ingest.trust {
        CommitUnitTrust::Session => ingest.identity,
        CommitUnitTrust::TrustedBackend => AuthorId::SYSTEM,
    }
}

/// Row cells supplied to write methods.
pub type RowCells = BTreeMap<String, Value>;

/// Build [`RowCells`] with bare identifier column names.
///
/// Keys are converted to column names with `stringify!`, and values are
/// converted with `Into<Value>`. Column and type validation remains lazy at
/// write/query validation time.
///
/// ```rust
/// # use jazz::db::doctest_support::{block_on, open_todos_db};
/// # use jazz::tx::DurabilityTier;
/// let db = block_on(open_todos_db())?;
/// let write = db.insert(
///     "todos",
///     jazz::row! {
///         title: "Ship it",
///         done: false,
///     },
/// )?;
/// block_on(write.wait(DurabilityTier::Local))?;
///
/// let todos = db.prepare_query(&db.table("todos"))?;
/// assert_eq!(db.read(&todos)?.len(), 1);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[macro_export]
macro_rules! row {
    () => {
        $crate::db::RowCells::new()
    };
    ($($key:ident : $value:expr),+ $(,)?) => {{
        let mut cells = $crate::db::RowCells::new();
        $(
            cells.insert(::std::string::String::from(stringify!($key)), ($value).into());
        )+
        cells
    }};
}

/// CRUD operations for an open mergeable transaction.
///
/// [`MergeableTx`] and [`MergeableTxRef`] implement this trait, so mergeable
/// CRUD has one definition regardless of who owns the transaction lifetime.
/// Import this trait to call its methods.
pub trait MergeableTxOps<S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    /// The database that owns the open transaction.
    fn db(&self) -> &Db<S>;

    /// The id of the already-open transaction.
    fn tx_id(&self) -> OpenBatchId;

    /// Stage an insert with a generated row id.
    fn insert(&self, table: &str, cells: RowCells) -> Result<RowUuid, Error> {
        let row = self.db().row_id_source.borrow_mut().next_row_id();
        self.insert_with_id(table, row, cells)?;
        Ok(row)
    }

    /// Stage an insert with a caller-supplied row id.
    fn insert_with_id(&self, table: &str, row: RowUuid, cells: RowCells) -> Result<(), Error> {
        self.insert_with_id_at_ms_option(table, row, cells, None)
    }

    /// Stage an insert with a caller-supplied row id and explicit millisecond provenance time.
    fn insert_with_id_at_ms(
        &self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        now_ms: u64,
    ) -> Result<(), Error> {
        self.insert_with_id_at_ms_option(table, row, cells, Some(now_ms))
    }

    /// Stage an update; omitted fields keep the transaction-local value.
    fn update(&self, table: &str, row: RowUuid, patch: RowCells) -> Result<(), Error> {
        self.update_at_ms_option(table, row, patch, None)
    }

    /// Stage an update with an explicit millisecond provenance time.
    fn update_at_ms(
        &self,
        table: &str,
        row: RowUuid,
        patch: RowCells,
        now_ms: u64,
    ) -> Result<(), Error> {
        self.update_at_ms_option(table, row, patch, Some(now_ms))
    }

    /// Stage a soft delete.
    fn delete(&self, table: &str, row: RowUuid) -> Result<(), Error> {
        self.delete_at_ms_option(table, row, None)
    }

    /// Stage a soft delete with explicit millisecond provenance time.
    fn delete_at_ms(&self, table: &str, row: RowUuid, now_ms: u64) -> Result<(), Error> {
        self.delete_at_ms_option(table, row, Some(now_ms))
    }

    /// Stage a restore, applying defaults for omitted columns.
    fn restore(&self, table: &str, row: RowUuid, cells: RowCells) -> Result<(), Error> {
        self.restore_at_ms_option(table, row, cells, None)
    }

    /// Stage a restore with explicit millisecond provenance time, applying defaults for omitted columns.
    fn restore_at_ms(
        &self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        now_ms: u64,
    ) -> Result<(), Error> {
        self.restore_at_ms_option(table, row, cells, Some(now_ms))
    }

    /// Read one row with this transaction's pending writes overlaid.
    fn read(&self, table: &str, row: RowUuid) -> Result<Option<RowCells>, Error> {
        self.db()
            .node
            .node
            .borrow_mut()
            .tx_read_in_schema(self.tx_id(), self.db().schema_version_id, table, row)
            .map_err(Into::into)
    }

    /// Read a prepared query with this transaction's pending writes overlaid.
    fn all_prepared(&self, prepared: &PreparedQuery) -> Result<Vec<CurrentRow>, Error> {
        self.all_prepared_with_opts(prepared, ReadOpts::default())
    }

    /// Read a prepared query with transaction-local writes and explicit read semantics.
    fn all_prepared_with_opts(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.db().transaction_all(self.tx_id(), prepared, opts)
    }

    /// Read a prepared query inside this transaction as `author`.
    fn all_prepared_for_identity(
        &self,
        prepared: &PreparedQuery,
        author: AuthorId,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.all_prepared_for_identity_with_opts(prepared, author, ReadOpts::default())
    }

    /// Read a prepared query as `author` with explicit read semantics.
    fn all_prepared_for_identity_with_opts(
        &self,
        prepared: &PreparedQuery,
        author: AuthorId,
        opts: ReadOpts,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.db()
            .transaction_all_for_identity(self.tx_id(), prepared, author, opts)
    }

    /// Stage an insert with an optional explicit provenance time.
    fn insert_with_id_at_ms_option(
        &self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        self.db()
            .stage_mergeable_insert(self.tx_id(), table, row, cells, now_ms)
    }

    /// Stage an update with an optional explicit provenance time.
    fn update_at_ms_option(
        &self,
        table: &str,
        row: RowUuid,
        patch: RowCells,
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        self.db()
            .stage_mergeable_update(self.tx_id(), table, row, patch, now_ms)
    }

    /// Stage a deletion with an optional explicit provenance time.
    fn delete_at_ms_option(
        &self,
        table: &str,
        row: RowUuid,
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        self.db()
            .stage_mergeable_delete(self.tx_id(), table, row, now_ms)
    }

    /// Stage a restore with an optional explicit provenance time.
    fn restore_at_ms_option(
        &self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        now_ms: Option<u64>,
    ) -> Result<(), Error> {
        self.db()
            .stage_mergeable_restore(self.tx_id(), table, row, cells, now_ms)
    }
}

/// Owning, Rust-facing handle for a group of mergeable writes.
///
/// This handle owns the transaction lifetime and abandons an uncommitted
/// transaction on drop. Use [`MergeableTxRef`] when a caller retains an
/// [`OpenBatchId`] between calls and must not close the transaction on return.
pub struct MergeableTx<'a, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    db: &'a Db<S>,
    tx_id: OpenBatchId,
    /// Set once the transaction has been committed, so `Drop` does not then
    /// abandon it. Without this, `commit` consumed `self` and `Drop` still ran
    /// `abandon_transaction_handle` on an already-committed transaction — benign
    /// only because `abandon_tx` tolerates an unknown id, and silent because the
    /// result was discarded.
    committed: bool,
}

impl<S> MergeableTx<'_, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    /// Commit all staged writes as one mergeable transaction.
    ///
    /// Once the commit succeeds, dropping this handle does not abandon the
    /// already-committed transaction. If it fails, dropping the handle attempts
    /// to abandon any transaction that remains open.
    pub fn commit(mut self) -> Result<TxId, Error> {
        let result = self.db.commit_mergeable_handle(self.tx_id);
        if result.is_ok() {
            self.committed = true;
        }
        result
    }
}

impl<S> MergeableTxOps<S> for MergeableTx<'_, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    fn db(&self) -> &Db<S> {
        self.db
    }

    fn tx_id(&self) -> OpenBatchId {
        self.tx_id
    }
}

/// Non-owning operations handle for an already-open mergeable transaction.
///
/// Construct this with [`Db::mergeable_tx_ref`] when another layer owns the
/// [`OpenBatchId`] lifetime. Dropping this ref never abandons the transaction.
pub struct MergeableTxRef<'a, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    db: &'a Db<S>,
    tx_id: OpenBatchId,
}

impl<S> MergeableTxOps<S> for MergeableTxRef<'_, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    fn db(&self) -> &Db<S> {
        self.db
    }

    fn tx_id(&self) -> OpenBatchId {
        self.tx_id
    }
}

impl<S> Drop for MergeableTx<'_, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let _ = self.db.abandon_transaction_handle(self.tx_id);
    }
}

/// CRUD and read operations for an open exclusive transaction.
///
/// [`ExclusiveTx`] and [`ExclusiveTxRef`] implement this trait, so exclusive
/// operations have one definition regardless of who owns the transaction
/// lifetime. Import this trait to call its methods.
pub trait ExclusiveTxOps<S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    /// The database that owns the open transaction.
    fn db(&self) -> &Db<S>;

    /// The id of the already-open transaction.
    fn tx_id(&self) -> OpenBatchId;

    /// Read one row inside the exclusive transaction.
    fn read(&self, table: &str, row: RowUuid) -> Result<Option<RowCells>, Error> {
        self.db().exclusive_read(self.tx_id(), table, row)
    }

    /// Read all current rows in a table inside the exclusive transaction.
    fn all(&self, table: &str) -> Result<Vec<CurrentRow>, Error> {
        self.db()
            .node
            .node
            .borrow_mut()
            .tx_current_rows(self.tx_id(), table)
            .map_err(Into::into)
    }

    /// Read a prepared query inside the exclusive transaction.
    fn all_prepared(&self, prepared: &PreparedQuery) -> Result<Vec<CurrentRow>, Error> {
        self.all_prepared_with_opts(prepared, ReadOpts::default())
    }

    /// Read a prepared query with transaction-local writes and explicit read semantics.
    fn all_prepared_with_opts(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.db().transaction_all(self.tx_id(), prepared, opts)
    }

    /// Read a prepared query inside the exclusive transaction as `author`.
    fn all_prepared_for_identity(
        &self,
        prepared: &PreparedQuery,
        author: AuthorId,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.all_prepared_for_identity_with_opts(prepared, author, ReadOpts::default())
    }

    /// Read a prepared query as `author` with explicit read semantics.
    fn all_prepared_for_identity_with_opts(
        &self,
        prepared: &PreparedQuery,
        author: AuthorId,
        opts: ReadOpts,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.db()
            .transaction_all_for_identity(self.tx_id(), prepared, author, opts)
    }

    /// Stage an insert with a generated row id.
    fn insert(&self, table: &str, cells: RowCells) -> Result<RowUuid, Error> {
        let row = self.db().row_id_source.borrow_mut().next_row_id();
        self.insert_with_id(table, row, cells)?;
        Ok(row)
    }

    /// Stage an insert with a caller-supplied row id.
    fn insert_with_id(&self, table: &str, row: RowUuid, cells: RowCells) -> Result<(), Error> {
        self.db()
            .stage_exclusive_insert(self.tx_id(), table, row, cells)
    }

    /// Stage an update; omitted fields keep the transaction-local value.
    fn update(&self, table: &str, row: RowUuid, patch: RowCells) -> Result<(), Error> {
        let mut cells = self.read(table, row)?.unwrap_or_default();
        cells.extend(patch);
        self.insert_with_id(table, row, cells)
    }

    /// Stage a soft delete.
    fn delete(&self, table: &str, row: RowUuid) -> Result<(), Error> {
        self.db().stage_exclusive_delete(self.tx_id(), table, row)
    }

    /// Stage a restore, applying defaults for omitted columns.
    fn restore(&self, table: &str, row: RowUuid, cells: RowCells) -> Result<(), Error> {
        self.db()
            .stage_exclusive_restore(self.tx_id(), table, row, cells)
    }
}

/// Owning, Rust-facing handle for an exclusive transaction over a stable snapshot.
///
/// This handle owns the transaction lifetime and abandons an uncommitted
/// transaction on drop. Use [`ExclusiveTxRef`] when a caller retains an
/// [`OpenBatchId`] between calls and must not close the transaction on return.
pub struct ExclusiveTx<'a, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    db: &'a Db<S>,
    tx_id: OpenBatchId,
    committed: bool,
}

impl<S> ExclusiveTx<'_, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    /// Commit the exclusive transaction.
    ///
    /// Once the commit succeeds, dropping this handle does not abandon the
    /// already-committed transaction. If it fails, dropping the handle attempts
    /// to abandon any transaction that remains open.
    pub fn commit(mut self) -> Result<TxId, Error> {
        let result = self.db.commit_exclusive_handle(self.tx_id);
        if result.is_ok() {
            self.committed = true;
        }
        result
    }
}

impl<S> ExclusiveTxOps<S> for ExclusiveTx<'_, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    fn db(&self) -> &Db<S> {
        self.db
    }

    fn tx_id(&self) -> OpenBatchId {
        self.tx_id
    }
}

impl<S> Drop for ExclusiveTx<'_, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let _ = self.db.abandon_exclusive_handle(self.tx_id);
    }
}

/// Non-owning operations handle for an already-open exclusive transaction.
///
/// Construct this with [`Db::exclusive_tx_ref`] when another layer owns the
/// [`OpenBatchId`] lifetime. Dropping this ref never abandons the transaction.
pub struct ExclusiveTxRef<'a, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    db: &'a Db<S>,
    tx_id: OpenBatchId,
}

impl<S> ExclusiveTxOps<S> for ExclusiveTxRef<'_, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    fn db(&self) -> &Db<S> {
        self.db
    }

    fn tx_id(&self) -> OpenBatchId {
        self.tx_id
    }
}

/// Handle for an applied local write.
pub struct WriteHandle<S>
where
    S: OrderedKvStorage,
{
    node: Weak<RefCell<NodeState<S>>>,
    row_uuid: RowUuid,
    tx_id: TxId,
    local_tier: DurabilityTier,
}

impl<S> WriteHandle<S>
where
    S: OrderedKvStorage,
{
    /// Generated or caller-supplied row id affected by this write.
    pub fn row_uuid(&self) -> RowUuid {
        self.row_uuid
    }

    /// Mergeable transaction id backing this write.
    ///
    /// ```rust
    /// # use jazz::db::doctest_support::{block_on, open_todos_db, todo_cells};
    /// let db = block_on(open_todos_db())?;
    /// let write = db.insert("todos", todo_cells("has id", false))?;
    ///
    /// let _row_id = write.row_uuid();
    /// let _tx_id = write.mergeable_tx_id();
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn mergeable_tx_id(&self) -> TxId {
        self.tx_id
    }

    /// Wait until this write has reached the requested tier.
    ///
    /// ```rust
    /// # use jazz::db::doctest_support::{block_on, open_todos_db, todo_cells};
    /// # use jazz::tx::DurabilityTier;
    /// let db = block_on(open_todos_db())?;
    /// let write = db.insert("todos", todo_cells("wait locally", false))?;
    ///
    /// let tx_id = block_on(write.wait(DurabilityTier::Local))?;
    /// assert_eq!(tx_id, write.mergeable_tx_id());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn wait(&self, tier: DurabilityTier) -> Result<TxId, Error> {
        if tier <= self.local_tier {
            return Ok(self.tx_id);
        }
        let state = self.write_state()?;
        match state.fate {
            Fate::Rejected(reason) => Err(write_rejected(reason)),
            Fate::Pending if tier >= DurabilityTier::Edge => Err(Error::new(
                ErrorCode::NotObserved,
                format!("write has not been accepted at requested tier {tier:?}"),
            )),
            Fate::Pending | Fate::Accepted if state.durability < tier => Err(Error::new(
                ErrorCode::NotObserved,
                format!("write has not reached requested tier {tier:?}"),
            )),
            Fate::Pending | Fate::Accepted => Ok(self.tx_id),
        }
    }

    /// Return the locally observed fate and durability for this write.
    pub fn write_state(&self) -> Result<WriteState, Error> {
        let Some(node) = self.node.upgrade() else {
            return Err(Error::new(
                ErrorCode::NotObserved,
                "database handle was dropped",
            ));
        };
        let Some((fate, _, durability)) = node.borrow_mut().transaction_state(self.tx_id) else {
            return Err(Error::new(
                ErrorCode::NotObserved,
                "transaction is not known locally",
            ));
        };
        Ok(WriteState { fate, durability })
    }
}

fn write_rejected(reason: RejectionReason) -> Error {
    Error::new(ErrorCode::WriteRejected, format!("{reason:?}"))
}

/// Decode Groove's recursively assembled terminal value into Jazz's typed
/// structured-result view. This separates relation fields from scalar row
/// fields, but does not join or assemble relation facts.
fn materialize_result_tree(query: &Query, snapshot: RelationSnapshot) -> Result<ResultTree, Error> {
    fn node_from_record(
        table: &str,
        record: OwnedRecord,
        arrays: &[crate::query::ArraySubquery],
    ) -> Result<ResultNode, Error> {
        let descriptor = *record.descriptor();
        let values = record.to_values().map_err(|error| {
            Error::new(
                ErrorCode::Protocol,
                format!("invalid Groove terminal record: {error}"),
            )
        })?;
        let array_by_name = arrays
            .iter()
            .map(|array| (array.column_name.as_str(), array))
            .collect::<BTreeMap<_, _>>();
        let mut scalar_fields = Vec::new();
        let mut scalar_values = Vec::new();
        let mut relations = BTreeMap::new();
        for (field, value) in descriptor.fields().iter().zip(values) {
            let Some(name) = field.name.as_deref() else {
                return Err(Error::new(
                    ErrorCode::Protocol,
                    "Groove terminal emitted an unnamed field",
                ));
            };
            let Some(array) = array_by_name.get(name) else {
                scalar_fields.push((name.to_owned(), field.value_type.clone()));
                scalar_values.push(value);
                continue;
            };
            let Value::Array(children) = value else {
                return Err(Error::new(
                    ErrorCode::Protocol,
                    format!("Groove terminal relation {name} was not an array"),
                ));
            };
            let children = children
                .into_iter()
                .map(|value| {
                    let Value::Record(record) = value else {
                        return Err(Error::new(
                            ErrorCode::Protocol,
                            format!("Groove terminal relation {name} contained a non-record"),
                        ));
                    };
                    node_from_record(&array.table, record, &array.nested_arrays)
                })
                .collect::<Result<Vec<_>, _>>()?;
            relations.insert(name.to_owned(), ResultRelation::Array(children));
        }
        let scalar_descriptor = RecordDescriptor::new(scalar_fields);
        let raw = scalar_descriptor.create(&scalar_values).map_err(|error| {
            Error::new(
                ErrorCode::Protocol,
                format!("invalid scalar projection from Groove terminal: {error}"),
            )
        })?;
        let row = CurrentRow::new(table, OwnedRecord::new(raw, scalar_descriptor));
        let occurrence = OutputOccurrenceId::single_source(ObjectId::from_uuid(row.row_uuid().0));
        Ok(ResultNode {
            occurrence,
            row,
            relations,
        })
    }

    if !snapshot.edges.is_empty() {
        return Err(Error::new(
            ErrorCode::Protocol,
            "structured result unexpectedly contained relation-fact edges",
        ));
    }
    let roots = snapshot
        .rows
        .into_iter()
        .take(snapshot.root_count)
        .map(|row| {
            let (descriptor, raw) = row.encoded_record();
            node_from_record(
                row.table(),
                OwnedRecord::new(raw.to_vec(), *descriptor),
                &query.array_subqueries,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ResultTree { roots })
}

struct SubscriptionState {
    terminal_rows: bool,
    kind: SubscriptionKind,
    groove_runtime_token: u64,
    /// The maintained subscription currently owned by this public stream.
    /// Rehydration replaces the Groove ID, and drop must clean up that new ID.
    local_subscription_cleanup: Rc<Cell<Option<(u64, groove::ivm::SubscriptionId)>>>,
    propagates_upstream: bool,
    author: AuthorId,
    authorization_mode: QueryAuthorizationMode,
    read_tier: DurabilityTier,
    remote_read_tier: Option<DurabilityTier>,
    /// Once this stream has an upstream, cached durable state needs a receipt
    /// from each replacement connection before it can be settled again.
    requires_authority_receipt: bool,
    /// Routing intent sent with this subscription's remote registration.
    remote_propagate_upstream: bool,
    read_view: ReadViewSpec,
    snapshot: RelationSnapshot,
    snapshot_index: RelationSnapshotIndex,
    snapshot_source: SubscriptionSnapshotSource,
    settled: bool,
    sender: UnboundedSender<SubscriptionEvent>,
}

#[derive(Clone, Default)]
struct RelationSnapshotIndex {
    roots: BTreeMap<OutputOccurrenceId, usize>,
    related: BTreeMap<(String, RowUuid), usize>,
    edges: BTreeSet<RelationEdge>,
}

impl RelationSnapshotIndex {
    fn from_snapshot(snapshot: &RelationSnapshot) -> Self {
        let mut index = Self::default();
        for (position, row) in snapshot.rows.iter().take(snapshot.root_count).enumerate() {
            index
                .roots
                .insert(subscription_row_occurrence_id(row), position);
        }
        for (offset, row) in snapshot.rows.iter().skip(snapshot.root_count).enumerate() {
            index.related.insert(
                (row.table().to_owned(), row.row_uuid()),
                snapshot.root_count + offset,
            );
        }
        index.edges = snapshot.edges.iter().cloned().collect();
        index
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SubscriptionSnapshotSource {
    LocalMaintained,
    LinkSnapshot,
}

enum SubscriptionKind {
    Prepared {
        shape: ValidatedQuery,
        binding: Binding,
        maintained_subscription: Option<LocalMaintainedViewSubscription>,
    },
}

/// Row identity removed from a materialized subscription result.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct RemovedRow {
    /// Logical table that contained the removed row.
    pub table: String,
    /// Stable row identity.
    pub row_uuid: RowUuid,
    /// Stable identity of the removed output occurrence.
    pub occurrence_id: OutputOccurrenceId,
}

impl RemovedRow {
    #[doc(hidden)]
    pub fn from_result_key(table: String, row_uuid: RowUuid, key: ResultKey) -> Self {
        Self {
            table,
            row_uuid,
            occurrence_id: key.as_occurrence().clone(),
        }
    }
}

/// One row addressed by its maintained output occurrence identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionOutputRow {
    /// Stable output occurrence identity.
    pub occurrence_id: OutputOccurrenceId,
    /// Materialized row body for this output occurrence.
    pub row: CurrentRow,
}

/// Immutable producer-owned decoding contract for a structured terminal root.
///
/// The maintained query compiler creates this alongside its app-row terminal.
/// Consumers install it before applying operations which name `id`; the
/// descriptor remains the source of truth for encoded types while these slots
/// map public fields to their physical record positions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TerminalRootLayout {
    /// Stable hash of the descriptor, slots, identities and carrier.
    pub id: String,
    /// Exact physical root descriptor used to decode packed bytes.
    pub root_descriptor: RecordDescriptor,
    /// Descriptor slot containing the stable root UUID.
    pub root_key_slot: usize,
    /// Exact descriptor identity of the root UUID slot.
    pub root_key_field_name: String,
    /// Public field-to-descriptor slot mappings, in public output order.
    pub public_fields: Vec<TerminalRootPublicField>,
    /// Physical representation used for public cells.
    pub carrier: TerminalRootCarrier,
}

/// One public root field's immutable physical slot identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TerminalRootPublicField {
    /// Public column name.
    pub name: String,
    /// Physical descriptor field name at `slot`.
    pub descriptor_field_name: String,
    /// Physical descriptor slot.
    pub slot: usize,
    /// Encoded representation of this individual slot.
    pub carrier: TerminalRootCarrier,
}

/// The producer representation applied around declared public column types.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TerminalRootCarrier {
    /// A physical `CurrentRow`: each application cell has one extra nullable
    /// carrier around its declared storage type.
    CurrentRow,
    /// A logical collector/projection record with declared storage types.
    Logical,
}

impl std::ops::Deref for SubscriptionOutputRow {
    type Target = CurrentRow;

    fn deref(&self) -> &Self::Target {
        &self.row
    }
}

/// Delta event emitted by a database subscription stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubscriptionEvent {
    /// Incremental or reset result change.
    Delta {
        /// Whether this delta replaces all previously observed rows and edges.
        ///
        /// Fresh subscriptions start with a reset delta from the empty result.
        reset: bool,
        /// Whether this event represents an authority-backed observation.
        /// A locally constructed opening frame is provisional until an
        /// authority publishes the corresponding reset.
        publishable: bool,
        /// Rows newly visible to the subscription.
        added: Vec<SubscriptionOutputRow>,
        /// Rows still visible with changed projected cells.
        updated: Vec<SubscriptionOutputRow>,
        /// Rows no longer visible to the subscription.
        removed: Vec<RemovedRow>,
        /// Typed structural edits to already hydrated terminal rows.
        terminal_operations: Vec<groove::ivm::TerminalOperation>,
        /// Immutable root decoding contract for `terminal_operations`, when
        /// this event carries structured terminal changes.
        terminal_layout: Option<TerminalRootLayout>,
        /// Whether the result is complete at the requested read tier.
        settled: bool,
        /// Read tier used to materialize the rows.
        tier: DurabilityTier,
    },
    /// The serving peer rejected the propagated upstream subscription.
    Rejected {
        /// Stable rejection class plus diagnostic detail from the serving peer.
        reason: SubscribeRejectReason,
    },
    /// The subscription stream was closed by the producer.
    Closed,
}

/// Stream of materialized subscription events.
pub struct SubscriptionStream {
    receiver: UnboundedReceiver<SubscriptionEvent>,
    _state: Rc<RefCell<SubscriptionState>>,
    cleanup: Option<Box<dyn FnOnce()>>,
}

struct CleanupGuard {
    cleanup: Option<Box<dyn FnOnce()>>,
}

impl CleanupGuard {
    fn new(cleanup: Box<dyn FnOnce()>) -> Self {
        Self {
            cleanup: Some(cleanup),
        }
    }

    fn take(&mut self) -> Box<dyn FnOnce()> {
        self.cleanup
            .take()
            .expect("cleanup guard must only be disarmed once")
    }
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        if let Some(cleanup) = self.cleanup.take() {
            cleanup();
        }
    }
}

impl SubscriptionStream {
    #[cfg(test)]
    async fn next_raw(&mut self) -> Option<SubscriptionEvent> {
        std::future::poll_fn(|cx| Pin::new(&mut self.receiver).poll_next(cx)).await
    }

    /// Await the next materialized subscription event.
    pub async fn next_event(&mut self) -> Option<SubscriptionEvent> {
        loop {
            let event =
                std::future::poll_fn(|cx| Pin::new(&mut self.receiver).poll_next(cx)).await?;
            if subscription_event_is_publishable(&event) {
                return Some(event);
            }
        }
    }

    /// Return the next queued materialized subscription event without waiting.
    pub fn try_next_event(&mut self) -> Option<SubscriptionEvent> {
        loop {
            let event = self.receiver.try_recv().ok()?;
            if subscription_event_is_publishable(&event) {
                return Some(event);
            }
        }
    }

    #[cfg(test)]
    fn retained_plan_authorization_mode(&self) -> Option<QueryAuthorizationMode> {
        let state = self._state.borrow();
        let SubscriptionKind::Prepared {
            maintained_subscription,
            ..
        } = &state.kind;
        maintained_subscription
            .as_ref()
            .and_then(LocalMaintainedViewSubscription::retained_plan_authorization_mode)
    }
}

fn subscription_event_is_publishable(event: &SubscriptionEvent) -> bool {
    !matches!(
        event,
        SubscriptionEvent::Delta {
            publishable: false,
            ..
        }
    )
}

impl Stream for SubscriptionStream {
    type Item = SubscriptionEvent;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match Pin::new(&mut this.receiver).poll_next(cx) {
                Poll::Ready(Some(event)) if subscription_event_is_publishable(&event) => {
                    return Poll::Ready(Some(event));
                }
                Poll::Ready(Some(_)) => continue,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl Drop for SubscriptionStream {
    fn drop(&mut self) {
        if let Some(cleanup) = self.cleanup.take() {
            cleanup();
        }
    }
}

/// Validated and bound query plan used by all `Db` reads and subscriptions.
#[derive(Clone, Debug)]
pub struct PreparedQuery {
    shape: ValidatedQuery,
    binding: Binding,
    local_plan: Option<PreparedQueryPlanHandle>,
    global_plan: Option<PreparedQueryPlanHandle>,
    /// Plans contain IDs from one live Groove graph registry. A catalogue
    /// change can replace that registry while this public handle survives.
    groove_runtime_token: u64,
}

impl PreparedQuery {
    /// Validated query shape.
    pub fn shape(&self) -> &ValidatedQuery {
        &self.shape
    }

    /// Bound parameter values.
    pub fn binding(&self) -> &Binding {
        &self.binding
    }

    fn plan_for_tier(
        &self,
        tier: DurabilityTier,
        groove_runtime_token: u64,
    ) -> Option<&PreparedQueryPlanHandle> {
        if self.groove_runtime_token != groove_runtime_token {
            return None;
        }
        match tier {
            DurabilityTier::Local => self.local_plan.as_ref(),
            DurabilityTier::Global => self.global_plan.as_ref(),
            DurabilityTier::None | DurabilityTier::Edge => None,
        }
    }

    #[cfg(test)]
    fn has_plan_for_tier(&self, tier: DurabilityTier) -> bool {
        self.plan_for_tier(tier, self.groove_runtime_token)
            .is_some()
    }
}

fn should_install_prepared_plan(shape: &ValidatedQuery) -> bool {
    !shape.query().joins.is_empty() || !shape.query().reachable.is_empty()
}

fn subscription_delta_event(
    tier: DurabilityTier,
    settled: bool,
    previous: &RelationSnapshot,
    current: &RelationSnapshot,
    terminal_rows: bool,
) -> SubscriptionEvent {
    subscription_delta_event_with_reset(tier, settled, previous, current, false, terminal_rows)
}

/// Publishes an ordered terminal as a root-addressed suffix splice.
///
/// A row delta has stable occurrence identities but no positional field. When
/// terminal ordering changes, retracting and re-adding the first changed suffix
/// is therefore the smallest representation that lets every consumer recover
/// the authoritative Groove order. Content-only changes retain their position
/// and remain ordinary `updated` roots, independent of total result size.
fn subscription_terminal_delta_event(
    tier: DurabilityTier,
    settled: bool,
    previous: &RelationSnapshot,
    previous_occurrences: &[OutputOccurrenceId],
    current: &RelationSnapshot,
    current_occurrences: &[OutputOccurrenceId],
) -> Result<SubscriptionEvent, Error> {
    let previous_roots = &previous.rows[..previous.root_count];
    let current_roots = &current.rows[..current.root_count];
    if previous_roots.len() != previous_occurrences.len()
        || current_roots.len() != current_occurrences.len()
    {
        return Err(Error::new(
            ErrorCode::Protocol,
            "maintained terminal occurrence sidecar length does not match root rows",
        ));
    }
    let common_prefix = previous_roots
        .iter()
        .zip(previous_occurrences)
        .zip(current_roots.iter().zip(current_occurrences))
        .take_while(|((_, previous), (_, current))| previous == current)
        .count();

    let updated = previous_roots[..common_prefix]
        .iter()
        .zip(&current_roots[..common_prefix])
        .zip(&current_occurrences[..common_prefix])
        .filter(|((previous, current), _)| previous != current)
        .map(|((_, current), occurrence_id)| SubscriptionOutputRow {
            occurrence_id: occurrence_id.clone(),
            row: current.clone(),
        })
        .collect();
    let removed = previous_roots[common_prefix..]
        .iter()
        .zip(&previous_occurrences[common_prefix..])
        .map(|(row, occurrence_id)| RemovedRow {
            table: row.table().to_owned(),
            row_uuid: row.row_uuid(),
            occurrence_id: occurrence_id.clone(),
        })
        .collect();
    let added = current_roots[common_prefix..]
        .iter()
        .zip(&current_occurrences[common_prefix..])
        .map(|(row, occurrence_id)| SubscriptionOutputRow {
            occurrence_id: occurrence_id.clone(),
            row: row.clone(),
        })
        .collect();

    Ok(SubscriptionEvent::Delta {
        reset: false,
        publishable: true,
        added,
        updated,
        removed,
        terminal_operations: Vec::new(),
        terminal_layout: None,
        settled,
        tier,
    })
}

fn subscription_delta_event_with_reset(
    tier: DurabilityTier,
    settled: bool,
    previous: &RelationSnapshot,
    current: &RelationSnapshot,
    reset: bool,
    _terminal_rows: bool,
) -> SubscriptionEvent {
    // A reset is a complete ordered snapshot.  Re-keying it through the
    // occurrence BTreeMap below would sort by identity and silently discard
    // the maintained query order (for example `order_by rank`).
    if reset {
        return SubscriptionEvent::Delta {
            reset: true,
            publishable: true,
            added: current
                .rows
                .iter()
                .cloned()
                .map(subscription_output_row)
                .collect(),
            updated: Vec::new(),
            removed: Vec::new(),
            terminal_operations: Vec::new(),
            terminal_layout: None,
            settled,
            tier,
        };
    }
    let mut previous_by_id = BTreeMap::new();
    for row in &previous.rows {
        previous_by_id.insert(subscription_row_key(row), row);
    }

    let mut current_by_id = BTreeMap::new();
    for row in &current.rows {
        current_by_id.insert(subscription_row_key(row), row);
    }

    let mut added = Vec::new();
    let mut updated = Vec::new();
    let mut removed = Vec::new();
    for (key, row) in &current_by_id {
        match previous_by_id.get(key) {
            None => added.push(subscription_output_row((*row).clone())),
            Some(previous_row) if *previous_row != *row => {
                updated.push(subscription_output_row((*row).clone()))
            }
            Some(_) => {}
        }
    }

    for (key, _) in &previous_by_id {
        if !current_by_id.contains_key(key) {
            let row = previous_by_id[key];
            removed.push(RemovedRow {
                table: row.table().to_owned(),
                row_uuid: row.row_uuid(),
                occurrence_id: key.clone(),
            });
        }
    }

    SubscriptionEvent::Delta {
        reset,
        publishable: true,
        added,
        updated,
        removed,
        terminal_operations: Vec::new(),
        terminal_layout: None,
        settled,
        tier,
    }
}

fn apply_maintained_update_to_snapshot(
    snapshot: &mut RelationSnapshot,
    snapshot_index: &mut RelationSnapshotIndex,
    update: LocalMaintainedViewSubscriptionUpdate,
    tier: DurabilityTier,
    settled: bool,
    _terminal_rows: bool,
) -> SubscriptionEvent {
    let LocalMaintainedViewSubscriptionUpdate {
        authoritative_membership_changed: _,
        added: update_added,
        removed: update_removed,
        added_edges: update_added_edges,
        removed_edges: update_removed_edges,
        terminal_operations,
        terminal_layout,
    } = update;

    if snapshot.rows.is_empty()
        && snapshot.edges.is_empty()
        && snapshot.root_count == 0
        && update_removed.is_empty()
        && update_removed_edges.is_empty()
    {
        if update_added_edges.is_empty() {
            snapshot.root_count = update_added.len();
            snapshot.rows.reserve(update_added.len());
            snapshot
                .rows
                .extend(update_added.iter().map(|(_, row)| row.clone()));
            snapshot_index.roots = update_added
                .iter()
                .enumerate()
                .map(|(index, (occurrence, _))| (occurrence.clone(), index))
                .collect();
            return SubscriptionEvent::Delta {
                reset: false,
                publishable: true,
                added: update_added
                    .into_iter()
                    .map(|(occurrence_id, row)| SubscriptionOutputRow { occurrence_id, row })
                    .collect(),
                updated: Vec::new(),
                removed: Vec::new(),
                terminal_operations: terminal_operations.clone(),
                terminal_layout: terminal_layout.clone(),
                settled,
                tier,
            };
        }

        let mut event_added = Vec::with_capacity(update_added.len());
        let mut added_related = Vec::new();
        let mut seen_rows = BTreeSet::new();
        for (occurrence_id, row) in &update_added {
            seen_rows.insert((row.table().to_owned(), row.row_uuid()));
            event_added.push((occurrence_id.clone(), row.clone()));
        }

        let mut seen_edges = BTreeSet::new();
        for (edge, row) in &update_added_edges {
            if seen_edges.insert(edge.clone()) {
                snapshot.edges.push(edge.clone());
            }
            let Some(row) = row else {
                continue;
            };
            if seen_rows.insert((row.table().to_owned(), row.row_uuid())) {
                added_related.push(row.clone());
            }
        }

        snapshot.root_count = event_added.len();
        snapshot
            .rows
            .reserve(event_added.len() + added_related.len());
        snapshot
            .rows
            .extend(event_added.iter().map(|(_, row)| row.clone()));
        snapshot.rows.extend(added_related.iter().cloned());
        *snapshot_index = RelationSnapshotIndex::from_snapshot(snapshot);
        snapshot_index.roots = event_added
            .iter()
            .enumerate()
            .map(|(index, (occurrence, _))| (occurrence.clone(), index))
            .collect();

        return SubscriptionEvent::Delta {
            reset: false,
            publishable: true,
            added: event_added
                .into_iter()
                .map(|(occurrence_id, row)| SubscriptionOutputRow { occurrence_id, row })
                .collect(),
            updated: Vec::new(),
            removed: Vec::new(),
            terminal_operations: terminal_operations.clone(),
            terminal_layout: terminal_layout.clone(),
            settled,
            tier,
        };
    }

    let mut added = Vec::new();
    let mut updated = Vec::new();
    let mut removed = Vec::new();
    let mut added_related = Vec::new();

    for (key, row) in &update_added {
        if let Some(position) = snapshot_index.roots.get(&key).copied() {
            if snapshot.rows[position] != *row {
                snapshot.rows[position] = row.clone();
                updated.push(SubscriptionOutputRow {
                    occurrence_id: key.clone(),
                    row: row.clone(),
                });
            }
        } else {
            snapshot.rows.insert(snapshot.root_count, row.clone());
            for position in snapshot_index.related.values_mut() {
                *position += 1;
            }
            snapshot_index
                .roots
                .insert(key.clone(), snapshot.root_count);
            snapshot.root_count += 1;
            added.push(SubscriptionOutputRow {
                occurrence_id: key.clone(),
                row: row.clone(),
            });
        }
    }

    let mut index = 0;
    while index < snapshot.root_count {
        let occurrence_id = snapshot_index
            .roots
            .iter()
            .find_map(|(occurrence, position)| (*position == index).then(|| occurrence.clone()))
            .expect("every maintained root row has an occurrence index");
        if update_removed.contains(&occurrence_id)
            && !update_added
                .iter()
                .any(|(added, _)| *added == occurrence_id)
        {
            let row = snapshot.rows.remove(index);
            snapshot.root_count -= 1;
            snapshot_index.roots.remove(&occurrence_id);
            for position in snapshot_index.roots.values_mut() {
                if *position > index {
                    *position -= 1;
                }
            }
            for position in snapshot_index.related.values_mut() {
                *position -= 1;
            }
            removed.push(RemovedRow {
                table: row.table().to_owned(),
                row_uuid: row.row_uuid(),
                occurrence_id,
            });
        } else {
            index += 1;
        }
    }

    if !update_removed_edges.is_empty() {
        snapshot.edges.retain(|edge| {
            let remove = update_removed_edges.iter().any(|removed| removed == edge);
            if remove {
                snapshot_index.edges.remove(edge);
            }
            !remove
        });
    }

    for (edge, row) in &update_added_edges {
        if snapshot_index.edges.insert(edge.clone()) {
            snapshot.edges.push(edge.clone());
        }
        let Some(row) = row else {
            continue;
        };
        let root_key = subscription_row_occurrence_id(row);
        if snapshot_index.roots.contains_key(&root_key) {
            continue;
        }
        let key = (row.table().to_owned(), row.row_uuid());
        if let Some(position) = snapshot_index.related.get(&key).copied() {
            snapshot.rows[position] = row.clone();
        } else {
            snapshot_index.related.insert(key, snapshot.rows.len());
            snapshot.rows.push(row.clone());
        }
        added_related.push(row.clone());
    }

    for removed_edge in &update_removed_edges {
        let still_referenced = snapshot_index.edges.iter().any(|edge| {
            edge.target_table == removed_edge.target_table
                && edge.target_row == removed_edge.target_row
        });
        let target_key = (removed_edge.target_table.clone(), removed_edge.target_row);
        let is_root = snapshot_index
            .roots
            .contains_key(&OutputOccurrenceId::single_source(ObjectId::from_uuid(
                target_key.1.0,
            )));
        if !still_referenced && !is_root {
            if let Some(position) = snapshot_index.related.remove(&target_key) {
                snapshot.rows.remove(position);
                for indexed_position in snapshot_index.related.values_mut() {
                    if *indexed_position > position {
                        *indexed_position -= 1;
                    }
                }
            }
        }
    }

    SubscriptionEvent::Delta {
        reset: false,
        publishable: true,
        added,
        updated,
        removed,
        terminal_operations,
        terminal_layout,
        settled,
        tier,
    }
}

/// Restore the query's observable root order after applying a maintained
/// membership transition. Groove owns membership/windowing, while this helper
/// only orders the selected roots before their row-only delta is bridged to an
/// application subscription.
fn order_maintained_snapshot_roots<S>(
    node: &NodeState<S>,
    query: &crate::query::Query,
    snapshot: &mut RelationSnapshot,
    snapshot_index: &mut RelationSnapshotIndex,
) -> Result<(), Error>
where
    S: OrderedKvStorage,
{
    let mut roots = snapshot.rows[..snapshot.root_count].to_vec();
    let mut occurrences = snapshot_root_occurrences(snapshot, snapshot_index)?;
    node.apply_query_order_with_occurrences(query, &mut roots, &mut occurrences)?;
    snapshot.rows[..snapshot.root_count].clone_from_slice(&roots);
    snapshot_index.roots = root_occurrence_positions(&occurrences);
    Ok(())
}

fn snapshot_root_occurrences(
    snapshot: &RelationSnapshot,
    snapshot_index: &RelationSnapshotIndex,
) -> Result<Vec<OutputOccurrenceId>, Error> {
    let mut occurrences = vec![None; snapshot.root_count];
    for (occurrence, position) in &snapshot_index.roots {
        let slot = occurrences.get_mut(*position).ok_or_else(|| {
            Error::new(
                ErrorCode::Protocol,
                "maintained root occurrence index exceeds snapshot roots",
            )
        })?;
        *slot = Some(occurrence.clone());
    }
    occurrences
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| {
            Error::new(
                ErrorCode::Protocol,
                "maintained snapshot root is missing an occurrence identity",
            )
        })
}

fn root_occurrence_positions(
    occurrences: &[OutputOccurrenceId],
) -> BTreeMap<OutputOccurrenceId, usize> {
    occurrences
        .iter()
        .cloned()
        .enumerate()
        .map(|(position, occurrence)| (occurrence, position))
        .collect()
}

fn relation_snapshot_index_with_root_occurrences(
    snapshot: &RelationSnapshot,
    occurrences: &[OutputOccurrenceId],
) -> Result<RelationSnapshotIndex, Error> {
    if snapshot.root_count != occurrences.len() {
        return Err(Error::new(
            ErrorCode::Protocol,
            "maintained terminal occurrence sidecar length does not match root rows",
        ));
    }
    let mut index = RelationSnapshotIndex::from_snapshot(snapshot);
    index.roots = root_occurrence_positions(occurrences);
    if index.roots.len() != occurrences.len() {
        return Err(Error::new(
            ErrorCode::Protocol,
            "maintained terminal occurrence sidecar contains duplicate identity",
        ));
    }
    Ok(index)
}

/// Flat tuple identity is carried by the maintained terminal sidecar, not the
/// public row: an external reset may omit hidden joined-row fields. Preserve
/// that sidecar whenever its materialized snapshot is the replacement being
/// installed; ordinary link-only snapshots retain row-derived indexing.
fn maintained_snapshot_index_or_row_index<S>(
    node: &mut NodeState<S>,
    kind: &SubscriptionKind,
    snapshot: &RelationSnapshot,
) -> Result<RelationSnapshotIndex, Error>
where
    S: OrderedKvStorage,
{
    let SubscriptionKind::Prepared {
        shape,
        maintained_subscription: Some(maintained),
        ..
    } = kind
    else {
        return Ok(RelationSnapshotIndex::from_snapshot(snapshot));
    };
    if shape.query().flat_join.is_none() {
        return Ok(RelationSnapshotIndex::from_snapshot(snapshot));
    }
    let materialized =
        node.materialize_local_maintained_relation_snapshot_with_occurrences(maintained)?;
    if materialized.snapshot.root_count == snapshot.root_count {
        return relation_snapshot_index_with_root_occurrences(
            snapshot,
            &materialized.root_occurrence_ids,
        );
    }
    Ok(RelationSnapshotIndex::from_snapshot(snapshot))
}

fn relation_snapshot_with_delta_slack(snapshot: &RelationSnapshot) -> RelationSnapshot {
    let mut snapshot = snapshot.clone();
    reserve_relation_snapshot_delta_slack(&mut snapshot);
    snapshot
}

fn reserve_relation_snapshot_delta_slack(snapshot: &mut RelationSnapshot) {
    fn slack(len: usize) -> usize {
        (len / 8).max(64)
    }

    snapshot.rows.reserve(slack(snapshot.rows.len()));
    snapshot.edges.reserve(slack(snapshot.edges.len()));
}

fn subscription_is_settled<S>(
    node: &NodeState<S>,
    active_authority_view_receipts: &ActiveAuthorityViewReceipts,
    shape: &ValidatedQuery,
    binding: &Binding,
    tier: DurabilityTier,
    read_view: ReadViewSpec,
    propagate_upstream: bool,
    requires_authority_receipt: bool,
) -> bool
where
    S: OrderedKvStorage,
{
    if tier <= DurabilityTier::Local {
        return true;
    }
    let binding_view_key = BindingViewKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: RegisterShapeOptions {
            tier,
            read_view,
            propagate_upstream,
        }
        .read_view_key(),
    };
    node.has_settled_result_set(binding_view_key)
        && !node.opening_pending_for_binding_view(binding_view_key)
        && (!requires_authority_receipt
            || active_authority_view_receipts
                .borrow()
                .as_ref()
                .is_some_and(|receipts| receipts.binding_views.contains(&binding_view_key)))
}

pub(crate) fn subscription_row_occurrence_id(row: &CurrentRow) -> OutputOccurrenceId {
    let root = ObjectId::from_uuid(row.row_uuid().0);
    let mut joined = Vec::new();
    for position in 1.. {
        let Some(Value::Uuid(row_id)) = row.raw_field(&format!("__flat_join_row_{position}"))
        else {
            break;
        };
        joined.push(ObjectId::from_uuid(row_id));
    }
    OutputOccurrenceId::new(root, joined)
}

fn subscription_outputs_with_occurrence_sidecar(
    snapshot: &RelationSnapshot,
    occurrence_ids: &[OutputOccurrenceId],
) -> Result<Vec<SubscriptionOutputRow>, Error> {
    if occurrence_ids.len() != snapshot.root_count {
        return Err(Error::new(
            ErrorCode::Protocol,
            "maintained root occurrence sidecar length does not match root rows",
        ));
    }
    let mut unique = BTreeSet::new();
    snapshot
        .rows
        .iter()
        .take(snapshot.root_count)
        .zip(occurrence_ids)
        .map(|(row, occurrence_id)| {
            let bytes = occurrence_id.canonical_bytes();
            if bytes.get(..16) != Some(row.row_uuid().0.as_bytes()) {
                return Err(Error::new(
                    ErrorCode::Protocol,
                    "maintained root occurrence sidecar root does not match row",
                ));
            }
            if !unique.insert(occurrence_id.clone()) {
                return Err(Error::new(
                    ErrorCode::Protocol,
                    "maintained root occurrence sidecar contains duplicate identity",
                ));
            }
            Ok(SubscriptionOutputRow {
                occurrence_id: occurrence_id.clone(),
                row: row.clone(),
            })
        })
        .collect()
}

fn reset_removed_roots(
    previous: &RelationSnapshot,
    previous_index: &RelationSnapshotIndex,
    current_occurrences: &[OutputOccurrenceId],
) -> Vec<RemovedRow> {
    let current = current_occurrences.iter().collect::<BTreeSet<_>>();
    previous_index
        .roots
        .iter()
        .filter(|(occurrence, _)| !current.contains(occurrence))
        .map(|(occurrence_id, position)| {
            let row = &previous.rows[*position];
            RemovedRow {
                table: row.table().to_owned(),
                row_uuid: row.row_uuid(),
                occurrence_id: occurrence_id.clone(),
            }
        })
        .collect()
}

fn subscription_output_row(row: CurrentRow) -> SubscriptionOutputRow {
    SubscriptionOutputRow {
        occurrence_id: subscription_row_occurrence_id(&row),
        row,
    }
}

fn subscription_row_key(row: &CurrentRow) -> OutputOccurrenceId {
    subscription_row_occurrence_id(row)
}

#[cfg(test)]
mod tests;
