//! Per-peer server-side sync state outside the Jazz data model. This module
//! owns shipped-complete-transaction-payload deduplication and per-subscription incremental result
//! maintenance for one downstream peer; subscriber-side settled subscription
//! result-set/completeness state lives on [`crate::node::NodeState`], and view construction itself
//! lives in [`crate::node::views`]. It sits beside the node in the layer map as
//! link-local state used to produce protocol messages.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::mpsc::TryRecvError;

use groove::ivm::{RecordDeltas, Subscription};
use groove::storage::OrderedKvStorage;

use crate::ids::{AuthorId, RowUuid};
use crate::node::content_store::Extent;
use crate::node::maintained_subscription_view::{
    MaintainedSubscriptionView,
    MaintainedSubscriptionViewFootprint as MaintainedSubscriptionViewIndexFootprint,
};
use crate::node::{Error, NodeState, PreparedQueryPlan};
use crate::protocol::{
    ContentExtent, PeerPayloadInventory, RegisterShapeOptions, ResultRowEntry, SubscriptionKey,
    SyncMessage, VersionBundle, VersionRecord,
};
use crate::query::{Binding, ValidatedQuery};
use crate::schema::TableSchema;
use crate::tx::{DurabilityTier, Transaction, TxId, TxKind};

const LARGE_REHYDRATE_RESULT_ROWS: usize = 1024;
const DEFAULT_EDGE_SCOPE_TTL_MS: u64 = 5_000;

/// Tracks what one downstream peer has already received.
#[derive(Debug)]
pub struct PeerState {
    role: PeerRole,
    permission_identity: Option<AuthorId>,
    shipped_complete_tx_payloads: BTreeSet<TxId>,
    ship_complete_exclusive_payloads: bool,
    subscriptions: BTreeMap<SubscriptionKey, PeerSubscriptionState>,
    deferred_edge_fates: BTreeMap<TxId, DeferredEdgeFate>,
    edge_scope_subscription_refs: BTreeMap<SubscriptionKey, usize>,
    idle_edge_scope_subscriptions: BTreeMap<SubscriptionKey, u64>,
    #[cfg(test)]
    force_full_recompute_path_for_test: bool,
    /// Deterministic counters for this peer.
    pub metrics: PeerMetrics,
}

/// Server-side role for one peer link.
///
/// Relay links are permanent topology links between non-client nodes and serve
/// system identity views. Edge-client links terminate one connecting client
/// identity at the edge boundary; all query reads served on that link are
/// policy-composed for the terminated identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PeerRole {
    /// Permanent relay/cache link to another node.
    Relay,
    /// Edge boundary link serving one terminated client identity.
    EdgeClient {
        /// Client author identity terminated at this edge boundary.
        identity: AuthorId,
    },
}

impl PeerRole {
    fn identity(self) -> AuthorId {
        match self {
            Self::Relay => AuthorId::SYSTEM,
            Self::EdgeClient { identity } => identity,
        }
    }
}

/// Server-side shipped-state for one downstream subscription on a `PeerState`.
///
/// In a real topology this lives on the upstream/server node's per-peer link
/// state and records what that peer has already been sent, including closure
/// contributions needed to emit incremental view updates. This has the same
/// `ResultRowEntry` shape as `NodeState::settled_result_sets`, but that node
/// map is the downstream subscriber's settled subscription result-set/completeness
/// state, not a cryptographic proof or protocol authority.
#[derive(Debug, Default)]
struct PeerSubscriptionState {
    closure_contributions: BTreeMap<ResultRowEntry, BTreeSet<ResultRowEntry>>,
    closure_contributions_complete: bool,
    row_index: BTreeMap<RowKey, RowSlot>,
    query_subscription: Option<Subscription>,
    maintained_subscription_view: Option<MaintainedSubscriptionViewSubscription>,
    prepared_query: Option<CachedPeerQueryPlan>,
    groove_runtime_token: Option<u64>,
}

impl PeerSubscriptionState {
    fn clear_groove_runtime_handles(&mut self) {
        self.query_subscription = None;
        self.maintained_subscription_view = None;
        self.prepared_query = None;
        self.groove_runtime_token = None;
    }
}

#[derive(Debug)]
struct MaintainedSubscriptionViewSubscription {
    subscription: Subscription,
    maintained: MaintainedSubscriptionView,
    tables: BTreeMap<String, TableSchema>,
}

struct MaintainedRehydrateRequest<'a> {
    shape: &'a ValidatedQuery,
    binding: &'a Binding,
    subscription: SubscriptionKey,
    previous_row_result_set: &'a BTreeSet<ResultRowEntry>,
    reset_result_set: bool,
    result_table_filter: Option<&'a str>,
}

type RowKey = (groove::Intern<String>, RowUuid);

#[derive(Debug)]
struct CachedPeerQueryPlan {
    shape: ValidatedQuery,
    binding: Binding,
    plan: PreparedQueryPlan,
}

#[derive(Clone, Debug)]
struct DeferredEdgeFate {
    tx: Transaction,
    versions: Vec<VersionRecord>,
    now_ms: u64,
    permission_identity: AuthorId,
    scope_subscriptions: Vec<SubscriptionKey>,
}

/// Peer-owned inputs to the edge eviction pin set.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PeerEvictionPins {
    /// Transactions currently parked on edge fate assignment.
    pub deferred_edge_fate_txs: BTreeSet<TxId>,
    /// Permission-scope subscriptions retained by active edge acceptance gates.
    pub referenced_scope_subscriptions: BTreeSet<SubscriptionKey>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RowSlot {
    entry: ResultRowEntry,
    refcount: usize,
}

impl PeerSubscriptionState {
    fn row_result_set(&self) -> BTreeSet<ResultRowEntry> {
        self.row_index.values().map(|slot| slot.entry).collect()
    }

    fn previous_tx_ids(&self) -> BTreeSet<TxId> {
        self.row_index.values().map(|slot| slot.entry.2).collect()
    }

    fn contains_entry(&self, entry: &ResultRowEntry) -> bool {
        self.row_index
            .get(&(entry.0, entry.1))
            .is_some_and(|slot| slot.entry == *entry)
    }

    fn entry_for_key(&self, key: RowKey) -> Option<ResultRowEntry> {
        self.row_index.get(&key).map(|slot| slot.entry)
    }
}

impl Default for PeerState {
    fn default() -> Self {
        Self {
            role: PeerRole::Relay,
            permission_identity: None,
            shipped_complete_tx_payloads: BTreeSet::new(),
            ship_complete_exclusive_payloads: false,
            subscriptions: BTreeMap::new(),
            deferred_edge_fates: BTreeMap::new(),
            edge_scope_subscription_refs: BTreeMap::new(),
            idle_edge_scope_subscriptions: BTreeMap::new(),
            #[cfg(test)]
            force_full_recompute_path_for_test: false,
            metrics: PeerMetrics::default(),
        }
    }
}

fn edge_scope_ttl_ms() -> u64 {
    std::env::var("JAZZ_EDGE_SCOPE_TTL_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_EDGE_SCOPE_TTL_MS)
}

impl PeerState {
    /// Construct a permanent relay peer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct a permanent relay peer.
    pub fn relay() -> Self {
        Self::default()
    }

    /// Construct an edge peer that terminates one client author identity.
    pub fn edge_client(identity: AuthorId) -> Self {
        Self {
            role: PeerRole::EdgeClient { identity },
            ..Self::default()
        }
    }

    /// Construct an edge peer whose wire identity and read-policy identity differ.
    ///
    /// Trusted backend websocket links still speak as their concrete peer identity
    /// for session/resume validation, but served reads must bypass row policies.
    pub fn edge_client_with_permission_identity(
        identity: AuthorId,
        permission_identity: AuthorId,
    ) -> Self {
        Self {
            role: PeerRole::EdgeClient { identity },
            permission_identity: Some(permission_identity),
            ..Self::default()
        }
    }

    /// Construct a peer narrowed to one author identity.
    ///
    /// This is retained as the compatibility spelling for edge-client links.
    pub fn for_author(identity: AuthorId) -> Self {
        Self::edge_client(identity)
    }

    /// Return the named role for this peer link.
    pub fn role(&self) -> PeerRole {
        self.role
    }

    /// Return the wire/session identity for this peer link.
    pub fn link_identity(&self) -> AuthorId {
        self.role.identity()
    }

    /// Return the identity used to evaluate reads on this peer link.
    pub fn identity(&self) -> AuthorId {
        self.permission_identity
            .unwrap_or_else(|| self.role.identity())
    }

    #[cfg(test)]
    pub(crate) fn force_full_recompute_path_for_test(&mut self, force: bool) {
        self.force_full_recompute_path_for_test = force;
    }

    #[cfg(test)]
    pub(crate) fn clear_query_subscription_for_test(&mut self, subscription: SubscriptionKey) {
        if let Some(state) = self.subscriptions.get_mut(&subscription) {
            state.query_subscription = None;
            state.prepared_query = None;
        }
    }

    fn force_full_recompute_path(&self) -> bool {
        #[cfg(test)]
        {
            self.force_full_recompute_path_for_test
        }
        #[cfg(not(test))]
        {
            false
        }
    }

    fn full_recompute_oracle_enabled(&self) -> bool {
        self.force_full_recompute_path()
    }

    fn clear_stale_groove_runtime_handles<S>(
        &mut self,
        node: &NodeState<S>,
        subscription: SubscriptionKey,
    ) where
        S: OrderedKvStorage,
    {
        let current_token = node.groove_runtime_token();
        if self.subscriptions.get(&subscription).is_some_and(|state| {
            state
                .groove_runtime_token
                .is_some_and(|token| token != current_token)
        }) {
            if let Some(state) = self.subscriptions.get_mut(&subscription) {
                state.clear_groove_runtime_handles();
            }
            self.refresh_maintained_subscription_view_footprint(subscription);
        }
    }

    /// Builds a full current-row view update, using tx-level refs for complete
    /// transaction payloads in this peer's inventory and bundles for new or
    /// partial view payload.
    pub fn current_rows_update<S>(
        &mut self,
        node: &mut NodeState<S>,
        table: &str,
    ) -> Result<SyncMessage, Error>
    where
        S: OrderedKvStorage,
    {
        let (shape, binding) = node.whole_table_shape_binding(table)?;
        let subscription = SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
        };
        self.clear_stale_groove_runtime_handles(node, subscription);
        let needs_prepare = self
            .subscriptions
            .get(&subscription)
            .and_then(|state| state.prepared_query.as_ref())
            .is_none();
        if needs_prepare {
            let (prepared_shape, prepared_binding, plan) = node.prepare_query_binding_for_link(
                &shape,
                &binding,
                DurabilityTier::Global,
                self.identity(),
            )?;
            let cached = CachedPeerQueryPlan {
                shape: prepared_shape,
                binding: prepared_binding,
                plan,
            };
            let state = self.subscriptions.entry(subscription).or_default();
            state.prepared_query = Some(cached);
            state.groove_runtime_token = Some(node.groove_runtime_token());
        } else {
            self.subscriptions.entry(subscription).or_default();
        }
        let previous_row_result_set = self
            .subscriptions
            .get(&subscription)
            .map(PeerSubscriptionState::row_result_set)
            .unwrap_or_default();
        if self
            .subscriptions
            .get(&subscription)
            .and_then(|state| state.maintained_subscription_view.as_ref())
            .is_none()
        {
            match self.maintained_subscription_view_support(node, &shape, &binding) {
                Ok(()) => {
                    match self.rehydrate_query_maintained_subscription_view(
                        node,
                        MaintainedRehydrateRequest {
                            shape: &shape,
                            binding: &binding,
                            subscription,
                            previous_row_result_set: &previous_row_result_set,
                            reset_result_set: false,
                            result_table_filter: Some(table),
                        },
                    ) {
                        Ok(update) => return Ok(update),
                        Err(err) => return Err(err),
                    }
                }
                Err(err) if self.full_recompute_oracle_enabled() => {
                    self.metrics
                        .maintained_subscription_view
                        .unsupported_skips_out += 1;
                    let _ = err;
                }
                Err(err) => return Err(err),
            }
        }
        if self
            .subscriptions
            .get(&subscription)
            .and_then(|state| state.maintained_subscription_view.as_ref())
            .is_some()
            && !self.full_recompute_oracle_enabled()
        {
            return self.query_update_maintained_subscription_view(
                node,
                &shape,
                &binding,
                subscription,
                Some(table),
            );
        }
        let prepared_plan = self
            .subscriptions
            .get(&subscription)
            .and_then(|state| state.prepared_query.as_ref())
            .map(|prepared| (&prepared.shape, &prepared.binding, &prepared.plan));
        let previous_tx_ids = previous_tx_ids(previous_row_result_set.iter());
        let mut update = node.view_update_for_query_binding_with_peer_payload_inventory_and_plan(
            &shape,
            &binding,
            subscription,
            self.shipped_complete_tx_payloads.iter().cloned(),
            previous_tx_ids,
            previous_row_result_set,
            self.identity(),
            prepared_plan,
        )?;
        filter_view_update_to_result_table(&mut update, table);
        self.record_outgoing_view_update(&update);
        let update = update;
        if self
            .subscriptions
            .get(&subscription)
            .and_then(|state| state.query_subscription.as_ref())
            .is_none()
            && self
                .subscriptions
                .get(&subscription)
                .and_then(|state| state.maintained_subscription_view.as_ref())
                .is_none()
            && let Some(prepared) = self
                .subscriptions
                .get(&subscription)
                .and_then(|state| state.prepared_query.as_ref())
            && let Some(receiver) =
                node.subscribe_query_binding_with_plan(&prepared.binding, &prepared.plan)?
        {
            drain_initial_subscription_snapshot(&receiver);
            let state = self.subscriptions.entry(subscription).or_default();
            state.query_subscription = Some(receiver);
            state.groove_runtime_token = Some(node.groove_runtime_token());
            self.rebuild_closure_contributions(node, &shape, &binding, subscription)?;
        }
        Ok(update)
    }

    /// Builds a query-binding view update, using tx-level refs for complete
    /// transaction payloads in this peer's inventory and bundles for new or
    /// partial view payload.
    pub fn query_update<S>(
        &mut self,
        node: &mut NodeState<S>,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<SyncMessage, Error>
    where
        S: OrderedKvStorage,
    {
        self.query_update_inner(node, shape, binding)
    }

    #[cfg(test)]
    pub(crate) fn track_query_for_test<S>(
        &mut self,
        node: &mut NodeState<S>,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<(), Error>
    where
        S: OrderedKvStorage,
    {
        let subscription = SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
        };
        let (prepared_shape, prepared_binding, plan) = node.prepare_query_binding_for_link(
            shape,
            binding,
            DurabilityTier::Global,
            self.identity(),
        )?;
        let cached = CachedPeerQueryPlan {
            shape: prepared_shape,
            binding: prepared_binding,
            plan,
        };
        let state = self.subscriptions.entry(subscription).or_default();
        state.prepared_query = Some(cached);
        state.groove_runtime_token = Some(node.groove_runtime_token());
        Ok(())
    }

    fn query_update_inner<S>(
        &mut self,
        node: &mut NodeState<S>,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<SyncMessage, Error>
    where
        S: OrderedKvStorage,
    {
        let subscription = SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
        };
        self.clear_stale_groove_runtime_handles(node, subscription);
        let Some(state) = self.subscriptions.get(&subscription) else {
            return Ok(SyncMessage::ViewUpdate {
                subscription,
                reset_result_set: false,
                version_bundles: Vec::new(),
                peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
                result_row_adds: Vec::new(),
                result_row_removes: Vec::new(),
            });
        };
        if state.maintained_subscription_view.is_some() && !self.full_recompute_oracle_enabled() {
            return self.query_update_maintained_subscription_view(
                node,
                shape,
                binding,
                subscription,
                None,
            );
        }
        if let Some(receiver) = state.query_subscription.as_ref() {
            let mut drained = Vec::new();
            loop {
                match receiver.try_recv() {
                    Ok(deltas) => {
                        if !deltas.is_empty() {
                            drained.push(deltas);
                        }
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        break;
                    }
                }
            }
            if drained.is_empty() {
                return Ok(SyncMessage::ViewUpdate {
                    subscription,
                    reset_result_set: false,
                    version_bundles: Vec::new(),
                    peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
                    result_row_adds: Vec::new(),
                    result_row_removes: Vec::new(),
                });
            } else {
                let update =
                    self.query_update_from_deltas(node, shape, binding, subscription, drained)?;
                self.record_outgoing_view_update_metadata(&update);
                return Ok(update);
            }
        }
        if !self.full_recompute_oracle_enabled() {
            return Err(Error::InvalidStoredValue(
                "live query subscription is missing maintained state",
            ));
        }
        let previous_row_result_set = state.row_result_set();
        let prepared_plan = state
            .prepared_query
            .as_ref()
            .map(|prepared| (&prepared.shape, &prepared.binding, &prepared.plan));
        let update = node.view_update_for_query_binding_with_peer_payload_inventory_and_plan(
            shape,
            binding,
            subscription,
            self.shipped_complete_tx_payloads.iter().cloned(),
            state.previous_tx_ids(),
            previous_row_result_set,
            self.identity(),
            prepared_plan,
        )?;
        self.record_outgoing_view_update(&update);
        Ok(update)
    }

    fn query_update_maintained_subscription_view<S>(
        &mut self,
        node: &mut NodeState<S>,
        shape: &ValidatedQuery,
        binding: &Binding,
        subscription: SubscriptionKey,
        result_table_filter: Option<&str>,
    ) -> Result<SyncMessage, Error>
    where
        S: OrderedKvStorage,
    {
        let previous_row_result_set = self
            .subscriptions
            .get(&subscription)
            .map(PeerSubscriptionState::row_result_set)
            .unwrap_or_default();
        self.maintained_subscription_view_support(node, shape, binding)?;
        let output_tables = node.maintained_view_terminal_tables(shape)?;
        let mut states = BTreeMap::<ResultRowEntry, (bool, bool)>::new();
        {
            let Some(maintained_subscription_view) = self
                .subscriptions
                .get_mut(&subscription)
                .and_then(|state| state.maintained_subscription_view.as_mut())
            else {
                return Ok(SyncMessage::ViewUpdate {
                    subscription,
                    reset_result_set: false,
                    version_bundles: Vec::new(),
                    peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
                    result_row_adds: Vec::new(),
                    result_row_removes: Vec::new(),
                });
            };
            loop {
                match maintained_subscription_view.subscription.try_recv() {
                    Ok(deltas) => {
                        self.metrics.maintained_subscription_view.delta_batches_in += 1;
                        let transitions = maintained_subscription_view
                            .maintained
                            .apply_tagged_deltas(
                                &deltas,
                                &maintained_subscription_view.tables,
                                &node.node_aliases,
                            )?;
                        for entry in transitions.adds {
                            let before = previous_row_result_set.contains(&entry);
                            states
                                .entry(entry)
                                .and_modify(|(_, after)| *after = true)
                                .or_insert((before, true));
                        }
                        for entry in transitions.removes {
                            let before = previous_row_result_set.contains(&entry);
                            states
                                .entry(entry)
                                .and_modify(|(_, after)| *after = false)
                                .or_insert((before, false));
                        }
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => break,
                }
            }
        }
        if states.is_empty() {
            return Ok(SyncMessage::ViewUpdate {
                subscription,
                reset_result_set: false,
                version_bundles: Vec::new(),
                peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
                result_row_adds: Vec::new(),
                result_row_removes: Vec::new(),
            });
        }
        let mut result_row_adds = Vec::new();
        let mut result_row_removes = Vec::new();
        for (entry, (before, after)) in states {
            if result_table_filter.is_some_and(|table| entry.0.as_str() != table) {
                continue;
            }
            if !output_tables.contains_key(entry.0.as_str()) {
                continue;
            }
            match (before, after) {
                (false, true) => result_row_adds.push(entry),
                (true, false) => result_row_removes.push(entry),
                _ => {}
            }
        }
        let previous_result_tx_ids = previous_tx_ids(previous_row_result_set.iter());
        let peer_complete_tx_payloads = self.shipped_complete_tx_payloads.iter().copied().collect();
        let update = {
            let maintained = &self
                .subscriptions
                .get(&subscription)
                .and_then(|state| state.maintained_subscription_view.as_ref())
                .ok_or(Error::InvalidStoredValue(
                    "maintained subscription view subscription missing",
                ))?
                .maintained;
            node.view_update_for_query_result_delta_maintained_view_add_bundles(
                crate::node::MaintainedViewBundleInputs {
                    subscription,
                    peer_complete_tx_payloads,
                    complete_exclusive_payloads: self.ship_complete_exclusive_payloads,
                    previous_result_set: previous_result_tx_ids,
                    result_row_adds,
                    result_row_removes,
                    identity: self.identity(),
                    versions_by_tx: |tx_id| maintained.versions_by_tx(tx_id),
                    replacement_for: |table: String, row_uuid| {
                        maintained.replacement_for(&table, row_uuid)
                    },
                },
            )
        };
        let update = update?;
        self.metrics.maintained_subscription_view.hits_out += 1;
        self.refresh_maintained_subscription_view_footprint(subscription);
        self.record_outgoing_view_update(&update);
        Ok(update)
    }

    fn maintained_subscription_view_support<S>(
        &self,
        node: &NodeState<S>,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<(), Error>
    where
        S: OrderedKvStorage,
    {
        if self.force_full_recompute_path() {
            return Err(Error::InvalidStoredValue(
                "maintained subscription view disabled for this peer",
            ));
        }
        node.maintained_view_support(shape, binding, self.identity())
    }

    fn rehydrate_query_maintained_subscription_view<S>(
        &mut self,
        node: &mut NodeState<S>,
        request: MaintainedRehydrateRequest<'_>,
    ) -> Result<SyncMessage, Error>
    where
        S: OrderedKvStorage,
    {
        let MaintainedRehydrateRequest {
            shape,
            binding,
            subscription,
            previous_row_result_set,
            reset_result_set,
            result_table_filter,
        } = request;
        let (receiver, maintained, transitions, tables) =
            node.maintained_subscription_view_from_cold_snapshot(shape, binding, self.identity())?;
        let output_tables = node.maintained_view_terminal_tables(shape)?;
        let result_row_adds = transitions
            .adds
            .into_iter()
            .filter(|entry| {
                result_table_filter.is_none_or(|table| entry.0.as_str() == table)
                    && output_tables.contains_key(entry.0.as_str())
            })
            .collect::<Vec<_>>();
        let current_row_result_set = result_row_adds.iter().copied().collect::<BTreeSet<_>>();
        let result_row_removes = previous_row_result_set
            .difference(&current_row_result_set)
            .copied()
            .collect::<Vec<_>>();
        let peer_complete_tx_payloads = self.shipped_complete_tx_payloads.iter().copied().collect();
        let update = node.view_update_for_query_result_delta_maintained_view_add_bundles(
            crate::node::MaintainedViewBundleInputs {
                subscription,
                peer_complete_tx_payloads,
                complete_exclusive_payloads: self.ship_complete_exclusive_payloads,
                previous_result_set: BTreeSet::new(),
                result_row_adds,
                result_row_removes,
                identity: self.identity(),
                versions_by_tx: |tx_id| maintained.versions_by_tx(tx_id),
                replacement_for: |table: String, row_uuid| {
                    maintained.replacement_for(&table, row_uuid)
                },
            },
        );
        let mut update = match update {
            Ok(update) => update,
            Err(err) => {
                node.unsubscribe_groove_subscription(receiver.id());
                return Err(err);
            }
        };
        if reset_result_set {
            view_update_reset_result_set(&mut update);
        }
        let maintained_subscription = MaintainedSubscriptionViewSubscription {
            subscription: receiver,
            maintained,
            tables,
        };
        let state = self.subscriptions.entry(subscription).or_default();
        state.maintained_subscription_view = Some(maintained_subscription);
        state.groove_runtime_token = Some(node.groove_runtime_token());
        self.record_outgoing_view_update(&update);
        self.metrics.maintained_subscription_view.hits_out += 1;
        self.refresh_maintained_subscription_view_footprint(subscription);
        Ok(update)
    }

    fn rehydrate_query_full_recompute_path<S>(
        &mut self,
        node: &mut NodeState<S>,
        shape: &ValidatedQuery,
        binding: &Binding,
        subscription: SubscriptionKey,
        previous_row_result_set: BTreeSet<ResultRowEntry>,
        previous_tx_ids: BTreeSet<TxId>,
        tier: DurabilityTier,
    ) -> Result<SyncMessage, Error>
    where
        S: OrderedKvStorage,
    {
        let prepared_plan = self
            .subscriptions
            .get(&subscription)
            .and_then(|state| state.prepared_query.as_ref())
            .map(|prepared| (&prepared.shape, &prepared.binding, &prepared.plan));
        let mut update = node
            .view_update_for_query_binding_with_peer_payload_inventory_and_plan_at_tier(
                shape,
                binding,
                subscription,
                self.shipped_complete_tx_payloads.iter().cloned(),
                [],
                [],
                self.identity(),
                prepared_plan,
                tier,
            )?;
        if !previous_row_result_set.is_empty() {
            let diff_update = node
                .view_update_for_query_binding_with_peer_payload_inventory_and_plan_at_tier(
                    shape,
                    binding,
                    subscription,
                    self.shipped_complete_tx_payloads.iter().cloned(),
                    previous_tx_ids,
                    previous_row_result_set,
                    self.identity(),
                    prepared_plan,
                    tier,
                )?;
            merge_rehydrate_diff(&mut update, diff_update);
        }
        view_update_reset_result_set(&mut update);
        // Large cold rehydrates store the complete result set on the peer but
        // defer per-output closure bookkeeping. Future non-live pulls can
        // full-diff from the row index, and any later live delta that needs
        // missing contribution/refcount state falls back to a full diff first.
        let large_rehydrate = view_update_result_add_count(&update) > LARGE_REHYDRATE_RESULT_ROWS;
        if !large_rehydrate
            && let Some(prepared) = self
                .subscriptions
                .get(&subscription)
                .and_then(|state| state.prepared_query.as_ref())
            && let Some(receiver) =
                node.subscribe_query_binding_with_plan(&prepared.binding, &prepared.plan)?
        {
            drain_initial_subscription_snapshot(&receiver);
            let state = self.subscriptions.entry(subscription).or_default();
            state.query_subscription = Some(receiver);
            state.groove_runtime_token = Some(node.groove_runtime_token());
        }
        self.record_outgoing_view_update(&update);
        if !large_rehydrate {
            self.rebuild_closure_contributions(node, shape, binding, subscription)?;
        }
        Ok(update)
    }

    /// Build a reset-result_set current-row view update.
    pub fn rehydrate_current_rows<S>(
        &mut self,
        node: &mut NodeState<S>,
        table: &str,
    ) -> Result<SyncMessage, Error>
    where
        S: OrderedKvStorage,
    {
        let (shape, binding) = node.whole_table_shape_binding(table)?;
        self.rehydrate_query(node, &shape, &binding)
    }

    /// Build a reset-result-set query-binding view update.
    pub fn rehydrate_query<S>(
        &mut self,
        node: &mut NodeState<S>,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<SyncMessage, Error>
    where
        S: OrderedKvStorage,
    {
        self.rehydrate_query_with_opts(node, shape, binding, RegisterShapeOptions::default())
    }

    /// Build a reset-result-set query-binding view update with registration options.
    pub fn rehydrate_query_with_opts<S>(
        &mut self,
        node: &mut NodeState<S>,
        shape: &ValidatedQuery,
        binding: &Binding,
        opts: RegisterShapeOptions,
    ) -> Result<SyncMessage, Error>
    where
        S: OrderedKvStorage,
    {
        let subscription = SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
        };
        self.clear_stale_groove_runtime_handles(node, subscription);
        let previous_row_result_set = self
            .subscriptions
            .get(&subscription)
            .map(PeerSubscriptionState::row_result_set)
            .unwrap_or_default();
        let previous_tx_ids = previous_tx_ids(previous_row_result_set.iter());
        self.forget_subscription_with_node(node, subscription);
        let (prepared_shape, prepared_binding, plan) =
            node.prepare_query_binding_for_link(shape, binding, opts.tier, self.identity())?;
        let cached = CachedPeerQueryPlan {
            shape: prepared_shape,
            binding: prepared_binding,
            plan,
        };
        let state = self.subscriptions.entry(subscription).or_default();
        state.prepared_query = Some(cached);
        state.groove_runtime_token = Some(node.groove_runtime_token());
        if self.full_recompute_oracle_enabled() || opts.tier != DurabilityTier::Global {
            return self.rehydrate_query_full_recompute_path(
                node,
                shape,
                binding,
                subscription,
                previous_row_result_set,
                previous_tx_ids,
                opts.tier,
            );
        }
        self.maintained_subscription_view_support(node, shape, binding)?;
        self.rehydrate_query_maintained_subscription_view(
            node,
            MaintainedRehydrateRequest {
                shape,
                binding,
                subscription,
                previous_row_result_set: &previous_row_result_set,
                reset_result_set: true,
                result_table_filter: None,
            },
        )
    }

    /// Handles a rehydrate request for the current-row view carried by this
    /// peer. The node remains simulation-first: the embedding chooses which peer/table a
    /// subscription ref belongs to, and the peer rebuilds result_set from data.
    pub fn handle_current_rows_rehydrate<S>(
        &mut self,
        node: &mut NodeState<S>,
        table: &str,
        message: SyncMessage,
    ) -> Result<SyncMessage, Error>
    where
        S: OrderedKvStorage,
    {
        let SyncMessage::Rehydrate { subscription } = message else {
            return Err(Error::UnsupportedSyncMessage("non-rehydrate peer request"));
        };
        let expected = node.whole_table_subscription_key(table)?;
        if subscription != expected {
            return Err(Error::UnsupportedSyncMessage(
                "rehydrate for different query",
            ));
        }
        self.rehydrate_current_rows(node, table)
    }

    /// Drops only the per-subscription result_set cache. Version payload dedup
    /// is per-peer and survives subscription rehydration.
    pub fn forget_subscription(&mut self, subscription: SubscriptionKey) {
        self.subscriptions.remove(&subscription);
    }

    /// Drop one subscription and eagerly unregister any maintained Groove
    /// subscription from the runtime before dropping the receiver.
    pub fn forget_subscription_with_node<S>(
        &mut self,
        node: &mut NodeState<S>,
        subscription: SubscriptionKey,
    ) -> bool
    where
        S: OrderedKvStorage,
    {
        let Some(mut state) = self.subscriptions.remove(&subscription) else {
            return false;
        };
        let unsubscribed = state.groove_runtime_token == Some(node.groove_runtime_token())
            && state
                .maintained_subscription_view
                .take()
                .is_some_and(|maintained| {
                    node.unsubscribe_groove_subscription(maintained.subscription.id())
                });
        drop(state);
        unsubscribed
    }

    /// Drop one query-binding result set on this peer.
    pub fn forget_query_binding(&mut self, shape: &ValidatedQuery, binding: &Binding) {
        self.forget_subscription(SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
        });
    }

    /// Drop one query-binding result set and eagerly unregister any maintained
    /// Groove subscription from the runtime before dropping the receiver.
    pub fn forget_query_binding_with_node<S>(
        &mut self,
        node: &mut NodeState<S>,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> bool
    where
        S: OrderedKvStorage,
    {
        self.forget_subscription_with_node(
            node,
            SubscriptionKey {
                shape_id: shape.shape_id(),
                binding_id: binding.binding_id(),
            },
        )
    }

    /// Return transaction refs whose complete payload bundles have shipped on this peer.
    pub fn shipped_complete_tx_payloads(&self) -> &BTreeSet<TxId> {
        &self.shipped_complete_tx_payloads
    }

    /// Configure whether accepted exclusive transactions should ship complete
    /// payloads so the downstream can safely author later exclusive
    /// transactions from refreshed state.
    pub fn set_ship_complete_exclusive_payloads(&mut self, enabled: bool) {
        self.ship_complete_exclusive_payloads = enabled;
    }

    /// Snapshot peer-owned pin-set roots for edge-cache eviction.
    pub fn eviction_pins(&self) -> PeerEvictionPins {
        PeerEvictionPins {
            deferred_edge_fate_txs: self.deferred_edge_fates.keys().copied().collect(),
            referenced_scope_subscriptions: self
                .edge_scope_subscription_refs
                .keys()
                .chain(self.idle_edge_scope_subscriptions.keys())
                .copied()
                .collect(),
        }
    }

    /// Forget complete-tx payload dedup markers for transactions whose local
    /// payloads were evicted, so a standard rehydrate may resend them.
    pub fn forget_evicted_versions(&mut self, tx_ids: impl IntoIterator<Item = TxId>) -> usize {
        tx_ids
            .into_iter()
            .filter(|tx_id| self.shipped_complete_tx_payloads.remove(tx_id))
            .count()
    }

    #[cfg(test)]
    pub(crate) fn retain_edge_scope_subscription_for_test(
        &mut self,
        subscription: SubscriptionKey,
    ) {
        self.retain_edge_scope_subscription(subscription);
    }

    #[cfg(test)]
    pub(crate) fn release_edge_scope_subscription_for_test(
        &mut self,
        subscription: SubscriptionKey,
        now_ms: u64,
    ) {
        let Some(refcount) = self.edge_scope_subscription_refs.get_mut(&subscription) else {
            return;
        };
        *refcount -= 1;
        if *refcount == 0 {
            self.edge_scope_subscription_refs.remove(&subscription);
            if edge_scope_ttl_ms() != 0 {
                self.idle_edge_scope_subscriptions
                    .insert(subscription, now_ms);
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn defer_edge_fate_for_test(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        now_ms: u64,
    ) {
        let permission_identity = self.identity();
        self.deferred_edge_fates.insert(
            tx.tx_id,
            DeferredEdgeFate {
                tx,
                versions,
                now_ms,
                permission_identity,
                scope_subscriptions: Vec::new(),
            },
        );
    }

    /// Serve one bulk-lane content extent fetch for this peer.
    pub fn handle_content_extent_fetch<S>(
        &mut self,
        node: &mut NodeState<S>,
        message: SyncMessage,
    ) -> Result<SyncMessage, Error>
    where
        S: OrderedKvStorage,
    {
        let SyncMessage::FetchContentExtent { row, extent } = message else {
            return Err(Error::UnsupportedSyncMessage(
                "non-content-fetch peer request",
            ));
        };
        self.serve_content_extents(node, row, [extent])
    }

    /// Build a bulk-lane response for extents that belong to one row.
    pub fn serve_content_extents<S>(
        &mut self,
        node: &mut NodeState<S>,
        row: RowUuid,
        extents: impl IntoIterator<Item = Extent>,
    ) -> Result<SyncMessage, Error>
    where
        S: OrderedKvStorage,
    {
        let mut out = Vec::new();
        for extent in extents {
            if extent.row != row {
                return Err(Error::UnsupportedSyncMessage(
                    "content extent row context mismatch",
                ));
            }
            if !node.content_extent_visible_to(row, &extent, self.identity())? {
                return Err(Error::UnsupportedSyncMessage(
                    "content extent is not visible for row",
                ));
            }
            out.push(ContentExtent {
                bytes: node.content_store().read(&extent)?,
                extent,
            });
        }
        Ok(SyncMessage::ContentExtents { extents: out })
    }

    /// Return current result_set for one subscription.
    pub fn subscription_result_sets(
        &self,
        subscription: SubscriptionKey,
    ) -> Option<BTreeSet<TxId>> {
        self.subscriptions
            .get(&subscription)
            .map(PeerSubscriptionState::previous_tx_ids)
    }

    /// Return this peer's maintained subscription view counters and latest footprint.
    pub fn maintained_subscription_view_metrics(&self) -> MaintainedSubscriptionViewMetrics {
        *self.metrics.maintained_subscription_view
    }

    /// Ingest a client mergeable commit unit at an edge boundary.
    ///
    /// The edge first stores the unit as pending relay history, then gates fate
    /// assignment on the first settled permission-scope subscription for the
    /// affected tables and writer. If a scope was not settled before this call,
    /// the unit remains pending and can be completed by
    /// [`Self::drain_deferred_edge_fates`] after the registered scope settles.
    pub fn ingest_edge_mergeable_commit_unit<S>(
        &mut self,
        node: &mut NodeState<S>,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        now_ms: u64,
    ) -> Result<Vec<SyncMessage>, Error>
    where
        S: OrderedKvStorage,
    {
        self.evict_idle_edge_scope_subscriptions(node, now_ms);
        if tx.kind != TxKind::Mergeable {
            return Err(Error::UnsupportedCommitUnit(
                "edge fate deferral only supports mergeable commit units",
            ));
        }
        let permission_identity = self.identity();
        if let Some(scope_subscriptions) = self.unsettled_permission_scope_subscriptions(
            node,
            permission_identity,
            &versions,
            true,
        )? {
            node.ingest_relay_commit_unit(tx.clone(), versions.clone())?;
            if !self.deferred_edge_fates.contains_key(&tx.tx_id) {
                for subscription in &scope_subscriptions {
                    self.retain_edge_scope_subscription(*subscription);
                }
                self.deferred_edge_fates.insert(
                    tx.tx_id,
                    DeferredEdgeFate {
                        tx,
                        versions,
                        now_ms,
                        permission_identity,
                        scope_subscriptions,
                    },
                );
            }
            return Ok(Vec::new());
        }
        node.ingest_edge_authority_mergeable_commit_unit_with_identity(
            tx,
            versions,
            now_ms,
            permission_identity,
        )
    }

    /// Assign fates for edge-ingested writes whose permission scopes have now
    /// delivered an initial settled result.
    pub fn drain_deferred_edge_fates<S>(
        &mut self,
        node: &mut NodeState<S>,
        now_ms: u64,
    ) -> Result<Vec<SyncMessage>, Error>
    where
        S: OrderedKvStorage,
    {
        self.evict_idle_edge_scope_subscriptions(node, now_ms);
        let deferred = self
            .deferred_edge_fates
            .iter()
            .map(|(tx_id, fate)| (*tx_id, fate.clone()))
            .collect::<Vec<_>>();
        let mut updates = Vec::new();
        for (tx_id, fate) in deferred {
            if self
                .unsettled_permission_scope_subscriptions(
                    node,
                    fate.permission_identity,
                    &fate.versions,
                    false,
                )?
                .is_some()
            {
                continue;
            }
            self.deferred_edge_fates.remove(&tx_id);
            for subscription in fate.scope_subscriptions {
                self.release_edge_scope_subscription(node, subscription, now_ms);
            }
            updates.extend(
                node.ingest_edge_authority_mergeable_commit_unit_with_identity(
                    fate.tx,
                    fate.versions,
                    fate.now_ms,
                    fate.permission_identity,
                )?,
            );
        }
        Ok(updates)
    }

    /// Number of edge fate assignments currently parked on permission scopes.
    pub fn deferred_edge_fate_count(&self) -> usize {
        self.deferred_edge_fates.len()
    }

    /// Number of distinct permission-scope subscriptions retained by deferred
    /// edge fate gates.
    pub fn edge_scope_subscription_count(&self) -> usize {
        self.edge_scope_subscription_refs.len()
    }

    fn record_outgoing_view_update_metadata(&mut self, update: &SyncMessage) {
        let SyncMessage::ViewUpdate {
            version_bundles,
            peer_payload_inventory,
            result_row_adds,
            result_row_removes,
            ..
        } = update
        else {
            return;
        };

        self.metrics.view_updates_out += 1;
        self.metrics.version_bundles_out += version_bundles.len() as u64;
        self.metrics.complete_tx_payload_refs_out +=
            peer_payload_inventory.complete_tx_payloads.len() as u64;
        self.metrics.result_adds_out += result_row_adds.len() as u64;
        self.metrics.result_removes_out += result_row_removes.len() as u64;

        for bundle in version_bundles {
            if !bundle_contains_complete_tx_payload(bundle) {
                continue;
            }
            if !self.shipped_complete_tx_payloads.insert(bundle.tx.tx_id) {
                self.metrics.duplicate_version_bundles_out += 1;
            }
        }
    }

    fn unsettled_permission_scope_subscriptions<S>(
        &mut self,
        node: &mut NodeState<S>,
        writer: AuthorId,
        versions: &[VersionRecord],
        retained_scope_is_unsettled: bool,
    ) -> Result<Option<Vec<SubscriptionKey>>, Error>
    where
        S: OrderedKvStorage,
    {
        let mut unsettled = Vec::new();
        let tables = versions
            .iter()
            .map(|version| version.table().to_owned())
            .collect::<BTreeSet<_>>();
        for table in tables {
            let Some((shape, binding)) = node.permission_scope_shape_binding(&table, writer)?
            else {
                continue;
            };
            let subscription = SubscriptionKey {
                shape_id: shape.shape_id(),
                binding_id: binding.binding_id(),
            };
            if retained_scope_is_unsettled
                && self
                    .edge_scope_subscription_refs
                    .contains_key(&subscription)
            {
                unsettled.push(subscription);
                continue;
            }
            if self.subscriptions.get(&subscription).is_some_and(|state| {
                state.query_subscription.is_some() || state.maintained_subscription_view.is_some()
            }) {
                continue;
            }
            let previous_role = self.role;
            self.role = PeerRole::EdgeClient { identity: writer };
            let rehydrate = self.rehydrate_query(node, &shape, &binding);
            self.role = previous_role;
            let _ = rehydrate?;
            unsettled.push(subscription);
        }
        if unsettled.is_empty() {
            Ok(None)
        } else {
            Ok(Some(unsettled))
        }
    }

    fn retain_edge_scope_subscription(&mut self, subscription: SubscriptionKey) {
        self.idle_edge_scope_subscriptions.remove(&subscription);
        *self
            .edge_scope_subscription_refs
            .entry(subscription)
            .or_default() += 1;
    }

    fn release_edge_scope_subscription<S>(
        &mut self,
        node: &mut NodeState<S>,
        subscription: SubscriptionKey,
        now_ms: u64,
    ) where
        S: OrderedKvStorage,
    {
        let Some(refcount) = self.edge_scope_subscription_refs.get_mut(&subscription) else {
            return;
        };
        *refcount -= 1;
        if *refcount == 0 {
            self.edge_scope_subscription_refs.remove(&subscription);
            if edge_scope_ttl_ms() == 0 {
                self.forget_subscription_with_node(node, subscription);
            } else {
                self.idle_edge_scope_subscriptions
                    .insert(subscription, now_ms);
            }
        }
    }

    fn evict_idle_edge_scope_subscriptions<S>(&mut self, node: &mut NodeState<S>, now_ms: u64)
    where
        S: OrderedKvStorage,
    {
        let ttl_ms = edge_scope_ttl_ms();
        if ttl_ms == 0 {
            let idle = std::mem::take(&mut self.idle_edge_scope_subscriptions);
            for subscription in idle.into_keys() {
                self.forget_subscription_with_node(node, subscription);
            }
            return;
        }

        let expired = self
            .idle_edge_scope_subscriptions
            .iter()
            .filter_map(|(subscription, idle_since_ms)| {
                (now_ms.saturating_sub(*idle_since_ms) >= ttl_ms).then_some(*subscription)
            })
            .collect::<Vec<_>>();
        for subscription in expired {
            self.idle_edge_scope_subscriptions.remove(&subscription);
            self.forget_subscription_with_node(node, subscription);
        }
    }

    fn record_outgoing_view_update(&mut self, update: &SyncMessage) {
        self.record_outgoing_view_update_metadata(update);
        self.apply_outgoing_view_update_result_set(update);
    }

    fn refresh_maintained_subscription_view_footprint(&mut self, subscription: SubscriptionKey) {
        self.metrics.maintained_subscription_view.footprint = self
            .subscriptions
            .get(&subscription)
            .and_then(|state| state.maintained_subscription_view.as_ref())
            .map(|maintained| maintained.maintained.footprint())
            .map(MaintainedSubscriptionViewMetricsFootprint::from)
            .unwrap_or_default();
    }

    fn apply_outgoing_view_update_result_set(&mut self, update: &SyncMessage) {
        let SyncMessage::ViewUpdate {
            subscription,
            reset_result_set,
            result_row_adds,
            result_row_removes,
            ..
        } = update
        else {
            return;
        };
        let state = self.subscriptions.entry(*subscription).or_default();
        if *reset_result_set {
            state.closure_contributions.clear();
            state.closure_contributions_complete = false;
            state.row_index.clear();
        }
        for member in result_row_removes {
            apply_contribution_remove(state, std::iter::once(member), &mut Vec::new());
        }
        for member in result_row_adds {
            apply_contribution_add(
                state,
                std::iter::once(member),
                &mut Vec::new(),
                &mut Vec::new(),
            );
        }
        // Diagnostic-only invariant check: detecting duplicate content versions
        // in the result set requires materializing and scanning it, which is
        // wasted work in release where the debug_assert compiles out. Gate the
        // whole scan to debug builds so it never runs on the release hot path
        // (this sat under the measured record_outgoing_view_update hotspot).
        #[cfg(debug_assertions)]
        {
            let row_result_set = state.row_result_set();
            if let Some((table, row_uuid, first, second)) =
                duplicate_row_result_set(&row_result_set)
            {
                debug_assert!(
                    first == second,
                    "peer subscription {subscription:?} has multiple content versions for {table}.{row_uuid:?}: {first:?} and {second:?}"
                );
            }
        }
    }

    fn query_update_from_deltas<S>(
        &mut self,
        node: &mut NodeState<S>,
        shape: &ValidatedQuery,
        binding: &Binding,
        subscription: SubscriptionKey,
        deltas: Vec<RecordDeltas>,
    ) -> Result<SyncMessage, Error>
    where
        S: OrderedKvStorage,
    {
        // Fold the cycle's deltas by net weight per output entry first: a row
        // can enter AND leave the result within one drain cycle, and batch
        // order must not decide which side wins. Same-entry retract+assert
        // pairs cancel here, which also drops redundant re-assertions without
        // any per-row recheck.
        let mut net_weights = BTreeMap::<ResultRowEntry, i64>::new();
        for delta_batch in deltas {
            for (record, weight) in delta_batch.iter() {
                if weight != 0 {
                    let entry = if weight > 0 {
                        node.query_output_entry_from_delta(shape, record)?
                    } else {
                        node.query_output_entry_from_retraction(shape, record)?
                    };
                    *net_weights.entry(entry).or_insert(0) += weight;
                }
            }
        }
        let mut row_outcomes =
            BTreeMap::<(groove::Intern<String>, RowUuid), (Option<ResultRowEntry>, bool)>::new();
        for (entry, weight) in net_weights {
            if weight == 0 {
                continue;
            }
            let outcome = row_outcomes
                .entry((entry.0, entry.1))
                .or_insert((None, false));
            if weight > 0 {
                debug_assert!(
                    outcome.0.is_none(),
                    "two net-positive versions for one row in a single drain cycle: {:?} and {entry:?}",
                    outcome.0
                );
                outcome.0 = Some(entry);
            } else {
                outcome.1 = true;
            }
        }
        let mut touched_rows =
            BTreeMap::<(groove::Intern<String>, RowUuid), Option<ResultRowEntry>>::new();
        for (key, (positive, any_negative)) in row_outcomes {
            match positive {
                Some(entry) => {
                    let already_present = self
                        .subscriptions
                        .get(&subscription)
                        .is_some_and(|state| state.contains_entry(&entry));
                    if already_present && !any_negative {
                        // Pure re-assertion of the entry the peer already
                        // ships; nothing to recompute.
                        continue;
                    }
                    touched_rows.insert(key, Some(entry));
                }
                None if any_negative => {
                    touched_rows.insert(key, None);
                }
                None => {}
            }
        }
        self.repair_touched_output_closure_contributions(
            node,
            shape,
            binding,
            subscription,
            &touched_rows,
        )?;
        if !self.can_apply_whole_table_delta_without_sibling_repair(shape, binding) {
            self.repair_missing_exclusive_sibling_touches(
                node,
                shape,
                subscription,
                &mut touched_rows,
            )?;
        }
        let mut result_row_adds = Vec::new();
        let mut result_row_removes = Vec::new();
        for (key, positive_entry) in touched_rows {
            if key.0.as_str() == shape.query().table {
                let existing_output = self
                    .subscriptions
                    .get(&subscription)
                    .and_then(|state| state.entry_for_key(key));
                if let Some(output) = existing_output
                    && let Some(contribution) = self
                        .subscriptions
                        .entry(subscription)
                        .or_default()
                        .closure_contributions
                        .remove(&output)
                {
                    let state = self.subscriptions.entry(subscription).or_default();
                    apply_contribution_remove(state, contribution.iter(), &mut result_row_removes);
                }
                if let Some(output) = positive_entry {
                    let contribution = node.query_output_closure_contribution(
                        shape,
                        binding,
                        output,
                        self.identity(),
                    )?;
                    let state = self.subscriptions.entry(subscription).or_default();
                    apply_contribution_add(
                        state,
                        contribution.iter(),
                        &mut result_row_adds,
                        &mut result_row_removes,
                    );
                    state.closure_contributions.insert(output, contribution);
                }
            } else if let Some(entry) = positive_entry {
                let state = self.subscriptions.entry(subscription).or_default();
                replace_index_entry(state, entry, &mut result_row_adds, &mut result_row_removes);
            } else {
                let state = self.subscriptions.entry(subscription).or_default();
                remove_index_key(state, key, &mut result_row_removes);
            }
        }
        debug_assert_subscription_state(
            self.subscriptions
                .get(&subscription)
                .expect("subscription state exists after delta update"),
            subscription,
        );
        node.view_update_for_query_result_delta(
            subscription,
            self.shipped_complete_tx_payloads.iter().cloned(),
            previous_tx_ids(result_row_removes.iter()),
            result_row_adds,
            result_row_removes,
            self.identity(),
        )
    }

    fn repair_touched_output_closure_contributions<S>(
        &mut self,
        node: &mut NodeState<S>,
        shape: &ValidatedQuery,
        binding: &Binding,
        subscription: SubscriptionKey,
        touched_rows: &BTreeMap<(groove::Intern<String>, RowUuid), Option<ResultRowEntry>>,
    ) -> Result<(), Error>
    where
        S: OrderedKvStorage,
    {
        if touched_rows.is_empty() {
            return Ok(());
        }
        let missing_outputs = self
            .subscriptions
            .get(&subscription)
            .map(|state| {
                touched_rows
                    .keys()
                    .filter(|key| key.0.as_str() == shape.query().table)
                    .filter_map(|key| state.entry_for_key(*key))
                    .filter(|output| !state.closure_contributions.contains_key(output))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        for output in missing_outputs {
            let contribution =
                node.query_output_closure_contribution(shape, binding, output, self.identity())?;
            self.subscriptions
                .entry(subscription)
                .or_default()
                .closure_contributions
                .insert(output, contribution);
        }
        Ok(())
    }

    fn repair_missing_exclusive_sibling_touches<S>(
        &mut self,
        node: &mut NodeState<S>,
        shape: &ValidatedQuery,
        subscription: SubscriptionKey,
        touched_rows: &mut BTreeMap<(groove::Intern<String>, RowUuid), Option<ResultRowEntry>>,
    ) -> Result<(), Error>
    where
        S: OrderedKvStorage,
    {
        let positive_entries = touched_rows
            .values()
            .filter_map(|entry| *entry)
            .collect::<BTreeSet<_>>();
        let positive_tx_ids = positive_entries
            .iter()
            .map(|(_, _, tx_id)| *tx_id)
            .collect::<BTreeSet<_>>();
        for tx_id in positive_tx_ids {
            let visible_siblings = node.visible_exclusive_tx_result_entries_for_table(
                &shape.query().table,
                tx_id,
                self.identity(),
            )?;
            for sibling in visible_siblings {
                let already_present = self
                    .subscriptions
                    .get(&subscription)
                    .is_some_and(|state| state.contains_entry(&sibling));
                if !already_present && !positive_entries.contains(&sibling) {
                    touched_rows.insert((sibling.0, sibling.1), Some(sibling));
                }
            }
        }
        Ok(())
    }

    fn can_apply_whole_table_delta_without_sibling_repair(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> bool {
        self.identity() == AuthorId::SYSTEM && is_degenerate_whole_table(shape, binding)
    }

    fn rebuild_closure_contributions<S>(
        &mut self,
        node: &mut NodeState<S>,
        shape: &ValidatedQuery,
        binding: &Binding,
        subscription: SubscriptionKey,
    ) -> Result<(), Error>
    where
        S: OrderedKvStorage,
    {
        let Some(row_set) = self
            .subscriptions
            .get(&subscription)
            .map(PeerSubscriptionState::row_result_set)
        else {
            let state = self.subscriptions.entry(subscription).or_default();
            state.closure_contributions.clear();
            state.closure_contributions_complete = true;
            state.row_index.clear();
            return Ok(());
        };
        self.rebuild_closure_contributions_from_set(node, shape, binding, subscription, &row_set)
    }

    fn rebuild_closure_contributions_from_set<S>(
        &mut self,
        node: &mut NodeState<S>,
        shape: &ValidatedQuery,
        binding: &Binding,
        subscription: SubscriptionKey,
        row_set: &BTreeSet<ResultRowEntry>,
    ) -> Result<(), Error>
    where
        S: OrderedKvStorage,
    {
        let outputs = row_set
            .iter()
            .filter(|(table, _, _)| table.as_str() == shape.query().table)
            .cloned()
            .collect::<Vec<_>>();
        let contributions =
            node.query_output_closure_contributions(shape, binding, outputs, self.identity())?;
        let state = self.subscriptions.entry(subscription).or_default();
        state.closure_contributions = contributions;
        state.closure_contributions_complete = true;
        rebuild_row_index_from_contributions(state);
        debug_assert_subscription_state(state, subscription);
        Ok(())
    }
}

fn apply_contribution_add<'a>(
    state: &mut PeerSubscriptionState,
    contribution: impl IntoIterator<Item = &'a ResultRowEntry>,
    result_row_adds: &mut Vec<ResultRowEntry>,
    result_row_removes: &mut Vec<ResultRowEntry>,
) {
    for entry in contribution {
        let key = (entry.0, entry.1);
        match state.row_index.get_mut(&key) {
            Some(slot) if slot.entry == *entry => {
                slot.refcount += 1;
            }
            Some(slot) if entry.2 > slot.entry.2 => {
                let old_entry = slot.entry;
                result_row_removes.push(old_entry);
                result_row_adds.push(*entry);
                slot.entry = *entry;
                slot.refcount += 1;
            }
            Some(slot) => {
                slot.refcount += 1;
            }
            None => {
                state.row_index.insert(
                    key,
                    RowSlot {
                        entry: *entry,
                        refcount: 1,
                    },
                );
                result_row_adds.push(*entry);
            }
        }
    }
}

fn apply_contribution_remove<'a>(
    state: &mut PeerSubscriptionState,
    contribution: impl IntoIterator<Item = &'a ResultRowEntry>,
    result_row_removes: &mut Vec<ResultRowEntry>,
) {
    for entry in contribution {
        let key = (entry.0, entry.1);
        let Some(slot) = state.row_index.get_mut(&key) else {
            continue;
        };
        if slot.refcount > 1 {
            slot.refcount -= 1;
        } else {
            let removed = slot.entry;
            state.row_index.remove(&key);
            result_row_removes.push(removed);
        }
    }
}

fn replace_index_entry(
    state: &mut PeerSubscriptionState,
    entry: ResultRowEntry,
    result_row_adds: &mut Vec<ResultRowEntry>,
    result_row_removes: &mut Vec<ResultRowEntry>,
) {
    let key = (entry.0, entry.1);
    if let Some(slot) = state.row_index.get_mut(&key)
        && slot.entry != entry
    {
        result_row_removes.push(slot.entry);
        slot.entry = entry;
        result_row_adds.push(entry);
    }
}

fn remove_index_key(
    state: &mut PeerSubscriptionState,
    key: RowKey,
    result_row_removes: &mut Vec<ResultRowEntry>,
) {
    if let Some(slot) = state.row_index.remove(&key) {
        result_row_removes.push(slot.entry);
    }
}

fn rebuild_row_index_from_contributions(state: &mut PeerSubscriptionState) {
    state.row_index.clear();
    let contributions = state
        .closure_contributions
        .values()
        .flat_map(|contribution| contribution.iter().copied())
        .collect::<Vec<_>>();
    apply_contribution_add(
        state,
        contributions.iter(),
        &mut Vec::new(),
        &mut Vec::new(),
    );
}

fn debug_assert_subscription_state(
    #[allow(unused_variables)] state: &PeerSubscriptionState,
    #[allow(unused_variables)] subscription: SubscriptionKey,
) {
    #[cfg(debug_assertions)]
    {
        if state.closure_contributions_complete && state.closure_contributions.len() <= 2048 {
            let mut recomputed = BTreeMap::<RowKey, RowSlot>::new();
            for entry in state
                .closure_contributions
                .values()
                .flat_map(|contribution| contribution.iter())
            {
                let key = (entry.0, entry.1);
                match recomputed.get_mut(&key) {
                    Some(slot) if slot.entry == *entry => {
                        slot.refcount += 1;
                    }
                    Some(slot) if entry.2 > slot.entry.2 => {
                        slot.entry = *entry;
                        slot.refcount += 1;
                    }
                    Some(slot) => {
                        slot.refcount += 1;
                    }
                    None => {
                        recomputed.insert(
                            key,
                            RowSlot {
                                entry: *entry,
                                refcount: 1,
                            },
                        );
                    }
                }
            }
            debug_assert_eq!(
                state.row_index, recomputed,
                "peer subscription {subscription:?} row_index diverged from closure contributions"
            );
        }
        // Diagnostic-only: gate the duplicate-version scan to debug builds.
        #[cfg(debug_assertions)]
        {
            let result_set = state.row_result_set();
            if let Some((table, row_uuid, first, second)) = duplicate_row_result_set(&result_set) {
                debug_assert!(
                    first == second,
                    "peer subscription {subscription:?} has multiple content versions for {table}.{row_uuid:?}: {first:?} and {second:?}"
                );
            }
        }
    }
}

#[cfg(debug_assertions)]
fn duplicate_row_result_set(
    result_set: &BTreeSet<ResultRowEntry>,
) -> Option<(String, RowUuid, TxId, TxId)> {
    let mut rows = BTreeMap::new();
    for (table, row_uuid, tx_id) in result_set {
        if let Some(first) = rows.insert((*table, *row_uuid), *tx_id) {
            return Some((table.to_string(), *row_uuid, first, *tx_id));
        }
    }
    None
}

fn previous_tx_ids<'a>(rows: impl IntoIterator<Item = &'a ResultRowEntry>) -> BTreeSet<TxId> {
    rows.into_iter().map(|(_, _, tx_id)| *tx_id).collect()
}

fn bundle_contains_complete_tx_payload(bundle: &VersionBundle) -> bool {
    usize::try_from(bundle.tx.n_total_writes).ok() == Some(bundle.versions.len())
}

fn filter_view_update_to_result_table(update: &mut SyncMessage, table: &str) {
    let SyncMessage::ViewUpdate {
        version_bundles,
        peer_payload_inventory,
        result_row_adds,
        result_row_removes,
        ..
    } = update
    else {
        return;
    };
    result_row_adds.retain(|(entry_table, _, _)| entry_table.as_str() == table);
    result_row_removes.retain(|(entry_table, _, _)| entry_table.as_str() == table);
    let retained_tx_ids = result_row_adds
        .iter()
        .chain(result_row_removes.iter())
        .map(|(_, _, tx_id)| *tx_id)
        .collect::<BTreeSet<_>>();
    version_bundles.retain(|bundle| retained_tx_ids.contains(&bundle.tx.tx_id));
    peer_payload_inventory
        .complete_tx_payloads
        .retain(|tx_id| retained_tx_ids.contains(tx_id));
}

fn drain_initial_subscription_snapshot(receiver: &Subscription) {
    while receiver.try_recv().is_ok() {}
}

fn view_update_reset_result_set(update: &mut SyncMessage) {
    let SyncMessage::ViewUpdate {
        reset_result_set, ..
    } = update
    else {
        return;
    };
    *reset_result_set = true;
}

fn view_update_result_add_count(update: &SyncMessage) -> usize {
    let SyncMessage::ViewUpdate {
        result_row_adds, ..
    } = update
    else {
        return 0;
    };
    result_row_adds.len()
}

fn is_degenerate_whole_table(shape: &ValidatedQuery, binding: &Binding) -> bool {
    let query = shape.query();
    query.filters.is_empty()
        && query.joins.is_empty()
        && query.includes.is_empty()
        && binding.values().is_empty()
}

fn merge_rehydrate_diff(update: &mut SyncMessage, diff: SyncMessage) {
    let SyncMessage::ViewUpdate {
        version_bundles,
        peer_payload_inventory,
        result_row_removes,
        ..
    } = diff
    else {
        return;
    };
    let SyncMessage::ViewUpdate {
        version_bundles: target_versions,
        peer_payload_inventory:
            PeerPayloadInventory {
                complete_tx_payloads: target_refs,
            },
        result_row_removes: target_removes,
        ..
    } = update
    else {
        return;
    };
    let mut seen_versions = target_versions
        .iter()
        .map(|bundle| bundle.tx.tx_id)
        .collect::<BTreeSet<_>>();
    target_versions.extend(
        version_bundles
            .into_iter()
            .filter(|bundle| seen_versions.insert(bundle.tx.tx_id)),
    );
    let mut seen_refs = target_refs.iter().copied().collect::<BTreeSet<_>>();
    target_refs.extend(
        peer_payload_inventory
            .complete_tx_payloads
            .into_iter()
            .filter(|tx_id| seen_refs.insert(*tx_id)),
    );
    let mut seen_removes = target_removes.iter().cloned().collect::<BTreeSet<_>>();
    target_removes.extend(
        result_row_removes
            .into_iter()
            .filter(|entry| seen_removes.insert(*entry)),
    );
}

/// Deterministic counters for peer-dedup assertions and future M2 benchmarks.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PeerMetrics {
    /// View updates emitted.
    pub view_updates_out: u64,
    /// Version bundles emitted.
    pub version_bundles_out: u64,
    /// Complete transaction payload bundles emitted after already shipping the same complete tx.
    pub duplicate_version_bundles_out: u64,
    /// Complete transaction references emitted.
    pub complete_tx_payload_refs_out: u64,
    /// Result-set additions emitted.
    pub result_adds_out: u64,
    /// Result-set removals emitted.
    pub result_removes_out: u64,
    /// Full query diffs used to repair gated exclusive transaction siblings.
    pub full_diff_recomputes_out: u64,
    /// Maintained subscription view counters and latest index footprint.
    pub maintained_subscription_view: Box<MaintainedSubscriptionViewMetrics>,
}

/// Latest maintained subscription view index sizes observed for one peer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MaintainedSubscriptionViewMetricsFootprint {
    /// Active result-current rows in the maintained index.
    pub result_rows: usize,
    /// Active readable version identities retained by full record identity.
    pub version_identities: usize,
    /// Entries reachable through the version-by-transaction index.
    pub version_tx_entries: usize,
    /// Active replacement winner entries across content and deletion maps.
    pub replacement_entries: usize,
}

/// Observable maintained subscription view metrics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MaintainedSubscriptionViewMetrics {
    /// Maintained subscription view updates served by the incremental path.
    pub hits_out: u64,
    /// Maintained subscription view attempts served by the full-recompute path.
    pub full_recomputes_out: u64,
    /// Maintained subscription view skips for query shapes rejected by capability checks.
    pub unsupported_skips_out: u64,
    /// Non-empty Groove delta batches drained by maintained subscription views.
    pub delta_batches_in: u64,
    /// Latest maintained subscription view index sizes observed for this peer.
    pub footprint: MaintainedSubscriptionViewMetricsFootprint,
}

impl From<MaintainedSubscriptionViewIndexFootprint> for MaintainedSubscriptionViewMetricsFootprint {
    fn from(footprint: MaintainedSubscriptionViewIndexFootprint) -> Self {
        Self {
            result_rows: footprint.result_rows,
            version_identities: footprint.version_identities,
            version_tx_entries: footprint.version_tx_entries,
            replacement_entries: footprint.replacement_entries,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;

    use crate::ids::{NodeUuid, RowUuid};
    use crate::node::MergeableCommit;
    use crate::protocol::SyncMessage;
    use crate::query::{OrderDirection, Query, claim, col, eq, gt, is_null, lit, ne, not, param};
    use crate::schema::{JazzSchema, Policy, TableSchema};
    use crate::time::GlobalSeq;
    use crate::tx::DeletionEvent;
    use crate::tx::{DurabilityTier, Fate, TxKind};
    use groove::ivm::{RecordDelta, RecordDeltas};
    use groove::records::Value;
    use groove::schema::{ColumnSchema, ColumnType};
    use groove::storage::RocksDbStorage;

    fn node(byte: u8) -> NodeUuid {
        NodeUuid::from_bytes([byte; 16])
    }

    fn row(byte: u8) -> RowUuid {
        RowUuid::from_bytes([byte; 16])
    }

    fn row_from_u64(value: u64) -> RowUuid {
        let mut bytes = [0; 16];
        bytes[..8].copy_from_slice(&value.to_be_bytes());
        RowUuid::from_bytes(bytes)
    }

    fn current_row_pair(row: crate::node::CurrentRow) -> (RowUuid, BTreeMap<String, Value>) {
        (row.row_uuid(), row.test_cells_by_descriptor())
    }

    fn title_cells(title: impl Into<String>) -> BTreeMap<String, Value> {
        BTreeMap::from([("title".to_owned(), Value::String(title.into()))])
    }

    fn maybe_title_cells(title: Option<&str>) -> BTreeMap<String, Value> {
        BTreeMap::from([
            (
                "anchor".to_owned(),
                Value::String(format!("anchor-{}", title.unwrap_or("null"))),
            ),
            (
                "maybe_title".to_owned(),
                Value::Nullable(title.map(|title| Box::new(Value::String(title.to_owned())))),
            ),
        ])
    }

    fn priority_cells(title: impl Into<String>, priority: u64) -> BTreeMap<String, Value> {
        BTreeMap::from([
            ("title".to_owned(), Value::String(title.into())),
            ("priority".to_owned(), Value::U64(priority)),
        ])
    }

    fn access_policy_schema() -> JazzSchema {
        JazzSchema::new([
            TableSchema::new(
                "docs",
                [
                    ColumnSchema::new("title", ColumnType::String),
                    ColumnSchema::new("project", ColumnType::Uuid),
                ],
            )
            .with_read_policy(Policy::shape(Query::from("docs").join_via(
                "docAccess",
                "doc",
                [eq(col("userID"), claim("sub"))],
            ))),
            TableSchema::new(
                "docAccess",
                [
                    ColumnSchema::new("doc", ColumnType::Uuid),
                    ColumnSchema::new("userID", ColumnType::Uuid),
                ],
            )
            .with_reference("doc", "docs"),
        ])
    }

    fn doc_cells(title: impl Into<String>, project: RowUuid) -> BTreeMap<String, Value> {
        BTreeMap::from([
            ("title".to_owned(), Value::String(title.into())),
            ("project".to_owned(), Value::Uuid(project.0)),
        ])
    }

    fn access_cells(doc: RowUuid, user: AuthorId) -> BTreeMap<String, Value> {
        BTreeMap::from([
            ("doc".to_owned(), Value::Uuid(doc.0)),
            ("userID".to_owned(), Value::Uuid(user.0)),
        ])
    }

    fn schema() -> JazzSchema {
        JazzSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new("title", ColumnType::String)],
        )])
    }

    fn nullable_title_schema() -> JazzSchema {
        JazzSchema::new([TableSchema::new(
            "todos",
            [
                ColumnSchema::new("anchor", ColumnType::String),
                ColumnSchema::new("maybe_title", ColumnType::String.nullable()),
            ],
        )])
    }

    fn priority_schema() -> JazzSchema {
        JazzSchema::new([TableSchema::new(
            "todos",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("priority", ColumnType::U64),
            ],
        )])
    }

    fn open_node_with_schema(
        node_uuid: NodeUuid,
        schema: JazzSchema,
    ) -> (tempfile::TempDir, NodeState<RocksDbStorage>) {
        let temp_dir = tempfile::tempdir().unwrap();
        let cfs = schema.column_families();
        let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
        let storage = RocksDbStorage::open(temp_dir.path(), &refs).unwrap();
        let node = NodeState::new(node_uuid, schema, storage).unwrap();
        (temp_dir, node)
    }

    fn open_node_with_uuid(node_uuid: NodeUuid) -> (tempfile::TempDir, NodeState<RocksDbStorage>) {
        let schema = schema();
        open_node_with_schema(node_uuid, schema)
    }

    fn accept_global(core: &mut NodeState<RocksDbStorage>, tx_id: TxId, seq: u64) {
        core.apply_fate_update(
            tx_id,
            Fate::Accepted,
            Some(GlobalSeq(seq)),
            Some(DurabilityTier::Global),
        )
        .unwrap();
    }

    fn title_shape_binding(title: &str) -> (ValidatedQuery, Binding) {
        let shape = Query::from("todos")
            .filter(eq(col("title"), param("title")))
            .validate(&schema())
            .unwrap();
        let binding = shape
            .bind(BTreeMap::from([(
                "title".to_owned(),
                Value::String(title.to_owned()),
            )]))
            .unwrap();
        (shape, binding)
    }

    fn title_contains_shape_binding(needle: &str) -> (ValidatedQuery, Binding) {
        let shape = Query::from("todos")
            .filter(crate::query::contains(
                col("title"),
                crate::query::lit(needle),
            ))
            .validate(&schema())
            .unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        (shape, binding)
    }

    fn title_contains_param_shape_binding(needle: &str) -> (ValidatedQuery, Binding) {
        let shape = Query::from("todos")
            .filter(crate::query::contains(col("title"), param("needle")))
            .validate(&schema())
            .unwrap();
        let binding = shape
            .bind(BTreeMap::from([(
                "needle".to_owned(),
                Value::String(needle.to_owned()),
            )]))
            .unwrap();
        (shape, binding)
    }

    fn title_not_param_shape_binding(title: &str) -> (ValidatedQuery, Binding) {
        let shape = Query::from("todos")
            .filter(ne(col("title"), param("title")))
            .validate(&schema())
            .unwrap();
        let binding = shape
            .bind(BTreeMap::from([(
                "title".to_owned(),
                Value::String(title.to_owned()),
            )]))
            .unwrap();
        (shape, binding)
    }

    fn title_after_literal_shape_binding(title: &str) -> (ValidatedQuery, Binding) {
        let shape = Query::from("todos")
            .filter(gt(col("title"), lit(title)))
            .validate(&schema())
            .unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        (shape, binding)
    }

    fn title_before_reversed_literal_shape_binding(title: &str) -> (ValidatedQuery, Binding) {
        let shape = Query::from("todos")
            .filter(gt(lit(title), col("title")))
            .validate(&schema())
            .unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        (shape, binding)
    }

    fn title_any_literal_shape_binding(left: &str, right: &str) -> (ValidatedQuery, Binding) {
        let shape = Query::from("todos")
            .filter(crate::query::any_of([
                eq(col("title"), lit(left)),
                eq(col("title"), lit(right)),
            ]))
            .validate(&schema())
            .unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        (shape, binding)
    }

    fn title_in_literal_shape_binding(
        values: impl IntoIterator<Item = &'static str>,
    ) -> (ValidatedQuery, Binding) {
        let shape = Query::from("todos")
            .filter(crate::query::in_list(
                col("title"),
                values.into_iter().map(lit),
            ))
            .validate(&schema())
            .unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        (shape, binding)
    }

    fn nullable_title_shape_binding(non_null: bool) -> (ValidatedQuery, Binding) {
        let predicate = is_null(col("maybe_title"));
        let predicate = if non_null { not(predicate) } else { predicate };
        let shape = Query::from("todos")
            .filter(predicate)
            .validate(&nullable_title_schema())
            .unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        (shape, binding)
    }

    fn subscription_key(shape: &ValidatedQuery, binding: &Binding) -> SubscriptionKey {
        SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
        }
    }

    fn row_result_set(
        peer: &PeerState,
        subscription: SubscriptionKey,
    ) -> Option<BTreeSet<ResultRowEntry>> {
        peer.subscriptions
            .get(&subscription)
            .map(PeerSubscriptionState::row_result_set)
    }

    fn maintained_subscription_id(
        peer: &PeerState,
        subscription: SubscriptionKey,
    ) -> Option<groove::ivm::SubscriptionId> {
        peer.subscriptions
            .get(&subscription)
            .and_then(|state| state.maintained_subscription_view.as_ref())
            .map(|maintained| maintained.subscription.id())
    }

    fn assert_unsupported_maintained_subscription_error(result: Result<SyncMessage, Error>) {
        match result {
            Err(Error::InvalidStoredValue(
                "maintained subscription view subscription does not support this query shape",
            )) => {}
            other => panic!("expected unsupported maintained subscription error, got {other:?}"),
        }
    }

    fn capture_view_update(update: SyncMessage) -> SyncMessage {
        let SyncMessage::ViewUpdate {
            subscription,
            reset_result_set,
            mut version_bundles,
            mut peer_payload_inventory,
            mut result_row_adds,
            mut result_row_removes,
        } = update
        else {
            panic!("expected view update");
        };
        version_bundles.sort_by_key(|bundle| bundle.tx.tx_id);
        peer_payload_inventory.complete_tx_payloads.sort();
        result_row_adds.sort();
        result_row_removes.sort();
        SyncMessage::ViewUpdate {
            subscription,
            reset_result_set,
            version_bundles,
            peer_payload_inventory,
            result_row_adds,
            result_row_removes,
        }
    }

    fn view_update_added_rows(update: SyncMessage) -> BTreeSet<RowUuid> {
        let SyncMessage::ViewUpdate {
            reset_result_set,
            result_row_adds,
            result_row_removes,
            ..
        } = update
        else {
            panic!("expected view update");
        };
        assert!(!reset_result_set);
        assert!(result_row_removes.is_empty());
        result_row_adds
            .into_iter()
            .map(|(_, row_uuid, _)| row_uuid)
            .collect()
    }

    fn assert_view_update_rows(
        update: SyncMessage,
        expected_adds: Vec<(&str, RowUuid, TxId)>,
        expected_removes: Vec<(&str, RowUuid, TxId)>,
    ) {
        let SyncMessage::ViewUpdate {
            result_row_adds,
            result_row_removes,
            ..
        } = update
        else {
            panic!("expected view update");
        };
        let mut result_row_adds = result_row_adds;
        let mut result_row_removes = result_row_removes;
        result_row_adds.sort();
        result_row_removes.sort();
        let mut expected_adds = expected_adds
            .into_iter()
            .map(|(table, row, tx)| (table.to_owned().into(), row, tx))
            .collect::<Vec<_>>();
        let mut expected_removes = expected_removes
            .into_iter()
            .map(|(table, row, tx)| (table.to_owned().into(), row, tx))
            .collect::<Vec<_>>();
        expected_adds.sort();
        expected_removes.sort();
        assert_eq!(result_row_adds, expected_adds);
        assert_eq!(result_row_removes, expected_removes);
    }

    fn query_subscription(
        peer: &PeerState,
        subscription: SubscriptionKey,
    ) -> Option<&Subscription> {
        peer.subscriptions
            .get(&subscription)
            .and_then(|state| state.query_subscription.as_ref())
    }

    #[test]
    fn maintained_subscription_view_default_rehydrate_installs_subscription() {
        let (_dir, mut core) = open_node_with_uuid(node(0x90));
        let tx_id = core
            .commit_mergeable(
                MergeableCommit::new("todos", row(0x10), 1_000).cells(title_cells("match")),
            )
            .unwrap();
        accept_global(&mut core, tx_id, 1);
        let (shape, binding) = title_shape_binding("match");
        let subscription = subscription_key(&shape, &binding);
        let mut peer = PeerState::new();

        peer.rehydrate_query(&mut core, &shape, &binding).unwrap();

        assert!(maintained_subscription_id(&peer, subscription).is_some());
    }

    #[test]
    fn maintained_subscription_view_limit_one_installs_subscription() {
        let (_dir, mut core) = open_node_with_uuid(node(0x90));
        let higher_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", row_from_u64(20), 1_000).cells(title_cells("higher")),
            )
            .unwrap();
        let lower_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", row_from_u64(10), 1_001).cells(title_cells("lower")),
            )
            .unwrap();
        accept_global(&mut core, higher_tx, 1);
        accept_global(&mut core, lower_tx, 2);
        let shape = Query::from("todos").limit(1).validate(&schema()).unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let subscription = subscription_key(&shape, &binding);
        let mut peer = PeerState::new();

        let update = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();

        assert!(maintained_subscription_id(&peer, subscription).is_some());
        assert_eq!(
            peer.maintained_subscription_view_metrics()
                .unsupported_skips_out,
            0
        );
        assert_eq!(
            peer.maintained_subscription_view_metrics()
                .full_recomputes_out,
            0
        );
        let SyncMessage::ViewUpdate {
            result_row_adds,
            result_row_removes,
            ..
        } = update
        else {
            panic!("expected view update");
        };
        assert_eq!(
            result_row_adds,
            vec![("todos".to_owned().into(), row_from_u64(10), lower_tx)]
        );
        assert!(result_row_removes.is_empty());
    }

    #[test]
    fn maintained_subscription_view_limit_one_switches_after_winner_delete_and_lower_insert() {
        let (_dir, mut core) = open_node_with_uuid(node(0x91));
        let first_row = row_from_u64(10);
        let second_row = row_from_u64(20);
        let first_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", first_row, 1_000).cells(title_cells("first")),
            )
            .unwrap();
        let second_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", second_row, 1_001).cells(title_cells("second")),
            )
            .unwrap();
        accept_global(&mut core, first_tx, 1);
        accept_global(&mut core, second_tx, 2);
        let shape = Query::from("todos").limit(1).validate(&schema()).unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let subscription = subscription_key(&shape, &binding);
        let mut peer = PeerState::new();

        peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
        assert!(maintained_subscription_id(&peer, subscription).is_some());

        let delete_first_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", first_row, 1_002).deletion(DeletionEvent::Deleted),
            )
            .unwrap();
        accept_global(&mut core, delete_first_tx, 3);
        let update = peer.query_update(&mut core, &shape, &binding).unwrap();
        let SyncMessage::ViewUpdate {
            result_row_adds,
            result_row_removes,
            ..
        } = update
        else {
            panic!("expected view update");
        };
        assert_eq!(
            result_row_removes,
            vec![("todos".to_owned().into(), first_row, first_tx)]
        );
        assert_eq!(
            result_row_adds,
            vec![("todos".to_owned().into(), second_row, second_tx)]
        );

        let new_first_row = row_from_u64(5);
        let new_first_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", new_first_row, 1_003).cells(title_cells("new first")),
            )
            .unwrap();
        accept_global(&mut core, new_first_tx, 4);
        let update = peer.query_update(&mut core, &shape, &binding).unwrap();
        let SyncMessage::ViewUpdate {
            result_row_adds,
            result_row_removes,
            ..
        } = update
        else {
            panic!("expected view update");
        };
        assert_eq!(
            result_row_removes,
            vec![("todos".to_owned().into(), second_row, second_tx)]
        );
        assert_eq!(
            result_row_adds,
            vec![("todos".to_owned().into(), new_first_row, new_first_tx)]
        );
        assert_eq!(
            peer.maintained_subscription_view_metrics()
                .full_recomputes_out,
            0
        );
        assert_eq!(peer.maintained_subscription_view_metrics().hits_out, 3);
    }

    #[test]
    fn maintained_subscription_view_order_by_asc_limit_two_initial_hydration() {
        let (_dir, mut core) = open_node_with_schema(node(0x92), priority_schema());
        let charlie_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", row_from_u64(30), 1_000)
                    .cells(priority_cells("charlie", 30)),
            )
            .unwrap();
        let alpha_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", row_from_u64(10), 1_001)
                    .cells(priority_cells("alpha", 10)),
            )
            .unwrap();
        let bravo_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", row_from_u64(20), 1_002)
                    .cells(priority_cells("bravo", 20)),
            )
            .unwrap();
        accept_global(&mut core, charlie_tx, 1);
        accept_global(&mut core, alpha_tx, 2);
        accept_global(&mut core, bravo_tx, 3);
        let shape = Query::from("todos")
            .order_by("priority", OrderDirection::Asc)
            .limit(2)
            .validate(&priority_schema())
            .unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let subscription = subscription_key(&shape, &binding);
        let mut peer = PeerState::new();

        core.maintained_view_support(&shape, &binding, AuthorId::SYSTEM)
            .unwrap();
        let update = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();

        assert!(maintained_subscription_id(&peer, subscription).is_some());
        assert_view_update_rows(
            update,
            vec![
                ("todos", row_from_u64(10), alpha_tx),
                ("todos", row_from_u64(20), bravo_tx),
            ],
            vec![],
        );
        let metrics = peer.maintained_subscription_view_metrics();
        assert_eq!(metrics.unsupported_skips_out, 0);
        assert_eq!(metrics.full_recomputes_out, 0);
    }

    #[test]
    fn maintained_subscription_view_order_by_asc_limit_two_boundary_insert_delete_updates() {
        let (_dir, mut core) = open_node_with_schema(node(0x93), priority_schema());
        let alpha = row_from_u64(10);
        let bravo = row_from_u64(20);
        let charlie = row_from_u64(30);
        let alpha_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", alpha, 1_000).cells(priority_cells("alpha", 10)),
            )
            .unwrap();
        let bravo_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", bravo, 1_001).cells(priority_cells("bravo", 20)),
            )
            .unwrap();
        let charlie_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", charlie, 1_002).cells(priority_cells("charlie", 30)),
            )
            .unwrap();
        accept_global(&mut core, alpha_tx, 1);
        accept_global(&mut core, bravo_tx, 2);
        accept_global(&mut core, charlie_tx, 3);
        let shape = Query::from("todos")
            .order_by("priority", OrderDirection::Asc)
            .limit(2)
            .validate(&priority_schema())
            .unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let mut peer = PeerState::new();

        peer.rehydrate_query(&mut core, &shape, &binding).unwrap();

        let aardvark = row_from_u64(5);
        let aardvark_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", aardvark, 1_003).cells(priority_cells("aardvark", 5)),
            )
            .unwrap();
        accept_global(&mut core, aardvark_tx, 4);
        let update = peer.query_update(&mut core, &shape, &binding).unwrap();
        assert_view_update_rows(
            update,
            vec![("todos", aardvark, aardvark_tx)],
            vec![("todos", bravo, bravo_tx)],
        );

        let delete_alpha_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", alpha, 1_004).deletion(DeletionEvent::Deleted),
            )
            .unwrap();
        accept_global(&mut core, delete_alpha_tx, 5);
        let update = peer.query_update(&mut core, &shape, &binding).unwrap();
        assert_view_update_rows(
            update,
            vec![("todos", bravo, bravo_tx)],
            vec![("todos", alpha, alpha_tx)],
        );
        assert_eq!(
            peer.maintained_subscription_view_metrics()
                .full_recomputes_out,
            0
        );
    }

    #[test]
    fn maintained_subscription_view_order_by_desc_limit_two_initial_hydration() {
        let (_dir, mut core) = open_node_with_schema(node(0x94), priority_schema());
        let alpha_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", row_from_u64(10), 1_000)
                    .cells(priority_cells("alpha", 10)),
            )
            .unwrap();
        let delta_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", row_from_u64(40), 1_001)
                    .cells(priority_cells("delta", 40)),
            )
            .unwrap();
        let charlie_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", row_from_u64(30), 1_002)
                    .cells(priority_cells("charlie", 30)),
            )
            .unwrap();
        accept_global(&mut core, alpha_tx, 1);
        accept_global(&mut core, delta_tx, 2);
        accept_global(&mut core, charlie_tx, 3);
        let shape = Query::from("todos")
            .order_by("priority", OrderDirection::Desc)
            .limit(2)
            .validate(&priority_schema())
            .unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let mut peer = PeerState::new();

        let update = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();

        assert_view_update_rows(
            update,
            vec![
                ("todos", row_from_u64(40), delta_tx),
                ("todos", row_from_u64(30), charlie_tx),
            ],
            vec![],
        );
        assert_eq!(
            peer.maintained_subscription_view_metrics()
                .unsupported_skips_out,
            0
        );
    }

    #[test]
    fn maintained_subscription_view_order_by_limit_two_ties_are_stable_by_row_uuid() {
        let (_dir, mut core) = open_node_with_schema(node(0x95), priority_schema());
        let third_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", row_from_u64(30), 1_000)
                    .cells(priority_cells("third", 7)),
            )
            .unwrap();
        let first_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", row_from_u64(10), 1_001)
                    .cells(priority_cells("first", 7)),
            )
            .unwrap();
        let second_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", row_from_u64(20), 1_002)
                    .cells(priority_cells("second", 7)),
            )
            .unwrap();
        accept_global(&mut core, third_tx, 1);
        accept_global(&mut core, first_tx, 2);
        accept_global(&mut core, second_tx, 3);
        let shape = Query::from("todos")
            .order_by("priority", OrderDirection::Asc)
            .limit(2)
            .validate(&priority_schema())
            .unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let mut peer = PeerState::new();

        let update = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();

        assert_view_update_rows(
            update,
            vec![
                ("todos", row_from_u64(10), first_tx),
                ("todos", row_from_u64(20), second_tx),
            ],
            vec![],
        );
    }

    #[test]
    fn maintained_subscription_view_order_by_offset_limit_uses_top_by_window() {
        let (_dir, mut core) = open_node_with_schema(node(0x96), priority_schema());
        let first_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", row_from_u64(10), 1_000)
                    .cells(priority_cells("first", 10)),
            )
            .unwrap();
        let second_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", row_from_u64(20), 1_001)
                    .cells(priority_cells("second", 20)),
            )
            .unwrap();
        let third_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", row_from_u64(30), 1_002)
                    .cells(priority_cells("third", 30)),
            )
            .unwrap();
        accept_global(&mut core, first_tx, 1);
        accept_global(&mut core, second_tx, 2);
        accept_global(&mut core, third_tx, 3);
        let shape = Query::from("todos")
            .order_by("priority", OrderDirection::Asc)
            .offset(1)
            .limit(1)
            .validate(&priority_schema())
            .unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let subscription = subscription_key(&shape, &binding);
        let mut peer = PeerState::new();

        let update = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();

        assert!(maintained_subscription_id(&peer, subscription).is_some());
        assert_view_update_rows(update, vec![("todos", row_from_u64(20), second_tx)], vec![]);

        let zeroth = row_from_u64(5);
        let zeroth_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", zeroth, 1_003).cells(priority_cells("zeroth", 5)),
            )
            .unwrap();
        accept_global(&mut core, zeroth_tx, 4);
        let update = peer.query_update(&mut core, &shape, &binding).unwrap();
        assert_view_update_rows(
            update,
            vec![("todos", row_from_u64(10), first_tx)],
            vec![("todos", row_from_u64(20), second_tx)],
        );
        assert_eq!(
            peer.maintained_subscription_view_metrics()
                .full_recomputes_out,
            0
        );
    }

    #[test]
    fn maintained_subscription_view_unsupported_limited_variants_error_loudly() {
        let (_dir, mut core) = open_node_with_uuid(node(0x90));
        let first_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", row(0x10), 1_000).cells(title_cells("alpha")),
            )
            .unwrap();
        let second_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", row(0x11), 1_001).cells(title_cells("beta")),
            )
            .unwrap();
        accept_global(&mut core, first_tx, 1);
        accept_global(&mut core, second_tx, 2);
        let no_order_limit = Query::from("todos").limit(2).validate(&schema()).unwrap();
        let order_without_limit = Query::from("todos")
            .order_by("title", OrderDirection::Asc)
            .validate(&schema())
            .unwrap();
        let offset_limit_one = Query::from("todos")
            .limit(1)
            .offset(1)
            .validate(&schema())
            .unwrap();
        let shapes = [no_order_limit, order_without_limit, offset_limit_one];
        let mut peer = PeerState::new();

        for shape in shapes {
            let binding = shape.bind(BTreeMap::new()).unwrap();
            let subscription = subscription_key(&shape, &binding);

            assert!(matches!(
                core.maintained_view_support(&shape, &binding, AuthorId::SYSTEM),
                Err(Error::InvalidStoredValue(
                    "maintained subscription view subscription does not support this query shape"
                ))
            ));
            assert_unsupported_maintained_subscription_error(
                peer.rehydrate_query(&mut core, &shape, &binding),
            );

            assert!(maintained_subscription_id(&peer, subscription).is_none());
        }

        let metrics = peer.maintained_subscription_view_metrics();
        assert_eq!(metrics.unsupported_skips_out, 0);
        assert_eq!(metrics.full_recomputes_out, 0);
    }

    #[test]
    fn maintained_subscription_view_unsupported_aggregate_rehydrate_errors_loudly() {
        let (_dir, mut core) = open_node_with_uuid(node(0x90));
        let first_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", row(0x10), 1_000).cells(title_cells("alpha")),
            )
            .unwrap();
        let second_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", row(0x11), 1_001).cells(title_cells("beta")),
            )
            .unwrap();
        accept_global(&mut core, first_tx, 1);
        accept_global(&mut core, second_tx, 2);
        let aggregate_shape = Query::from("todos").count().validate(&schema()).unwrap();
        let aggregate_binding = aggregate_shape.bind(BTreeMap::new()).unwrap();
        let aggregate_subscription = subscription_key(&aggregate_shape, &aggregate_binding);
        let mut peer = PeerState::new();

        assert!(matches!(
            core.maintained_view_support(&aggregate_shape, &aggregate_binding, AuthorId::SYSTEM),
            Err(Error::InvalidStoredValue(
                "maintained subscription view subscription does not support this query shape"
            ))
        ));
        assert_unsupported_maintained_subscription_error(peer.rehydrate_query(
            &mut core,
            &aggregate_shape,
            &aggregate_binding,
        ));

        let metrics = peer.maintained_subscription_view_metrics();
        assert_eq!(metrics.unsupported_skips_out, 0);
        assert_eq!(metrics.full_recomputes_out, 0);
        assert!(maintained_subscription_id(&peer, aggregate_subscription).is_none());
    }

    #[test]
    fn peer_runtime_handles_do_not_cross_node_runtime_instances() {
        let user = AuthorId::from_bytes([0xa1; 16]);
        let (_first_dir, mut first_core) =
            open_node_with_schema(node(0x90), access_policy_schema());
        let mut peer = PeerState::edge_client(user);

        peer.current_rows_update(&mut first_core, "docs").unwrap();

        let (_second_dir, mut second_core) =
            open_node_with_schema(node(0x90), access_policy_schema());

        peer.current_rows_update(&mut second_core, "docs").unwrap();
    }

    #[test]
    fn maintained_subscription_view_forget_with_node_unsubscribes_and_drops_state() {
        let (_dir, mut core) = open_node_with_uuid(node(0x91));
        let row_uuid = row(0x11);
        let tx_id = core
            .commit_mergeable(
                MergeableCommit::new("todos", row_uuid, 1_000).cells(title_cells("match")),
            )
            .unwrap();
        accept_global(&mut core, tx_id, 1);
        let (shape, binding) = title_shape_binding("match");
        let subscription = subscription_key(&shape, &binding);
        let mut peer = PeerState::new();

        peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
        let maintained_id = maintained_subscription_id(&peer, subscription)
            .expect("supported maintained-view rehydrate must install maintained subscription");

        assert!(peer.forget_subscription_with_node(&mut core, subscription));
        assert!(maintained_subscription_id(&peer, subscription).is_none());
        assert!(row_result_set(&peer, subscription).is_none());
        assert!(!core.unsubscribe_groove_subscription(maintained_id));

        let stale_tick = peer.query_update(&mut core, &shape, &binding).unwrap();
        assert_eq!(
            stale_tick,
            SyncMessage::ViewUpdate {
                subscription,
                reset_result_set: false,
                version_bundles: Vec::new(),
                peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
                result_row_adds: Vec::new(),
                result_row_removes: Vec::new(),
            }
        );
    }

    #[test]
    fn maintained_subscription_view_forget_query_binding_with_node_unsubscribes() {
        let (_dir, mut core) = open_node_with_uuid(node(0x94));
        let row_uuid = row(0x41);
        let tx_id = core
            .commit_mergeable(
                MergeableCommit::new("todos", row_uuid, 1_000).cells(title_cells("match")),
            )
            .unwrap();
        accept_global(&mut core, tx_id, 1);
        let (shape, binding) = title_shape_binding("match");
        let subscription = subscription_key(&shape, &binding);
        let mut peer = PeerState::new();

        peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
        let maintained_id = maintained_subscription_id(&peer, subscription).unwrap();

        assert!(peer.forget_query_binding_with_node(&mut core, &shape, &binding));
        assert!(maintained_subscription_id(&peer, subscription).is_none());
        assert!(!core.unsubscribe_groove_subscription(maintained_id));
    }

    #[test]
    fn maintained_subscription_view_hit_metrics_and_footprint_update() {
        let (_dir, mut core) = open_node_with_uuid(node(0x95));
        let tx_id = core
            .commit_mergeable(
                MergeableCommit::new("todos", row(0x51), 1_000).cells(title_cells("match")),
            )
            .unwrap();
        accept_global(&mut core, tx_id, 1);
        let (shape, binding) = title_shape_binding("match");
        let mut peer = PeerState::new();

        peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
        let metrics = peer.maintained_subscription_view_metrics();
        assert_eq!(metrics.hits_out, 1);
        assert_eq!(metrics.full_recomputes_out, 0);
        assert_eq!(metrics.footprint.result_rows, 1);
        assert!(metrics.footprint.version_identities >= 1);
        assert!(metrics.footprint.version_tx_entries >= 1);
    }

    #[test]
    fn maintained_subscription_view_contains_literal_stays_maintained() {
        let (_dir, mut core) = open_node_with_uuid(node(0x9a));
        let initial = core
            .commit_mergeable(
                MergeableCommit::new("todos", row(0x5a), 1_000).cells(title_cells("api docs")),
            )
            .unwrap();
        accept_global(&mut core, initial, 1);
        let excluded = core
            .commit_mergeable(
                MergeableCommit::new("todos", row(0x5b), 1_001).cells(title_cells("notes")),
            )
            .unwrap();
        accept_global(&mut core, excluded, 2);
        let (shape, binding) = title_contains_shape_binding("api");
        let subscription = subscription_key(&shape, &binding);
        let mut peer = PeerState::new();

        peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
        assert!(maintained_subscription_id(&peer, subscription).is_some());
        assert_eq!(
            peer.maintained_subscription_view_metrics()
                .full_recomputes_out,
            0
        );

        let added = core
            .commit_mergeable(
                MergeableCommit::new("todos", row(0x5c), 1_002).cells(title_cells("api reference")),
            )
            .unwrap();
        accept_global(&mut core, added, 3);

        let update = peer.query_update(&mut core, &shape, &binding).unwrap();
        let SyncMessage::ViewUpdate {
            result_row_adds,
            result_row_removes,
            ..
        } = update
        else {
            panic!("expected view update");
        };
        assert_eq!(
            result_row_adds,
            vec![("todos".to_owned().into(), row(0x5c), added)]
        );
        assert!(result_row_removes.is_empty());
        assert_eq!(
            peer.maintained_subscription_view_metrics()
                .full_recomputes_out,
            0
        );
        assert_eq!(peer.maintained_subscription_view_metrics().hits_out, 2);
    }

    #[test]
    fn maintained_subscription_view_contains_param_stays_maintained() {
        let (_dir, mut core) = open_node_with_uuid(node(0x9b));
        let initial = core
            .commit_mergeable(
                MergeableCommit::new("todos", row(0x6a), 1_000).cells(title_cells("api docs")),
            )
            .unwrap();
        accept_global(&mut core, initial, 1);
        let excluded = core
            .commit_mergeable(
                MergeableCommit::new("todos", row(0x6b), 1_001).cells(title_cells("notes")),
            )
            .unwrap();
        accept_global(&mut core, excluded, 2);
        let (shape, binding) = title_contains_param_shape_binding("api");
        let subscription = subscription_key(&shape, &binding);
        let mut peer = PeerState::new();

        peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
        assert!(maintained_subscription_id(&peer, subscription).is_some());
        assert_eq!(
            peer.maintained_subscription_view_metrics()
                .full_recomputes_out,
            0
        );

        let added = core
            .commit_mergeable(
                MergeableCommit::new("todos", row(0x6c), 1_002).cells(title_cells("api reference")),
            )
            .unwrap();
        accept_global(&mut core, added, 3);

        let update = peer.query_update(&mut core, &shape, &binding).unwrap();
        let SyncMessage::ViewUpdate {
            result_row_adds,
            result_row_removes,
            ..
        } = update
        else {
            panic!("expected view update");
        };
        assert_eq!(
            result_row_adds,
            vec![("todos".to_owned().into(), row(0x6c), added)]
        );
        assert!(result_row_removes.is_empty());
        assert_eq!(
            peer.maintained_subscription_view_metrics()
                .full_recomputes_out,
            0
        );
        assert_eq!(peer.maintained_subscription_view_metrics().hits_out, 2);
    }

    #[test]
    fn maintained_subscription_view_ne_param_stays_maintained() {
        let (_dir, mut core) = open_node_with_uuid(node(0x9c));
        let initial = core
            .commit_mergeable(
                MergeableCommit::new("todos", row(0x7a), 1_000).cells(title_cells("ship it")),
            )
            .unwrap();
        accept_global(&mut core, initial, 1);
        let excluded = core
            .commit_mergeable(
                MergeableCommit::new("todos", row(0x7b), 1_001).cells(title_cells("skip")),
            )
            .unwrap();
        accept_global(&mut core, excluded, 2);
        let (shape, binding) = title_not_param_shape_binding("skip");
        let subscription = subscription_key(&shape, &binding);
        let mut peer = PeerState::new();

        peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
        assert!(maintained_subscription_id(&peer, subscription).is_some());
        assert_eq!(
            peer.maintained_subscription_view_metrics()
                .full_recomputes_out,
            0
        );

        let added = core
            .commit_mergeable(
                MergeableCommit::new("todos", row(0x7c), 1_002).cells(title_cells("done")),
            )
            .unwrap();
        accept_global(&mut core, added, 3);
        let still_excluded = core
            .commit_mergeable(
                MergeableCommit::new("todos", row(0x7d), 1_003).cells(title_cells("skip")),
            )
            .unwrap();
        accept_global(&mut core, still_excluded, 4);

        let update = peer.query_update(&mut core, &shape, &binding).unwrap();
        let SyncMessage::ViewUpdate {
            result_row_adds,
            result_row_removes,
            ..
        } = update
        else {
            panic!("expected view update");
        };
        assert_eq!(
            result_row_adds,
            vec![("todos".to_owned().into(), row(0x7c), added)]
        );
        assert!(result_row_removes.is_empty());
        assert_eq!(
            peer.maintained_subscription_view_metrics()
                .full_recomputes_out,
            0
        );
        assert_eq!(peer.maintained_subscription_view_metrics().hits_out, 2);
    }

    #[test]
    fn maintained_subscription_view_range_literal_stays_maintained() {
        let (_dir, mut core) = open_node_with_uuid(node(0xa1));
        let initial = core
            .commit_mergeable(
                MergeableCommit::new("todos", row(0x81), 1_000).cells(title_cells("omega")),
            )
            .unwrap();
        accept_global(&mut core, initial, 1);
        let excluded = core
            .commit_mergeable(
                MergeableCommit::new("todos", row(0x82), 1_001).cells(title_cells("alpha")),
            )
            .unwrap();
        accept_global(&mut core, excluded, 2);
        let (shape, binding) = title_after_literal_shape_binding("m");
        let subscription = subscription_key(&shape, &binding);
        let mut peer = PeerState::new();

        peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
        assert!(maintained_subscription_id(&peer, subscription).is_some());
        assert_eq!(
            peer.maintained_subscription_view_metrics()
                .full_recomputes_out,
            0
        );

        let added = core
            .commit_mergeable(
                MergeableCommit::new("todos", row(0x83), 1_002).cells(title_cells("zeta")),
            )
            .unwrap();
        accept_global(&mut core, added, 3);
        let still_excluded = core
            .commit_mergeable(
                MergeableCommit::new("todos", row(0x84), 1_003).cells(title_cells("beta")),
            )
            .unwrap();
        accept_global(&mut core, still_excluded, 4);

        let update = peer.query_update(&mut core, &shape, &binding).unwrap();
        assert_eq!(view_update_added_rows(update), BTreeSet::from([row(0x83)]));
        assert_eq!(
            peer.maintained_subscription_view_metrics()
                .full_recomputes_out,
            0
        );
        assert_eq!(peer.maintained_subscription_view_metrics().hits_out, 2);
    }

    #[test]
    fn maintained_subscription_view_reversed_range_literal_stays_maintained() {
        let (_dir, mut core) = open_node_with_uuid(node(0xa2));
        let initial = core
            .commit_mergeable(
                MergeableCommit::new("todos", row(0x85), 1_000).cells(title_cells("alpha")),
            )
            .unwrap();
        accept_global(&mut core, initial, 1);
        let excluded = core
            .commit_mergeable(
                MergeableCommit::new("todos", row(0x86), 1_001).cells(title_cells("omega")),
            )
            .unwrap();
        accept_global(&mut core, excluded, 2);
        let (shape, binding) = title_before_reversed_literal_shape_binding("m");
        let subscription = subscription_key(&shape, &binding);
        let mut peer = PeerState::new();

        peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
        assert!(maintained_subscription_id(&peer, subscription).is_some());

        let added = core
            .commit_mergeable(
                MergeableCommit::new("todos", row(0x87), 1_002).cells(title_cells("beta")),
            )
            .unwrap();
        accept_global(&mut core, added, 3);
        let still_excluded = core
            .commit_mergeable(
                MergeableCommit::new("todos", row(0x88), 1_003).cells(title_cells("zeta")),
            )
            .unwrap();
        accept_global(&mut core, still_excluded, 4);

        let update = peer.query_update(&mut core, &shape, &binding).unwrap();
        assert_eq!(view_update_added_rows(update), BTreeSet::from([row(0x87)]));
        assert_eq!(
            peer.maintained_subscription_view_metrics()
                .full_recomputes_out,
            0
        );
        assert_eq!(peer.maintained_subscription_view_metrics().hits_out, 2);
    }

    #[test]
    fn maintained_subscription_view_any_literal_stays_maintained() {
        let (_dir, mut core) = open_node_with_uuid(node(0xa4));
        let initial = core
            .commit_mergeable(
                MergeableCommit::new("todos", row(0x89), 1_000).cells(title_cells("alpha")),
            )
            .unwrap();
        accept_global(&mut core, initial, 1);
        let (shape, binding) = title_any_literal_shape_binding("alpha", "beta");
        let subscription = subscription_key(&shape, &binding);
        let mut peer = PeerState::new();

        peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
        assert!(maintained_subscription_id(&peer, subscription).is_some());

        let added = core
            .commit_mergeable(
                MergeableCommit::new("todos", row(0x8a), 1_001).cells(title_cells("beta")),
            )
            .unwrap();
        accept_global(&mut core, added, 2);

        let update = peer.query_update(&mut core, &shape, &binding).unwrap();
        assert_eq!(view_update_added_rows(update), BTreeSet::from([row(0x8a)]));
        assert_eq!(
            peer.maintained_subscription_view_metrics()
                .full_recomputes_out,
            0
        );
        assert_eq!(peer.maintained_subscription_view_metrics().hits_out, 2);
    }

    #[test]
    fn maintained_subscription_view_in_literal_stays_maintained() {
        let (_dir, mut core) = open_node_with_uuid(node(0xa5));
        let initial = core
            .commit_mergeable(
                MergeableCommit::new("todos", row(0x8b), 1_000).cells(title_cells("alpha")),
            )
            .unwrap();
        accept_global(&mut core, initial, 1);
        let (shape, binding) = title_in_literal_shape_binding(["alpha", "beta"]);
        let subscription = subscription_key(&shape, &binding);
        let mut peer = PeerState::new();

        peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
        assert!(maintained_subscription_id(&peer, subscription).is_some());

        let added = core
            .commit_mergeable(
                MergeableCommit::new("todos", row(0x8c), 1_001).cells(title_cells("beta")),
            )
            .unwrap();
        accept_global(&mut core, added, 2);

        let update = peer.query_update(&mut core, &shape, &binding).unwrap();
        assert_eq!(view_update_added_rows(update), BTreeSet::from([row(0x8c)]));
        assert_eq!(
            peer.maintained_subscription_view_metrics()
                .full_recomputes_out,
            0
        );
        assert_eq!(peer.maintained_subscription_view_metrics().hits_out, 2);
    }

    #[test]
    fn maintained_subscription_view_empty_in_and_any_are_false() {
        let (_dir, mut core) = open_node_with_uuid(node(0xa6));
        let existing = core
            .commit_mergeable(
                MergeableCommit::new("todos", row(0x8d), 1_000).cells(title_cells("alpha")),
            )
            .unwrap();
        accept_global(&mut core, existing, 1);
        let empty_in = title_in_literal_shape_binding([]).0;
        let empty_any = Query::from("todos")
            .filter(crate::query::any_of([]))
            .validate(&schema())
            .unwrap();
        let mut peer = PeerState::new();

        for shape in [empty_in, empty_any] {
            let binding = shape.bind(BTreeMap::new()).unwrap();
            let subscription = subscription_key(&shape, &binding);
            peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
            assert!(maintained_subscription_id(&peer, subscription).is_some());
            assert!(row_result_set(&peer, subscription).unwrap().is_empty());
        }
    }

    #[test]
    fn maintained_subscription_view_any_with_param_is_unsupported() {
        let (_dir, mut core) = open_node_with_uuid(node(0xa7));
        let shape = Query::from("todos")
            .filter(crate::query::any_of([
                eq(col("title"), lit("alpha")),
                eq(col("title"), param("title")),
            ]))
            .validate(&schema())
            .unwrap();
        let binding = shape
            .bind(BTreeMap::from([(
                "title".to_owned(),
                Value::String("beta".to_owned()),
            )]))
            .unwrap();
        let subscription = subscription_key(&shape, &binding);
        let mut peer = PeerState::new();

        assert!(matches!(
            core.maintained_view_support(&shape, &binding, AuthorId::SYSTEM),
            Err(Error::InvalidStoredValue(
                "maintained subscription view subscription does not support this query shape"
            ))
        ));
        assert!(matches!(
            peer.rehydrate_query(&mut core, &shape, &binding),
            Err(Error::InvalidStoredValue(
                "unsupported query predicate shape"
            ))
        ));
        assert!(maintained_subscription_id(&peer, subscription).is_none());
    }

    #[test]
    fn maintained_subscription_view_null_predicates_stay_maintained() {
        for (case, non_null) in [(0xa3, false), (0xa4, true)] {
            let (_dir, mut core) = open_node_with_schema(node(case), nullable_title_schema());
            let initial = core
                .commit_mergeable(MergeableCommit::new("todos", row(case), 1_000).cells(
                    maybe_title_cells(if non_null { Some("present") } else { None }),
                ))
                .unwrap();
            accept_global(&mut core, initial, 1);
            let excluded = core
                .commit_mergeable(MergeableCommit::new("todos", row(case + 1), 1_001).cells(
                    maybe_title_cells(if non_null { None } else { Some("present") }),
                ))
                .unwrap();
            accept_global(&mut core, excluded, 2);
            let (shape, binding) = nullable_title_shape_binding(non_null);
            let subscription = subscription_key(&shape, &binding);
            let mut peer = PeerState::new();

            peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
            assert!(maintained_subscription_id(&peer, subscription).is_some());

            let added_row = row(case + 2);
            let added = core
                .commit_mergeable(
                    MergeableCommit::new("todos", added_row, 1_002)
                        .cells(maybe_title_cells(if non_null { Some("new") } else { None })),
                )
                .unwrap();
            accept_global(&mut core, added, 3);
            if !non_null {
                let still_excluded = core
                    .commit_mergeable(
                        MergeableCommit::new("todos", row(case + 3), 1_003)
                            .cells(maybe_title_cells(Some("new"))),
                    )
                    .unwrap();
                accept_global(&mut core, still_excluded, 4);
            }

            let update = peer.query_update(&mut core, &shape, &binding).unwrap();
            assert_eq!(view_update_added_rows(update), BTreeSet::from([added_row]));
            assert_eq!(
                peer.maintained_subscription_view_metrics()
                    .full_recomputes_out,
                0
            );
            assert_eq!(peer.maintained_subscription_view_metrics().hits_out, 2);
        }
    }

    #[test]
    fn maintained_subscription_view_exclusive_delta_stays_maintained() {
        let (_dir, mut core) = open_node_with_uuid(node(0x96));
        let (shape, binding) = title_shape_binding("match");
        let mut peer = PeerState::new();

        peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
        let tx = core.open_exclusive().unwrap();
        core.tx_write(tx, "todos", row(0x61), title_cells("match"), None)
            .unwrap();
        let (tx_id, _unit) = core.commit_exclusive(tx, AuthorId::SYSTEM, 1_000).unwrap();
        accept_global(&mut core, tx_id, 1);

        let update = peer.query_update(&mut core, &shape, &binding).unwrap();
        let SyncMessage::ViewUpdate {
            version_bundles,
            result_row_adds,
            result_row_removes,
            ..
        } = update
        else {
            panic!("expected view update");
        };
        assert_eq!(
            result_row_adds,
            vec![("todos".to_owned().into(), row(0x61), tx_id)]
        );
        assert!(result_row_removes.is_empty());
        assert_eq!(version_bundles.len(), 1);
        assert_eq!(version_bundles[0].tx.tx_id, tx_id);
        assert_eq!(version_bundles[0].tx.kind, TxKind::Exclusive);
        assert_eq!(
            peer.maintained_subscription_view_metrics()
                .full_recomputes_out,
            0
        );
        assert_eq!(peer.maintained_subscription_view_metrics().hits_out, 2);
    }

    #[test]
    fn maintained_subscription_view_exclusive_delta_ships_view_scoped_partial_bundle() {
        let (_dir, mut core) = open_node_with_uuid(node(0x97));
        let (shape, binding) = title_shape_binding("match");
        let mut peer = PeerState::new();

        peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
        let tx = core.open_exclusive().unwrap();
        core.tx_write(tx, "todos", row(0x71), title_cells("match"), None)
            .unwrap();
        core.tx_write(tx, "todos", row(0x72), title_cells("other"), None)
            .unwrap();
        let (tx_id, _unit) = core.commit_exclusive(tx, AuthorId::SYSTEM, 1_000).unwrap();
        accept_global(&mut core, tx_id, 1);

        let update = peer.query_update(&mut core, &shape, &binding).unwrap();
        let SyncMessage::ViewUpdate {
            version_bundles,
            peer_payload_inventory:
                crate::protocol::PeerPayloadInventory {
                    complete_tx_payloads: complete_tx_payload_refs,
                },
            result_row_adds,
            result_row_removes,
            ..
        } = update
        else {
            panic!("expected view update");
        };
        assert_eq!(
            result_row_adds,
            vec![("todos".to_owned().into(), row(0x71), tx_id)]
        );
        assert!(result_row_removes.is_empty());
        assert!(complete_tx_payload_refs.is_empty());
        assert_eq!(version_bundles.len(), 1);
        assert_eq!(version_bundles[0].tx.tx_id, tx_id);
        assert_eq!(version_bundles[0].tx.kind, TxKind::Exclusive);
        assert!(version_bundles[0].tx.n_total_writes > version_bundles[0].versions.len() as u32);
        assert_eq!(version_bundles[0].versions.len(), 1);
        assert_eq!(version_bundles[0].versions[0].row_uuid(), row(0x71));
        assert!(peer.shipped_complete_tx_payloads().is_empty());
        assert_eq!(
            peer.maintained_subscription_view_metrics()
                .full_recomputes_out,
            0
        );
        assert_eq!(peer.metrics.full_diff_recomputes_out, 0);
    }

    #[test]
    fn maintained_subscription_view_policy_view_exclusive_delta_ships_identity_scoped_partial_bundle()
     {
        let schema = access_policy_schema();
        let (_dir, mut core) = open_node_with_schema(node(0x98), schema);
        let user_a = AuthorId::from_bytes([0xa1; 16]);
        let user_b = AuthorId::from_bytes([0xb2; 16]);
        let doc_a = row(0x81);
        let doc_b = row(0x82);
        let project = row(0x83);

        let tx = core.open_exclusive().unwrap();
        core.tx_write(tx, "docs", doc_a, doc_cells("a", project), None)
            .unwrap();
        core.tx_write(tx, "docs", doc_b, doc_cells("b", project), None)
            .unwrap();
        let (docs_tx, _unit) = core.commit_exclusive(tx, AuthorId::SYSTEM, 10).unwrap();
        accept_global(&mut core, docs_tx, 1);
        let grant_a = core
            .commit_mergeable(
                MergeableCommit::new("docAccess", row(0x84), 11).cells(access_cells(doc_a, user_a)),
            )
            .unwrap();
        accept_global(&mut core, grant_a, 2);
        let grant_b = core
            .commit_mergeable(
                MergeableCommit::new("docAccess", row(0x85), 12).cells(access_cells(doc_b, user_b)),
            )
            .unwrap();
        accept_global(&mut core, grant_b, 3);

        let mut peer = PeerState::for_author(user_a);
        let update = peer.current_rows_update(&mut core, "docs").unwrap();
        let SyncMessage::ViewUpdate {
            version_bundles,
            peer_payload_inventory:
                crate::protocol::PeerPayloadInventory {
                    complete_tx_payloads: complete_tx_payload_refs,
                },
            result_row_adds,
            result_row_removes,
            ..
        } = update
        else {
            panic!("expected view update");
        };
        assert_eq!(
            result_row_adds,
            vec![("docs".to_owned().into(), doc_a, docs_tx)]
        );
        assert!(result_row_removes.is_empty());
        assert!(complete_tx_payload_refs.is_empty());
        assert_eq!(version_bundles.len(), 1);
        assert_eq!(version_bundles[0].tx.tx_id, docs_tx);
        assert_eq!(version_bundles[0].tx.kind, TxKind::Exclusive);
        assert!(version_bundles[0].tx.n_total_writes > version_bundles[0].versions.len() as u32);
        assert_eq!(version_bundles[0].versions.len(), 1);
        assert_eq!(version_bundles[0].versions[0].row_uuid(), doc_a);
        assert!(peer.shipped_complete_tx_payloads().is_empty());
        assert_eq!(
            peer.maintained_subscription_view_metrics()
                .full_recomputes_out,
            0
        );
        assert_eq!(peer.metrics.full_diff_recomputes_out, 0);
    }

    #[test]
    fn maintained_subscription_view_rehydrate_replaces_subscription_and_fresh_indexes() {
        let (_off_dir, mut off_core) = open_node_with_uuid(node(0x92));
        let (_on_dir, mut on_core) = open_node_with_uuid(node(0x92));
        let first = row(0x21);
        let second = row(0x22);
        for core in [&mut off_core, &mut on_core] {
            let tx_id = core
                .commit_mergeable(
                    MergeableCommit::new("todos", first, 1_000).cells(title_cells("match")),
                )
                .unwrap();
            accept_global(core, tx_id, 1);
        }
        let (shape, binding) = title_shape_binding("match");
        let subscription = subscription_key(&shape, &binding);
        let mut off_peer = PeerState::new();
        off_peer.force_full_recompute_path_for_test(true);
        let mut on_peer = PeerState::new();

        let off_initial = off_peer
            .rehydrate_query(&mut off_core, &shape, &binding)
            .unwrap();
        let on_initial = on_peer
            .rehydrate_query(&mut on_core, &shape, &binding)
            .unwrap();
        assert_eq!(
            capture_view_update(on_initial),
            capture_view_update(off_initial)
        );
        let old_id = maintained_subscription_id(&on_peer, subscription)
            .expect("initial maintained subscription missing");

        for core in [&mut off_core, &mut on_core] {
            let tx_id = core
                .commit_mergeable(
                    MergeableCommit::new("todos", second, 2_000).cells(title_cells("match")),
                )
                .unwrap();
            accept_global(core, tx_id, 2);
        }
        let off_tick = off_peer
            .query_update(&mut off_core, &shape, &binding)
            .unwrap();
        let on_tick = on_peer
            .query_update(&mut on_core, &shape, &binding)
            .unwrap();
        assert_eq!(capture_view_update(on_tick), capture_view_update(off_tick));

        let off_rehydrate = off_peer
            .rehydrate_query(&mut off_core, &shape, &binding)
            .unwrap();
        let on_rehydrate = on_peer
            .rehydrate_query(&mut on_core, &shape, &binding)
            .unwrap();
        let new_id = maintained_subscription_id(&on_peer, subscription)
            .expect("replacement maintained subscription missing");
        assert_ne!(old_id, new_id);
        assert!(!on_core.unsubscribe_groove_subscription(old_id));
        assert_eq!(
            capture_view_update(on_rehydrate),
            capture_view_update(off_rehydrate)
        );
    }

    #[test]
    fn maintained_subscription_view_new_binding_after_forget_has_no_stale_state() {
        let (_dir, mut core) = open_node_with_uuid(node(0x93));
        let match_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", row(0x31), 1_000).cells(title_cells("match")),
            )
            .unwrap();
        accept_global(&mut core, match_tx, 1);
        let other_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", row(0x32), 1_001).cells(title_cells("other")),
            )
            .unwrap();
        accept_global(&mut core, other_tx, 2);

        let (shape, match_binding) = title_shape_binding("match");
        let (_, other_binding) = title_shape_binding("other");
        let match_subscription = subscription_key(&shape, &match_binding);
        let other_subscription = subscription_key(&shape, &other_binding);
        assert_ne!(match_subscription, other_subscription);

        let mut peer = PeerState::new();
        peer.rehydrate_query(&mut core, &shape, &match_binding)
            .unwrap();
        assert!(peer.forget_subscription_with_node(&mut core, match_subscription));

        let update = peer
            .rehydrate_query(&mut core, &shape, &other_binding)
            .unwrap();
        assert!(maintained_subscription_id(&peer, match_subscription).is_none());
        assert!(maintained_subscription_id(&peer, other_subscription).is_some());
        assert_eq!(
            row_result_set(&peer, other_subscription),
            Some(BTreeSet::from([(
                groove::Intern::new("todos".to_owned()),
                row(0x32),
                other_tx,
            )]))
        );
        let SyncMessage::ViewUpdate {
            result_row_adds,
            result_row_removes,
            reset_result_set,
            ..
        } = update
        else {
            panic!("expected view update");
        };
        assert!(reset_result_set);
        assert_eq!(
            result_row_adds,
            vec![(groove::Intern::new("todos".to_owned()), row(0x32), other_tx,)]
        );
        assert!(result_row_removes.is_empty());
    }

    #[test]
    fn peer_state_dedups_version_payloads_across_subscription_views() {
        let (_dir, mut core) = open_node_with_uuid(node(9));
        let row = row(1);
        let tx_id = core
            .commit_mergeable(MergeableCommit::new("todos", row, 10).cells(title_cells("shared")))
            .unwrap();
        accept_global(&mut core, tx_id, 1);
        let mut peer = PeerState::new();

        let first = peer.current_rows_update(&mut core, "todos").unwrap();
        let SyncMessage::ViewUpdate {
            version_bundles,
            peer_payload_inventory:
                crate::protocol::PeerPayloadInventory {
                    complete_tx_payloads: complete_tx_payload_refs,
                },
            result_row_adds,
            result_row_removes,
            ..
        } = first
        else {
            panic!("expected view update");
        };
        assert_eq!(version_bundles.len(), 1);
        assert!(complete_tx_payload_refs.is_empty());
        assert_eq!(
            result_row_adds,
            vec![("todos".to_owned().into(), row, tx_id)]
        );
        assert!(result_row_removes.is_empty());

        let second = peer.current_rows_update(&mut core, "todos").unwrap();
        let SyncMessage::ViewUpdate {
            version_bundles,
            peer_payload_inventory:
                crate::protocol::PeerPayloadInventory {
                    complete_tx_payloads: complete_tx_payload_refs,
                },
            result_row_adds,
            result_row_removes,
            ..
        } = second
        else {
            panic!("expected view update");
        };
        assert!(version_bundles.is_empty());
        assert!(complete_tx_payload_refs.is_empty());
        assert!(result_row_adds.is_empty());
        assert!(result_row_removes.is_empty());
        assert_eq!(peer.metrics.version_bundles_out, 1);
        assert_eq!(peer.metrics.complete_tx_payload_refs_out, 0);
        assert_eq!(peer.metrics.result_adds_out, 1);
        assert_eq!(peer.metrics.result_removes_out, 0);
        assert_eq!(
            peer.shipped_complete_tx_payloads(),
            &BTreeSet::from([tx_id])
        );
    }

    #[test]
    fn current_rows_update_installs_maintained_subscription_for_relay_and_edge_client() {
        let schema = access_policy_schema();
        let (_dir, mut core) = open_node_with_schema(node(9), schema);
        let owner = AuthorId::from_bytes([0xa1; 16]);
        let other = AuthorId::from_bytes([0xb2; 16]);
        let project = row(0x40);
        let doc = row(0x41);
        let grant = row(0x42);
        let doc_tx = core
            .commit_mergeable(
                MergeableCommit::new("docs", doc, 10).cells(doc_cells("visible", project)),
            )
            .unwrap();
        accept_global(&mut core, doc_tx, 1);
        let grant_tx = core
            .commit_mergeable(
                MergeableCommit::new("docAccess", grant, 11).cells(access_cells(doc, owner)),
            )
            .unwrap();
        accept_global(&mut core, grant_tx, 2);
        let subscription = core.whole_table_subscription_key("docs").unwrap();

        let mut relay = PeerState::relay();
        let relay_update = relay.current_rows_update(&mut core, "docs").unwrap();
        assert!(maintained_subscription_id(&relay, subscription).is_some());
        assert_eq!(relay.maintained_subscription_view_metrics().hits_out, 1);
        assert!(view_update_added_rows(relay_update).contains(&doc));

        let mut edge_owner = PeerState::edge_client(owner);
        let edge_update = edge_owner.current_rows_update(&mut core, "docs").unwrap();
        assert!(maintained_subscription_id(&edge_owner, subscription).is_some());
        assert_eq!(
            edge_owner.maintained_subscription_view_metrics().hits_out,
            1
        );
        assert!(view_update_added_rows(edge_update).contains(&doc));

        let mut edge_other = PeerState::edge_client(other);
        let other_update = edge_other.current_rows_update(&mut core, "docs").unwrap();
        assert!(maintained_subscription_id(&edge_other, subscription).is_some());
        assert_eq!(
            edge_other.maintained_subscription_view_metrics().hits_out,
            1
        );
        assert!(!view_update_added_rows(other_update).contains(&doc));
    }

    #[test]
    fn grant_later_exclusive_tx_extends_view_scoped_partial_bundle_after_policy_grant() {
        let schema = access_policy_schema();
        let (_writer_dir, mut writer) = open_node_with_schema(node(1), schema.clone());
        let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
        let (_reader_dir, mut reader) = open_node_with_schema(node(3), schema);
        let user = AuthorId::from_bytes([0xa1; 16]);
        let doc_one = row(1);
        let doc_two = row(2);
        let project = row(9);

        let tx = writer.open_exclusive().unwrap();
        writer
            .tx_write(tx, "docs", doc_one, doc_cells("one", project), None)
            .unwrap();
        writer
            .tx_write(tx, "docs", doc_two, doc_cells("two", project), None)
            .unwrap();
        let (docs_tx, unit) = writer.commit_exclusive(tx, AuthorId::SYSTEM, 10).unwrap();
        let [fate] = core.apply_sync_message(unit).unwrap().try_into().unwrap();
        assert!(matches!(
            fate,
            SyncMessage::FateUpdate {
                fate: Fate::Accepted,
                ..
            }
        ));

        let first_grant = core
            .commit_mergeable(
                MergeableCommit::new("docAccess", row(11), 11).cells(access_cells(doc_one, user)),
            )
            .unwrap();
        accept_global(&mut core, first_grant, 2);

        let mut peer = PeerState::for_author(user);
        let first_update = peer.current_rows_update(&mut core, "docs").unwrap();
        let SyncMessage::ViewUpdate {
            version_bundles,
            peer_payload_inventory:
                crate::protocol::PeerPayloadInventory {
                    complete_tx_payloads: complete_tx_payload_refs,
                },
            result_row_adds,
            ..
        } = &first_update
        else {
            panic!("expected view update");
        };
        assert!(complete_tx_payload_refs.is_empty());
        assert_eq!(
            result_row_adds,
            &vec![("docs".to_owned().into(), doc_one, docs_tx)]
        );
        assert_eq!(version_bundles.len(), 1);
        assert_eq!(version_bundles[0].tx.tx_id, docs_tx);
        assert_eq!(version_bundles[0].tx.kind, TxKind::Exclusive);
        assert_eq!(version_bundles[0].versions.len(), 1);
        assert_eq!(version_bundles[0].versions[0].row_uuid(), doc_one);
        assert!(peer.shipped_complete_tx_payloads().is_empty());
        reader.apply_sync_message(first_update).unwrap();
        assert_eq!(
            reader
                .subscription_current_rows("docs", DurabilityTier::Global)
                .unwrap()
                .into_iter()
                .map(current_row_pair)
                .collect::<BTreeMap<_, _>>(),
            BTreeMap::from([(doc_one, doc_cells("one", project))])
        );

        let second_grant = core
            .commit_mergeable(
                MergeableCommit::new("docAccess", row(12), 12).cells(access_cells(doc_two, user)),
            )
            .unwrap();
        accept_global(&mut core, second_grant, 3);

        let grant_update = peer.current_rows_update(&mut core, "docs").unwrap();
        let SyncMessage::ViewUpdate {
            version_bundles,
            peer_payload_inventory:
                crate::protocol::PeerPayloadInventory {
                    complete_tx_payloads: complete_tx_payload_refs,
                },
            result_row_adds,
            result_row_removes,
            ..
        } = &grant_update
        else {
            panic!("expected view update");
        };
        assert!(complete_tx_payload_refs.is_empty());
        assert!(result_row_removes.is_empty());
        assert_eq!(
            result_row_adds,
            &vec![("docs".to_owned().into(), doc_two, docs_tx),]
        );
        assert_eq!(version_bundles.len(), 1);
        assert_eq!(version_bundles[0].tx.tx_id, docs_tx);
        assert_eq!(version_bundles[0].tx.kind, TxKind::Exclusive);
        assert_eq!(version_bundles[0].versions.len(), 1);
        assert_eq!(version_bundles[0].versions[0].row_uuid(), doc_two);
        assert_eq!(peer.metrics.full_diff_recomputes_out, 0);

        reader.apply_sync_message(grant_update).unwrap();
        assert_eq!(
            reader
                .subscription_current_rows("docs", DurabilityTier::Global)
                .unwrap()
                .into_iter()
                .map(current_row_pair)
                .collect::<BTreeMap<_, _>>(),
            BTreeMap::from([
                (doc_one, doc_cells("one", project)),
                (doc_two, doc_cells("two", project)),
            ])
        );
    }

    #[test]
    fn large_rehydrate_defers_closure_contributions_until_incremental_delta_needs_them() {
        let (_dir, mut core) = open_node_with_uuid(node(9));
        let mut tx_ids = Vec::new();
        for idx in 0..=LARGE_REHYDRATE_RESULT_ROWS {
            let tx_id = core
                .commit_mergeable(
                    MergeableCommit::new("todos", row_from_u64(idx as u64), 10 + idx as u64)
                        .cells(title_cells("match")),
                )
                .unwrap();
            accept_global(&mut core, tx_id, idx as u64 + 1);
            tx_ids.push(tx_id);
        }

        let (shape, binding) = title_shape_binding("match");
        let subscription = subscription_key(&shape, &binding);
        let mut peer = PeerState::new();
        peer.force_full_recompute_path_for_test(true);
        peer.rehydrate_query(&mut core, &shape, &binding).unwrap();

        let state = peer.subscriptions.get(&subscription).unwrap();
        assert_eq!(state.row_index.len(), LARGE_REHYDRATE_RESULT_ROWS + 1);
        assert!(state.closure_contributions.is_empty());
        assert!(!state.closure_contributions_complete);
        assert!(state.query_subscription.is_none());

        let (_prepared_shape, prepared_binding, plan) = core
            .prepare_query_binding_for_link(
                &shape,
                &binding,
                DurabilityTier::Global,
                peer.identity(),
            )
            .unwrap();
        let receiver = core
            .subscribe_query_binding_with_plan(&prepared_binding, &plan)
            .unwrap()
            .unwrap();
        drain_initial_subscription_snapshot(&receiver);
        peer.subscriptions
            .entry(subscription)
            .or_default()
            .query_subscription = Some(receiver);

        let changed_row = row_from_u64(0);
        let changed_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", changed_row, 20_000)
                    .parents(vec![tx_ids[0]])
                    .cells(title_cells("other")),
            )
            .unwrap();
        accept_global(&mut core, changed_tx, 20_000);

        let update = peer.query_update(&mut core, &shape, &binding).unwrap();
        let SyncMessage::ViewUpdate {
            result_row_adds,
            result_row_removes,
            ..
        } = &update
        else {
            panic!("expected view update");
        };
        assert!(result_row_adds.is_empty());
        assert_eq!(
            result_row_removes,
            &vec![("todos".to_owned().into(), changed_row, tx_ids[0])]
        );
        assert_eq!(peer.metrics.full_diff_recomputes_out, 0);

        let state = peer.subscriptions.get(&subscription).unwrap();
        assert_eq!(state.row_index.len(), LARGE_REHYDRATE_RESULT_ROWS);
        assert!(state.closure_contributions.is_empty());
        assert!(!state.closure_contributions_complete);
        assert!(
            !state
                .row_index
                .contains_key(&("todos".to_owned().into(), changed_row))
        );
    }

    #[test]
    fn all_exclusive_never_gated_stays_incremental() {
        let (_core_dir, mut core) = open_node_with_uuid(node(9));
        let row_one = row(1);
        let row_two = row(2);
        let mut peer = PeerState::new();

        let empty = peer.current_rows_update(&mut core, "todos").unwrap();
        let SyncMessage::ViewUpdate {
            result_row_adds,
            version_bundles,
            ..
        } = empty
        else {
            panic!("expected view update");
        };
        assert!(result_row_adds.is_empty());
        assert!(version_bundles.is_empty());

        let tx = core.open_exclusive().unwrap();
        core.tx_write(tx, "todos", row_one, title_cells("one"), None)
            .unwrap();
        core.tx_write(tx, "todos", row_two, title_cells("two"), None)
            .unwrap();
        let (tx_id, _unit) = core.commit_exclusive(tx, AuthorId::SYSTEM, 10).unwrap();
        accept_global(&mut core, tx_id, 1);

        let update = peer.current_rows_update(&mut core, "todos").unwrap();
        let SyncMessage::ViewUpdate {
            result_row_adds,
            version_bundles,
            peer_payload_inventory:
                crate::protocol::PeerPayloadInventory {
                    complete_tx_payloads: complete_tx_payload_refs,
                },
            result_row_removes,
            ..
        } = update
        else {
            panic!("expected view update");
        };
        assert_eq!(
            result_row_adds,
            vec![
                ("todos".to_owned().into(), row_one, tx_id),
                ("todos".to_owned().into(), row_two, tx_id),
            ]
        );
        assert_eq!(version_bundles.len(), 1);
        assert!(complete_tx_payload_refs.is_empty());
        assert!(result_row_removes.is_empty());
        assert_eq!(peer.metrics.full_diff_recomputes_out, 0);
    }

    #[test]
    fn legacy_query_delta_repairs_missing_exclusive_sibling_incrementally() {
        let (_dir, mut core) = open_node_with_uuid(node(9));
        let row_one = row(1);
        let row_two = row(2);
        let (shape, binding) = title_shape_binding("match");
        let subscription = subscription_key(&shape, &binding);
        let mut peer = PeerState::new();
        peer.force_full_recompute_path_for_test(true);
        peer.rehydrate_query(&mut core, &shape, &binding).unwrap();

        let receiver = query_subscription(&peer, subscription).unwrap();
        let tx = core.open_exclusive().unwrap();
        core.tx_write(tx, "todos", row_one, title_cells("match"), None)
            .unwrap();
        core.tx_write(tx, "todos", row_two, title_cells("match"), None)
            .unwrap();
        let (tx_id, _unit) = core.commit_exclusive(tx, AuthorId::SYSTEM, 10).unwrap();
        accept_global(&mut core, tx_id, 1);

        let real_deltas = receiver
            .try_recv()
            .expect("exclusive commit should produce a query delta");
        let one_positive = real_deltas
            .deltas
            .iter()
            .find(|delta| delta.weight > 0)
            .expect("exclusive commit should add a matching row")
            .clone();
        let partial_delta = RecordDeltas {
            descriptor: real_deltas.descriptor,
            deltas: vec![one_positive],
        };

        let update = peer
            .query_update_from_deltas(
                &mut core,
                &shape,
                &binding,
                subscription,
                vec![partial_delta],
            )
            .unwrap();
        let SyncMessage::ViewUpdate {
            result_row_adds,
            result_row_removes,
            version_bundles,
            ..
        } = update
        else {
            panic!("expected query view update");
        };
        assert!(result_row_removes.is_empty());
        assert_eq!(
            result_row_adds,
            vec![
                ("todos".to_owned().into(), row_one, tx_id),
                ("todos".to_owned().into(), row_two, tx_id),
            ]
        );
        assert_eq!(version_bundles.len(), 1);
        assert_eq!(version_bundles[0].tx.tx_id, tx_id);
        assert_eq!(version_bundles[0].tx.kind, TxKind::Exclusive);
        assert_eq!(version_bundles[0].versions.len(), 2);
        assert_eq!(peer.metrics.full_diff_recomputes_out, 0);
        assert_eq!(
            row_result_set(&peer, subscription),
            Some(BTreeSet::from([
                ("todos".to_owned().into(), row_one, tx_id),
                ("todos".to_owned().into(), row_two, tx_id),
            ]))
        );
    }

    #[test]
    fn peer_state_records_current_result_set_and_can_rehydrate() {
        let (_dir, mut core) = open_node_with_uuid(node(9));
        let row = row(1);
        let tx_id = core
            .commit_mergeable(MergeableCommit::new("todos", row, 10).cells(title_cells("task")))
            .unwrap();
        accept_global(&mut core, tx_id, 1);
        let mut peer = PeerState::new();
        let subscription = core.whole_table_subscription_key("todos").unwrap();

        peer.current_rows_update(&mut core, "todos").unwrap();
        assert_eq!(
            peer.subscription_result_sets(subscription),
            Some(BTreeSet::from([tx_id]))
        );

        peer.forget_subscription(subscription);
        assert!(peer.subscription_result_sets(subscription).is_none());
        let rehydrated = peer.current_rows_update(&mut core, "todos").unwrap();
        let SyncMessage::ViewUpdate {
            version_bundles,
            peer_payload_inventory:
                crate::protocol::PeerPayloadInventory {
                    complete_tx_payloads: complete_tx_payload_refs,
                },
            result_row_adds,
            result_row_removes,
            ..
        } = rehydrated
        else {
            panic!("expected view update");
        };
        assert!(version_bundles.is_empty());
        assert_eq!(complete_tx_payload_refs, vec![tx_id]);
        assert_eq!(
            result_row_adds,
            vec![("todos".to_owned().into(), row, tx_id)]
        );
        assert!(result_row_removes.is_empty());

        let rows = core.current_rows("todos", DurabilityTier::Local).unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn rehydrate_keeps_peer_payload_dedup_but_resends_result_set() {
        let (_core_dir, mut core) = open_node_with_uuid(node(9));
        let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
        let deleted_row = row(1);
        let live_row = row(2);
        let deleted_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", deleted_row, 10).cells(title_cells("deleted")),
            )
            .unwrap();
        accept_global(&mut core, deleted_tx, 1);
        let live_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", live_row, 11).cells(title_cells("live")),
            )
            .unwrap();
        accept_global(&mut core, live_tx, 2);
        let mut peer = PeerState::new();

        let initial = peer.current_rows_update(&mut core, "todos").unwrap();
        reader.apply_sync_message(initial).unwrap();
        assert_eq!(
            reader
                .subscription_current_rows("todos", DurabilityTier::Local)
                .unwrap()
                .len(),
            2
        );

        let deletion_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", deleted_row, 12).deletion(DeletionEvent::Deleted),
            )
            .unwrap();
        accept_global(&mut core, deletion_tx, 3);
        let missed_remove = peer.current_rows_update(&mut core, "todos").unwrap();
        let SyncMessage::ViewUpdate {
            result_row_removes, ..
        } = &missed_remove
        else {
            panic!("expected view update");
        };
        assert_eq!(
            result_row_removes,
            &vec![("todos".to_owned().into(), deleted_row, deleted_tx)]
        );

        let subscription = core.whole_table_subscription_key("todos").unwrap();
        let rehydrated = peer
            .handle_current_rows_rehydrate(
                &mut core,
                "todos",
                SyncMessage::Rehydrate { subscription },
            )
            .unwrap();
        let SyncMessage::ViewUpdate {
            reset_result_set,
            version_bundles,
            peer_payload_inventory:
                crate::protocol::PeerPayloadInventory {
                    complete_tx_payloads: complete_tx_payload_refs,
                },
            result_row_adds,
            result_row_removes,
            ..
        } = &rehydrated
        else {
            panic!("expected view update");
        };
        assert!(*reset_result_set);
        assert!(complete_tx_payload_refs.contains(&live_tx));
        assert_eq!(
            result_row_adds,
            &vec![("todos".to_owned().into(), live_row, live_tx)]
        );
        assert!(result_row_removes.is_empty());
        assert!(
            version_bundles
                .iter()
                .all(|bundle| bundle.tx.tx_id != live_tx && bundle.tx.tx_id != deleted_tx),
            "rehydrate must not resend already shipped content bundles"
        );
        reader.apply_sync_message(rehydrated).unwrap();
        assert_eq!(
            reader
                .subscription_current_rows("todos", DurabilityTier::Global)
                .unwrap()
                .into_iter()
                .map(current_row_pair)
                .collect::<BTreeMap<_, _>>(),
            BTreeMap::from([(live_row, title_cells("live"))])
        );
    }

    #[test]
    fn peer_state_sends_result_removes_after_deletes() {
        let (_core_dir, mut core) = open_node_with_uuid(node(9));
        let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
        let row = row(1);
        let tx_id = core
            .commit_mergeable(MergeableCommit::new("todos", row, 10).cells(title_cells("task")))
            .unwrap();
        accept_global(&mut core, tx_id, 1);
        let mut peer = PeerState::new();

        let initial = peer.current_rows_update(&mut core, "todos").unwrap();
        reader.apply_sync_message(initial).unwrap();
        assert_eq!(
            reader
                .subscription_current_rows("todos", DurabilityTier::Local)
                .unwrap()
                .len(),
            1
        );

        let deletion_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", row, 11).deletion(DeletionEvent::Deleted),
            )
            .unwrap();
        accept_global(&mut core, deletion_tx, 2);
        let removed = peer.current_rows_update(&mut core, "todos").unwrap();
        let SyncMessage::ViewUpdate {
            result_row_adds,
            result_row_removes,
            ..
        } = &removed
        else {
            panic!("expected view update");
        };
        assert!(result_row_adds.is_empty());
        assert_eq!(
            result_row_removes,
            &vec![("todos".to_owned().into(), row, tx_id)]
        );
        reader.apply_sync_message(removed).unwrap();
        assert!(
            reader
                .subscription_current_rows("todos", DurabilityTier::Local)
                .unwrap()
                .is_empty()
        );
        assert_eq!(peer.metrics.result_removes_out, 1);
    }

    #[test]
    fn whole_table_incremental_delta_ships_restore_register_witness() {
        let (_core_dir, mut core) = open_node_with_uuid(node(9));
        let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
        let row = row(1);
        let content_tx = core
            .commit_mergeable(MergeableCommit::new("todos", row, 10).cells(title_cells("task")))
            .unwrap();
        accept_global(&mut core, content_tx, 1);
        let mut peer = PeerState::new();

        reader
            .apply_sync_message(peer.current_rows_update(&mut core, "todos").unwrap())
            .unwrap();
        let deletion_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", row, 11).deletion(DeletionEvent::Deleted),
            )
            .unwrap();
        accept_global(&mut core, deletion_tx, 2);
        reader
            .apply_sync_message(peer.current_rows_update(&mut core, "todos").unwrap())
            .unwrap();
        assert!(
            reader
                .subscription_current_rows("todos", DurabilityTier::Global)
                .unwrap()
                .is_empty()
        );

        let restore_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", row, 12).deletion(DeletionEvent::Restored),
            )
            .unwrap();
        accept_global(&mut core, restore_tx, 3);
        let restored = peer.current_rows_update(&mut core, "todos").unwrap();
        let SyncMessage::ViewUpdate {
            version_bundles,
            peer_payload_inventory:
                crate::protocol::PeerPayloadInventory {
                    complete_tx_payloads: complete_tx_payload_refs,
                },
            result_row_adds,
            result_row_removes,
            ..
        } = &restored
        else {
            panic!("expected view update");
        };
        assert_eq!(
            result_row_adds,
            &vec![("todos".to_owned().into(), row, content_tx)]
        );
        assert!(result_row_removes.is_empty());
        assert!(
            version_bundles
                .iter()
                .any(|bundle| bundle.tx.tx_id == restore_tx)
                || complete_tx_payload_refs.contains(&restore_tx),
            "restore register must ship as negative knowledge with the result add"
        );
        reader.apply_sync_message(restored).unwrap();
        assert_eq!(
            reader
                .subscription_current_rows("todos", DurabilityTier::Global)
                .unwrap()
                .into_iter()
                .map(current_row_pair)
                .collect::<BTreeMap<_, _>>(),
            BTreeMap::from([(row, title_cells("task"))])
        );
    }

    #[test]
    fn incremental_query_result_set_tracks_identical_cell_rewrite_tx_id() {
        let (_dir, mut core) = open_node_with_uuid(node(9));
        let row_uuid = row(1);
        let first_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", row_uuid, 10).cells(title_cells("same")),
            )
            .unwrap();
        accept_global(&mut core, first_tx, 1);
        let shape = Query::from("todos")
            .filter(eq(col("title"), param("title")))
            .validate(&schema())
            .unwrap();
        let binding = shape
            .bind(BTreeMap::from([(
                "title".to_owned(),
                Value::String("same".to_owned()),
            )]))
            .unwrap();
        let subscription = SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
        };
        let mut peer = PeerState::new();
        peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
        assert_eq!(
            row_result_set(&peer, subscription),
            Some(BTreeSet::from([(
                "todos".to_owned().into(),
                row_uuid,
                first_tx
            )]))
        );

        let second_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", row_uuid, 11).cells(title_cells("same")),
            )
            .unwrap();
        accept_global(&mut core, second_tx, 2);
        let update = peer.query_update(&mut core, &shape, &binding).unwrap();
        let SyncMessage::ViewUpdate {
            result_row_adds,
            result_row_removes,
            ..
        } = update
        else {
            panic!("expected query view update");
        };
        assert_eq!(
            result_row_removes,
            vec![("todos".to_owned().into(), row_uuid, first_tx)]
        );
        assert_eq!(
            result_row_adds,
            vec![("todos".to_owned().into(), row_uuid, second_tx)]
        );
        assert_eq!(
            row_result_set(&peer, subscription),
            Some(BTreeSet::from([(
                "todos".to_owned().into(),
                row_uuid,
                second_tx
            )]))
        );
    }

    #[test]
    fn incremental_query_result_set_drops_enter_then_leave_same_drain_cycle() {
        let (_core_dir, mut core) = open_node_with_uuid(node(9));
        let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
        let row_uuid = row(1);
        let (shape, binding) = title_shape_binding("match");
        let subscription = subscription_key(&shape, &binding);
        let mut peer = PeerState::new();

        reader
            .apply_sync_message(peer.rehydrate_query(&mut core, &shape, &binding).unwrap())
            .unwrap();

        let match_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", row_uuid, 10).cells(title_cells("match")),
            )
            .unwrap();
        accept_global(&mut core, match_tx, 1);
        let unmatch_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", row_uuid, 11)
                    .parents(vec![match_tx])
                    .cells(title_cells("other")),
            )
            .unwrap();
        accept_global(&mut core, unmatch_tx, 2);

        let update = peer.query_update(&mut core, &shape, &binding).unwrap();
        let SyncMessage::ViewUpdate {
            result_row_adds,
            result_row_removes,
            ..
        } = &update
        else {
            panic!("expected query view update");
        };
        assert!(
            result_row_adds.is_empty(),
            "enter-then-leave in one drain must not ship a stale add"
        );
        assert!(result_row_removes.is_empty());
        assert!(row_result_set(&peer, subscription).is_none_or(|set| set.is_empty()));
        reader.apply_sync_message(update).unwrap();
        assert!(
            reader
                .query_rows(&shape, &binding, DurabilityTier::Global)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn incremental_query_result_set_keeps_leave_then_reenter_same_drain_cycle() {
        let (_core_dir, mut core) = open_node_with_uuid(node(9));
        let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
        let row_uuid = row(1);
        let first_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", row_uuid, 10).cells(title_cells("match")),
            )
            .unwrap();
        accept_global(&mut core, first_tx, 1);
        let (shape, binding) = title_shape_binding("match");
        let subscription = subscription_key(&shape, &binding);
        let mut peer = PeerState::new();

        reader
            .apply_sync_message(peer.rehydrate_query(&mut core, &shape, &binding).unwrap())
            .unwrap();
        assert_eq!(
            row_result_set(&peer, subscription),
            Some(BTreeSet::from([(
                "todos".to_owned().into(),
                row_uuid,
                first_tx
            )]))
        );

        let unmatch_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", row_uuid, 11)
                    .parents(vec![first_tx])
                    .cells(title_cells("other")),
            )
            .unwrap();
        accept_global(&mut core, unmatch_tx, 2);
        let second_match_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", row_uuid, 12)
                    .parents(vec![unmatch_tx])
                    .cells(title_cells("match")),
            )
            .unwrap();
        accept_global(&mut core, second_match_tx, 3);

        let update = peer.query_update(&mut core, &shape, &binding).unwrap();
        let SyncMessage::ViewUpdate {
            result_row_adds,
            result_row_removes,
            ..
        } = &update
        else {
            panic!("expected query view update");
        };
        assert_eq!(
            result_row_removes,
            &vec![("todos".to_owned().into(), row_uuid, first_tx)]
        );
        assert_eq!(
            result_row_adds,
            &vec![("todos".to_owned().into(), row_uuid, second_match_tx)]
        );
        assert_eq!(
            row_result_set(&peer, subscription),
            Some(BTreeSet::from([(
                "todos".to_owned().into(),
                row_uuid,
                second_match_tx
            )]))
        );
        reader.apply_sync_message(update).unwrap();
        assert_eq!(
            reader
                .query_rows(&shape, &binding, DurabilityTier::Global)
                .unwrap()
                .into_iter()
                .map(current_row_pair)
                .collect::<BTreeMap<_, _>>(),
            BTreeMap::from([(row_uuid, title_cells("match"))])
        );
    }

    #[test]
    fn incremental_query_result_set_same_tx_retract_assert_churn_nets_no_update() {
        let (_dir, mut core) = open_node_with_uuid(node(9));
        let row_uuid = row(1);
        let first_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", row_uuid, 10).cells(title_cells("same")),
            )
            .unwrap();
        accept_global(&mut core, first_tx, 1);
        let (shape, binding) = title_shape_binding("same");
        let subscription = subscription_key(&shape, &binding);
        let mut peer = PeerState::new();
        peer.force_full_recompute_path_for_test(true);
        peer.rehydrate_query(&mut core, &shape, &binding).unwrap();

        let second_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", row_uuid, 11)
                    .parents(vec![first_tx])
                    .cells(title_cells("same")),
            )
            .unwrap();
        accept_global(&mut core, second_tx, 2);
        let receiver = query_subscription(&peer, subscription).unwrap();
        let real_deltas = receiver.try_recv().unwrap();
        let real_delta = real_deltas
            .deltas
            .iter()
            .find(|delta| delta.weight != 0)
            .expect("rewrite should produce a query delta")
            .clone();
        let churn = RecordDeltas {
            descriptor: real_deltas.descriptor,
            deltas: vec![
                RecordDelta {
                    record: real_delta.record.clone(),
                    weight: -1,
                },
                RecordDelta {
                    record: real_delta.record,
                    weight: 1,
                },
            ],
        };

        let update = peer
            .query_update_from_deltas(&mut core, &shape, &binding, subscription, vec![churn])
            .unwrap();
        let SyncMessage::ViewUpdate {
            result_row_adds,
            result_row_removes,
            ..
        } = update
        else {
            panic!("expected query view update");
        };
        assert!(result_row_adds.is_empty());
        assert!(result_row_removes.is_empty());
        assert_eq!(
            row_result_set(&peer, subscription),
            Some(BTreeSet::from([(
                "todos".to_owned().into(),
                row_uuid,
                first_tx
            )]))
        );
    }

    #[test]
    fn incremental_query_result_set_rebuilds_stale_closure_rows() {
        let schema = JazzSchema::new([
            TableSchema::new("stock", [ColumnSchema::new("quantity", ColumnType::U64)]),
            TableSchema::new("orderLines", [ColumnSchema::new("stock", ColumnType::Uuid)])
                .with_reference("stock", "stock"),
        ]);
        let (_dir, mut core) = open_node_with_schema(node(9), schema.clone());
        let stock_row = row(1);
        let first_line_row = row(2);
        let second_line_row = row(3);
        let stock_v1 = core
            .commit_mergeable(
                MergeableCommit::new("stock", stock_row, 10)
                    .cells(BTreeMap::from([("quantity".to_owned(), Value::U64(10))])),
            )
            .unwrap();
        accept_global(&mut core, stock_v1, 1);
        let first_line_tx = core
            .commit_mergeable(
                MergeableCommit::new("orderLines", first_line_row, 11).cells(BTreeMap::from([(
                    "stock".to_owned(),
                    Value::Uuid(stock_row.0),
                )])),
            )
            .unwrap();
        accept_global(&mut core, first_line_tx, 2);
        let shape = Query::from("orderLines").validate(&schema).unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let subscription = subscription_key(&shape, &binding);
        let mut peer = PeerState::new();

        peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
        assert_eq!(
            row_result_set(&peer, subscription),
            Some(BTreeSet::from([
                (
                    "orderLines".to_owned().into(),
                    first_line_row,
                    first_line_tx
                ),
                ("stock".to_owned().into(), stock_row, stock_v1),
            ]))
        );

        let stock_v2 = core
            .commit_mergeable(
                MergeableCommit::new("stock", stock_row, 12)
                    .parents(vec![stock_v1])
                    .cells(BTreeMap::from([("quantity".to_owned(), Value::U64(9))])),
            )
            .unwrap();
        accept_global(&mut core, stock_v2, 3);
        let second_line_tx = core
            .commit_mergeable(
                MergeableCommit::new("orderLines", second_line_row, 13).cells(BTreeMap::from([(
                    "stock".to_owned(),
                    Value::Uuid(stock_row.0),
                )])),
            )
            .unwrap();
        accept_global(&mut core, second_line_tx, 4);

        let update = peer.query_update(&mut core, &shape, &binding).unwrap();
        let SyncMessage::ViewUpdate {
            result_row_adds,
            result_row_removes,
            ..
        } = update
        else {
            panic!("expected query view update");
        };
        assert_eq!(
            result_row_removes,
            vec![("stock".to_owned().into(), stock_row, stock_v1)]
        );
        assert_eq!(
            result_row_adds,
            vec![
                (
                    "orderLines".to_owned().into(),
                    second_line_row,
                    second_line_tx
                ),
                ("stock".to_owned().into(), stock_row, stock_v2),
            ]
        );
        assert_eq!(
            row_result_set(&peer, subscription),
            Some(BTreeSet::from([
                (
                    "orderLines".to_owned().into(),
                    first_line_row,
                    first_line_tx
                ),
                (
                    "orderLines".to_owned().into(),
                    second_line_row,
                    second_line_tx
                ),
                ("stock".to_owned().into(), stock_row, stock_v2),
            ]))
        );
    }

    #[test]
    fn incremental_query_result_sets_match_full_rehydrate_after_seeded_commits() {
        let (_dir, mut core) = open_node_with_uuid(node(9));
        let initial = [("a", row(1)), ("b", row(2)), ("a", row(3)), ("c", row(4))];
        let mut seq = 1;
        for (title, row_uuid) in initial {
            let tx_id = core
                .commit_mergeable(
                    MergeableCommit::new("todos", row_uuid, 10 + seq).cells(title_cells(title)),
                )
                .unwrap();
            accept_global(&mut core, tx_id, seq);
            seq += 1;
        }
        let shape = Query::from("todos")
            .filter(eq(col("title"), param("title")))
            .validate(&schema())
            .unwrap();
        let bindings = ["a", "b", "c"]
            .into_iter()
            .map(|title| {
                shape
                    .bind(BTreeMap::from([(
                        "title".to_owned(),
                        Value::String(title.to_owned()),
                    )]))
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let mut peers = bindings
            .iter()
            .map(|binding| {
                let mut peer = PeerState::new();
                peer.rehydrate_query(&mut core, &shape, binding).unwrap();
                peer
            })
            .collect::<Vec<_>>();
        let whole_subscription = core.whole_table_subscription_key("todos").unwrap();
        let mut whole_table_link = PeerState::new();
        whole_table_link
            .current_rows_update(&mut core, "todos")
            .unwrap();

        let title_cycle = ["b", "c", "a", "b", "a", "c"];
        let mut current_titles = ["a", "b", "a", "c"];
        for step in 0..18 {
            let row_idx = step % 4;
            let row_uuid = row(row_idx as u8 + 1);
            let mut title = title_cycle[step % title_cycle.len()];
            if title == current_titles[row_idx] {
                title = title_cycle[(step + 1) % title_cycle.len()];
            }
            current_titles[row_idx] = title;
            let tx_id = core
                .commit_mergeable(
                    MergeableCommit::new("todos", row_uuid, 100 + step as u64)
                        .cells(title_cells(title)),
                )
                .unwrap();
            accept_global(&mut core, tx_id, seq);
            seq += 1;

            for (peer, binding) in peers.iter_mut().zip(bindings.iter()) {
                peer.query_update(&mut core, &shape, binding).unwrap();
            }
            whole_table_link
                .current_rows_update(&mut core, "todos")
                .unwrap();
            for (peer, binding) in peers.iter().zip(bindings.iter()) {
                let mut fresh = PeerState::new();
                fresh.rehydrate_query(&mut core, &shape, binding).unwrap();
                let subscription = SubscriptionKey {
                    shape_id: shape.shape_id(),
                    binding_id: binding.binding_id(),
                };
                assert_eq!(
                    row_result_set(peer, subscription),
                    row_result_set(&fresh, subscription),
                    "incremental result set diverged from full rehydrate at step {step}"
                );
            }
            let mut fresh_whole = PeerState::new();
            fresh_whole.current_rows_update(&mut core, "todos").unwrap();
            assert_eq!(
                row_result_set(&whole_table_link, whole_subscription),
                row_result_set(&fresh_whole, whole_subscription),
                "incremental whole-table result set diverged from full rehydrate at step {step}"
            );
        }
    }
}
