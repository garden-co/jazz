//! Peer-local subscription and edge-authority support state.
//!
//! The transport/update algorithms remain in the parent `peer` module. This
//! module owns the state that those algorithms retain between deliveries,
//! including the short-lived authority-scope subscriptions used for edge fate
//! assignment.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use groove::ivm::MultisinkSubscription;
use groove::records::Value;

use super::super::ids::AuthorSubject;
use super::super::node::PreparedQueryPlanHandle;
use super::super::node::maintained_subscription_view::{
    MaintainedSubscriptionView, MaintainedTerminalSchemas,
};
use super::super::protocol::{
    BindingViewKey, KnownStateCompleteness, KnownStateDeclaration, ProgramFactEntry, ReadViewSpec,
    RegisterShapeOptions, ResultMemberEntry, SubscriptionKey, VersionRecord,
};
use super::super::query::{Binding, ValidatedQuery};
use super::super::schema::TableSchema;
use super::super::tools::OutputOccurrenceId;
use super::super::tx::{DurabilityTier, Transaction, TxId};

const DEFAULT_EDGE_SCOPE_TTL_MS: u64 = 5_000;

pub(super) fn fast_current_membership_position(
    known_state: &Option<KnownStateDeclaration>,
) -> Option<super::super::time::GlobalTime> {
    match known_state {
        Some(KnownStateDeclaration::Fast {
            completeness: KnownStateCompleteness::FastCurrentMembership,
            position,
        }) => Some(*position),
        Some(KnownStateDeclaration::FastWithAuthorizationProgress {
            completeness: KnownStateCompleteness::FastCurrentMembership,
            position,
            ..
        }) => Some(*position),
        Some(KnownStateDeclaration::ExactVersionSet { .. }) | None => None,
    }
}

pub(super) fn fast_authorization_progress(
    known_state: &Option<KnownStateDeclaration>,
) -> Option<u64> {
    match known_state {
        Some(KnownStateDeclaration::FastWithAuthorizationProgress {
            completeness: KnownStateCompleteness::FastCurrentMembership,
            authorization_progress,
            ..
        }) => Some(*authorization_progress),
        Some(KnownStateDeclaration::Fast { .. })
        | Some(KnownStateDeclaration::ExactVersionSet { .. })
        | None => None,
    }
}

pub(super) fn member_settle_position(
    member: &ResultMemberEntry,
) -> Option<super::super::time::GlobalTime> {
    match member {
        ResultMemberEntry::Row(row) | ResultMemberEntry::TypedRow { row, .. } => {
            row.settle_position
        }
        ResultMemberEntry::Synthetic { .. } | ResultMemberEntry::PathTuple { .. } => None,
    }
}

pub(super) fn fast_cursor_requires_authoritative_reset(
    position: super::super::time::GlobalTime,
    previous: &BTreeSet<ResultMemberEntry>,
    current: &BTreeSet<ResultMemberEntry>,
) -> bool {
    fast_cursor_membership_mismatch(position, previous, current)
}

pub(super) fn fast_cursor_membership_mismatch(
    position: super::super::time::GlobalTime,
    previous: &BTreeSet<ResultMemberEntry>,
    current: &BTreeSet<ResultMemberEntry>,
) -> bool {
    previous.difference(current).next().is_some()
        || current
            .difference(previous)
            .any(|member| member_settle_position(member).is_none_or(|settled| settled <= position))
}

/// Server-side role for one peer link.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PeerRole {
    /// Permanent relay/cache link to another node.
    Relay,
    /// Link serving one terminated client identity.
    ClientLink {
        /// Client author identity terminated by this link.
        identity: AuthorSubject,
    },
}

impl PeerRole {
    pub(super) fn identity(self) -> AuthorSubject {
        match self {
            Self::Relay => AuthorSubject::SYSTEM,
            Self::ClientLink { identity } => identity,
        }
    }
}

/// Server-side shipped-state for one downstream subscription on a peer link.
#[derive(Debug, Default)]
pub(super) struct PeerSubscriptionState {
    /// Immutable admitted policy context for this usage site. Relay links can
    /// multiplex sessions, so this must not be inferred from connection role.
    pub(super) policy_binding: Option<(AuthorSubject, BTreeMap<String, Value>)>,
    pub(super) result_member_set: BTreeSet<ResultMemberEntry>,
    pub(super) program_fact_set: BTreeSet<ProgramFactEntry>,
    pub(super) member_index: BTreeMap<MemberIndexKey, MemberSlot>,
    pub(super) maintained_subscription_view: Option<MaintainedSubscriptionViewSubscription>,
    pub(super) prepared_query: Option<CachedPeerQueryPlan>,
    pub(super) groove_runtime_token: Option<u64>,
    pub(super) authorization_progress: u64,
    pub(super) has_served_authorization_progress: bool,
}

impl PeerSubscriptionState {
    pub(super) fn clear_groove_runtime_handles(&mut self) {
        self.maintained_subscription_view = None;
        if let Some(prepared_query) = &mut self.prepared_query {
            // The compiled plan belongs to one runtime, but its semantic
            // context remains the admitted context for this subscription.
            prepared_query.clear_runtime_plan();
        }
        self.groove_runtime_token = None;
    }

    pub(super) fn member_result_set(&self) -> BTreeSet<ResultMemberEntry> {
        self.result_member_set.clone()
    }

    pub(super) fn program_fact_set(&self) -> BTreeSet<ProgramFactEntry> {
        self.program_fact_set.clone()
    }

    pub(super) fn previous_tx_ids(&self) -> BTreeSet<TxId> {
        self.result_member_set
            .iter()
            .filter_map(ResultMemberEntry::as_row)
            .map(|(_, _, tx_id)| tx_id)
            .collect()
    }
}

#[derive(Debug)]
pub(super) struct MaintainedSubscriptionViewSubscription {
    pub(super) subscription: MultisinkSubscription,
    pub(super) maintained: MaintainedSubscriptionView,
    pub(super) terminal_schemas: MaintainedTerminalSchemas,
    pub(super) tables: BTreeMap<String, TableSchema>,
    /// Authoritative source membership for an Edge child of a durable relay.
    pub(super) source_binding_view: Option<BindingViewKey>,
    pub(super) initial_received: bool,
}

pub(super) struct MaintainedRehydrateRequest<'a> {
    pub(super) shape: &'a ValidatedQuery,
    pub(super) binding: &'a Binding,
    pub(super) subscription: SubscriptionKey,
    pub(super) previous_member_result_set: &'a BTreeSet<ResultMemberEntry>,
    pub(super) reset_result_set: bool,
    pub(super) result_table_filter: Option<&'a str>,
    pub(super) tier: DurabilityTier,
    pub(super) read_view: &'a ReadViewSpec,
    pub(super) purpose: RehydratePurpose,
}

/// Regular queries require all prepared values. Authorization support treats
/// an absent policy claim as an empty proof instead.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RehydratePurpose {
    Query,
    AuthorizationSupport,
}

/// One physical row emitted for a rendered output occurrence.
///
/// A terminal root and each of its included children deliberately share the
/// root occurrence address, but they must keep independent contribution
/// lifetimes.  The physical table/row identity therefore belongs in the
/// delivery index as well as the occurrence.
pub(super) type RowKey = (
    OutputOccurrenceId,
    groove::Intern<String>,
    super::super::ids::RowUuid,
);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum MemberIndexKey {
    Row(RowKey),
    Synthetic { table: String, row: Vec<u8> },
    Member(ResultMemberEntry),
}

#[derive(Debug)]
pub(super) struct CachedPeerQueryPlan {
    tier: DurabilityTier,
    read_view: Arc<ReadViewSpec>,
    plan: Option<PreparedQueryPlanHandle>,
}

impl CachedPeerQueryPlan {
    pub(super) fn with_plan(opts: &RegisterShapeOptions, plan: PreparedQueryPlanHandle) -> Self {
        Self::with_context(opts.tier, Arc::new(opts.read_view.clone()), plan)
    }

    pub(super) fn with_context(
        tier: DurabilityTier,
        read_view: Arc<ReadViewSpec>,
        plan: PreparedQueryPlanHandle,
    ) -> Self {
        Self {
            tier,
            read_view,
            plan: Some(plan),
        }
    }

    pub(super) fn tier(&self) -> DurabilityTier {
        self.tier
    }

    pub(super) fn has_runtime_plan(&self) -> bool {
        self.plan.is_some()
    }

    pub(super) fn replace_runtime_plan(&mut self, plan: PreparedQueryPlanHandle) {
        self.plan = Some(plan);
    }

    pub(super) fn clear_runtime_plan(&mut self) {
        self.plan = None;
    }

    pub(super) fn context(&self) -> (DurabilityTier, Arc<ReadViewSpec>) {
        (self.tier, Arc::clone(&self.read_view))
    }
}

#[derive(Clone, Debug)]
pub(super) struct DeferredEdgeFate {
    pub(super) tx: Transaction,
    pub(super) versions: Vec<VersionRecord>,
    /// Wall-clock authority time captured when the client upload arrived.
    /// Deferred admission deliberately preserves this security boundary rather
    /// than treating time spent awaiting permission support as client slack.
    pub(super) admission_now_ms: u64,
    pub(super) permission_identity: AuthorSubject,
    pub(super) scope_subscriptions: Vec<SubscriptionKey>,
}

/// Peer-owned inputs to the edge eviction pin set.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PeerEvictionPins {
    /// Transactions currently parked on edge fate assignment.
    pub deferred_edge_fate_txs: BTreeSet<TxId>,
    /// Permission-scope subscriptions retained by active edge acceptance gates.
    pub referenced_scope_subscriptions: BTreeSet<SubscriptionKey>,
}

impl PeerEvictionPins {
    /// Merge another peer's pin roots into this aggregate pin set.
    pub fn extend(&mut self, other: Self) {
        self.deferred_edge_fate_txs
            .extend(other.deferred_edge_fate_txs);
        self.referenced_scope_subscriptions
            .extend(other.referenced_scope_subscriptions);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct MemberSlot {
    pub(super) member: ResultMemberEntry,
    pub(super) refcount: usize,
}

pub(super) fn edge_scope_ttl_ms() -> u64 {
    std::env::var("JAZZ_EDGE_SCOPE_TTL_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_EDGE_SCOPE_TTL_MS)
}
