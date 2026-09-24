//! Peer-local subscription and relay-authority support state.
//!
//! The transport/update algorithms remain in the parent `peer` module. This
//! module owns the state that those algorithms retain between deliveries,
//! including the short-lived authority-scope subscriptions used for relay fate
//! assignment.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use groove::ivm::MultisinkSubscription;
use groove::records::Value;

use super::super::ids::AuthorSubject;
use super::super::ids::SchemaVersionId;
use super::super::node::maintained_subscription_view::{
    MaintainedSubscriptionView, MaintainedTerminalSchemas,
};
use super::super::node::{
    CoveredInputReceiver, LocalAuthorityReconciliation, PreparedQueryPlanHandle,
};
use super::super::protocol::{
    AuthorityResultKey, KnownStateCompleteness, KnownStateDeclaration, ReadViewSpec,
    RegisterShapeOptions, ResultMemberEntry, SubscriptionKey,
};
use super::super::query::{Binding, ValidatedQuery};
use super::super::schema::TableSchema;
use super::super::tools::OutputOccurrenceId;
use super::super::tx::{DurabilityTier, TxId};

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
    /// The authenticated principal whose policy may be composed for this
    /// link. A relay is transport only: treating it as `SYSTEM` here would
    /// turn a topology role into a policy bypass.
    pub(super) fn permission_subject(self) -> Option<AuthorSubject> {
        match self {
            Self::Relay => None,
            Self::ClientLink { identity } => Some(identity),
        }
    }
}

/// Server-side shipped-state for one downstream subscription on a peer link.
#[derive(Debug, Default)]
pub(super) struct PeerSubscriptionState {
    /// Immutable admitted policy context for this usage site. Relay links can
    /// multiplex sessions, so this must not be inferred from connection role.
    pub(super) policy_binding: Option<(AuthorSubject, BTreeMap<String, Value>)>,
    /// Exact upstream authority receipt consumed by this served usage site.
    ///
    /// A relay's maintained receiver has its own synthetic subscription key
    /// so that downstream policy scopes do not share a runtime. Its source,
    /// however, is the separately registered upstream usage site. Keeping the
    /// source key here preserves that relationship without trying to recover
    /// it from the (non-unique) canonical binding view.
    pub(super) authority_result_source: Option<AuthorityResultKey>,
    /// This non-authoritative scope-relay usage must not materialize its
    /// receiver until `authority_result_source` has a live reset.  It is
    /// deliberately separate from the source key: direct authorities may
    /// retain a D source without awaiting an upstream handoff.
    pub(super) awaiting_selected_authority_source: bool,
    pub(super) result_member_set: BTreeSet<ResultMemberEntry>,
    pub(super) supporting_revision: Option<[u8; 16]>,
    /// Shared Local-plus-authority provenance. Receiver/materialization state
    /// remains peer-owned; exact-source reconciliation is shared with the DB
    /// facade rather than reimplemented at this transport boundary.
    pub(super) local_authority: LocalAuthorityReconciliation,
    pub(super) member_index: BTreeMap<MemberIndexKey, MemberSlot>,
    pub(super) maintained_subscription_view: Option<MaintainedSubscriptionViewSubscription>,
    pub(super) prepared_query: Option<CachedPeerQueryPlan>,
    pub(super) groove_runtime_token: Option<u64>,
    /// UUID adoption invalidates publication metadata without replacing Groove.
    pub(super) physical_identity_generation: Option<u64>,
    pub(super) authorization_progress: u64,
    pub(super) has_served_authorization_progress: bool,
}

impl PeerSubscriptionState {
    pub(super) fn clear_groove_runtime_handles(&mut self) {
        self.supporting_revision = None;
        self.maintained_subscription_view = None;
        if let Some(prepared_query) = &mut self.prepared_query {
            // The compiled plan belongs to one runtime, but its semantic
            // context remains the admitted context for this subscription.
            prepared_query.clear_runtime_plan();
        }
        self.groove_runtime_token = None;
        self.physical_identity_generation = None;
    }

    pub(super) fn member_result_set(&self) -> BTreeSet<ResultMemberEntry> {
        self.result_member_set.clone()
    }

    pub(super) fn supporting_rows(&self) -> impl Iterator<Item = &crate::protocol::SupportingRow> {
        self.maintained_subscription_view
            .iter()
            .flat_map(|view| view.maintained.supporting_rows())
    }

    pub(super) fn previous_tx_ids(&self) -> BTreeSet<TxId> {
        if self.supporting_revision.is_none() {
            return BTreeSet::new();
        }
        self.maintained_subscription_view
            .iter()
            .flat_map(|view| view.maintained.acknowledged_supporting_rows())
            .map(|row| row.version.tx)
            .collect()
    }
}

#[derive(Debug)]
pub(super) struct MaintainedSubscriptionViewSubscription {
    pub(super) subscription: MultisinkSubscription,
    pub(super) maintained: MaintainedSubscriptionView,
    pub(super) terminal_schemas: MaintainedTerminalSchemas,
    pub(super) tables: BTreeMap<String, TableSchema>,
    /// Exact receiver-owned inputs for a relay child. `None` means this
    /// is an ordinary trusted-serving maintained view, not a receiver.
    pub(super) covered_input_receiver: Option<CoveredInputReceiver>,
    pub(super) result_schema_version: SchemaVersionId,
    /// Exact authoritative source membership for a child of a durable
    /// relay. A canonical binding view alone is not a permission boundary.
    pub(super) source_authority_result: Option<AuthorityResultKey>,
    pub(super) initial_received: bool,
}

pub(super) struct MaintainedRehydrateRequest<'a> {
    pub(super) shape: &'a ValidatedQuery,
    pub(super) binding: &'a Binding,
    pub(super) subscription: SubscriptionKey,
    pub(super) previous_member_result_set: &'a BTreeSet<ResultMemberEntry>,
    pub(super) reset_input_set: bool,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct MemberSlot {
    pub(super) member: ResultMemberEntry,
    pub(super) refcount: usize,
}
