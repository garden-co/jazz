//! Per-peer server-side sync state outside the Jazz data model. This module
//! owns shipped-complete-transaction-payload deduplication and per-subscription incremental result
//! maintenance for one downstream peer; subscriber-side settled canonical
//! binding-view result-set/completeness state lives on [`crate::node::NodeState`],
//! and view construction itself lives in [`crate::node::views`]. It sits beside
//! the node in the layer map as link-local state used to produce protocol messages.

mod delivery;

use delivery::*;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::TryRecvError;

use groove::db::{StorageReadBucket, StorageReadMetrics};
use groove::records::Value;
use groove::storage::{OrderedKvStorage, ReopenableStorage};
use web_time::Instant;

use crate::authorization_scope::AuthorityScopeAggregate;
use crate::ids::AuthorSubject;
use crate::node::maintained_subscription_view::{
    MaintainedSubscriptionViewFootprint as MaintainedSubscriptionViewIndexFootprint,
    ResultTransitions,
};
use crate::node::{Error, NodeState, PublicationOutcome};
#[cfg(test)]
use crate::protocol::KnownStateCompleteness;
#[cfg(test)]
use crate::protocol::ResultRowEntry;
use crate::protocol::{
    AuthorityResultKey, DelegatedSessionBinding, KnownStateDeclaration, ProgramFactEntry,
    ReadViewSpec, RegisterShapeOptions, ResultMemberEntry, RowVersionRef, ShapeAst, Subscribe,
    SubscriptionKey, SyncMessage, VersionBundle, VersionCarrier, VersionRecord,
    expand_version_carriers,
};
use crate::protocol_limits::validate_fetch_row_versions;
use crate::query::{Binding, ValidatedQuery};
use crate::schema::TableSchema;
use crate::time::GlobalTime;
use crate::tx::{DurabilityTier, Transaction, TxId, TxKind};

mod subscription_state;

#[cfg(test)]
use subscription_state::fast_cursor_membership_mismatch;
use subscription_state::{
    CachedPeerQueryPlan, DeferredEdgeFate, MaintainedRehydrateRequest,
    MaintainedSubscriptionViewSubscription, MemberIndexKey, MemberSlot, PeerSubscriptionState,
    RehydratePurpose, RowKey, edge_scope_ttl_ms, fast_authorization_progress,
    fast_current_membership_position, fast_cursor_requires_authoritative_reset,
    member_settle_position,
};
pub use subscription_state::{PeerEvictionPins, PeerRole};

/// Tracks what one downstream peer has already received.
#[derive(Debug)]
pub struct PeerState {
    /// Stable process-local identity used to release served-shape ownership.
    /// It is intentionally not a wire identity: reconnecting a peer state
    /// retains its publications, while a new peer state cannot release them.
    publication_owner: u64,
    role: PeerRole,
    permission_identity: Option<AuthorSubject>,
    /// Server/host-issued link capability.  This deliberately is not derived
    /// from `PeerRole`, a wire hello, or a semantic sync message.
    transport_capability: RelayTransportCapability,
    shipped_complete_tx_payloads: BTreeSet<TxId>,
    ship_complete_exclusive_payloads: bool,
    /// Maintained evaluator and shipped-membership state for canonical
    /// coverage outputs and concrete downstream publications. These entries
    /// describe what was evaluated or sent; receiver cursors live separately.
    publication_states: BTreeMap<SubscriptionKey, PeerSubscriptionState>,
    /// Receiver-owned cursors keyed only by the concrete usage subscription
    /// that declared them. A shared canonical coverage output must never adopt
    /// one subscriber's cursor.
    downstream_known_states: BTreeMap<SubscriptionKey, KnownStateDeclaration>,
    deferred_edge_fates: BTreeMap<TxId, DeferredEdgeFate>,
    edge_scope_subscription_refs: BTreeMap<SubscriptionKey, usize>,
    idle_edge_scope_subscriptions: BTreeMap<SubscriptionKey, u64>,
    /// Completed authority-local aggregate proofs used by terminal commit
    /// admission.  This is intentionally separate from ordinary views.
    authority_scope_proofs: u64,
    announced_catalogue_fingerprint: Option<[u8; 32]>,
    /// Deterministic counters for this peer.
    pub metrics: PeerMetrics,
}

impl Default for PeerState {
    fn default() -> Self {
        Self {
            publication_owner: NEXT_PUBLICATION_OWNER.fetch_add(1, Ordering::Relaxed),
            // The default is a standalone, SYSTEM-scoped peer helper. A real
            // relay can multiplex admitted sessions and must opt into the
            // explicit `PeerState::relay()` role, where every served
            // subscription is required to carry its immutable policy binding.
            role: PeerRole::ClientLink {
                identity: AuthorSubject::SYSTEM,
            },
            permission_identity: None,
            transport_capability: RelayTransportCapability::OrdinarySession,
            shipped_complete_tx_payloads: BTreeSet::new(),
            ship_complete_exclusive_payloads: false,
            publication_states: BTreeMap::new(),
            downstream_known_states: BTreeMap::new(),
            deferred_edge_fates: BTreeMap::new(),
            edge_scope_subscription_refs: BTreeMap::new(),
            idle_edge_scope_subscriptions: BTreeMap::new(),
            authority_scope_proofs: 0,
            announced_catalogue_fingerprint: None,
            metrics: PeerMetrics::default(),
        }
    }
}

/// Closed admission capability for a peer transport.
///
/// A generic relay may multiplex independently admitted bindings. A
/// scope-isolated client relay has exactly one server-authenticated binding,
/// so raw claims and arbitrary delegated request bindings are never accepted.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum RelayTransportCapability {
    OrdinarySession,
    #[allow(dead_code)] // constructed only by the private serving admission path
    ScopeIsolatedClientRelay {
        binding: DelegatedSessionBinding,
        admission_epoch: u64,
    },
    MultiplexedRelay,
}

static NEXT_PUBLICATION_OWNER: AtomicU64 = AtomicU64::new(1);

include!("peer/publication.rs");
include!("peer/known_state.rs");
include!("peer/repair.rs");

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
    /// Maintained subscription view counters and latest index footprint.
    pub maintained_subscription_view: Box<MaintainedSubscriptionViewMetrics>,
}

/// Latest maintained subscription view index sizes observed for one peer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MaintainedSubscriptionViewMetricsFootprint {
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
    /// Active recursive app-row roots retained for structured array output.
    pub structured_app_rows: usize,
    /// Approximate heap bytes retained by result_weights.
    pub result_weights_bytes: usize,
    /// Approximate heap bytes retained by result_payloads.
    pub result_payloads_bytes: usize,
    /// Approximate heap bytes retained by WeightedVersionIndex.
    pub versions_bytes: usize,
    /// Approximate heap bytes retained by ReplacementIndex.
    pub replacements_bytes: usize,
    /// Approximate heap bytes retained by recursive app-row roots.
    pub structured_app_rows_bytes: usize,
    /// Approximate heap bytes retained by maintained-view indexes.
    pub total_heap_bytes: usize,
}

/// Observable maintained subscription view metrics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MaintainedSubscriptionViewMetrics {
    /// Maintained subscription view updates served by the incremental path.
    pub hits_out: u64,
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
            result_weights: footprint.result_weights,
            result_payloads: footprint.result_payloads,
            version_identities: footprint.version_identities,
            version_tx_entries: footprint.version_tx_entries,
            replacement_entries: footprint.replacement_entries,
            structured_app_rows: footprint.structured_app_rows,
            result_weights_bytes: footprint.result_weights_bytes,
            result_payloads_bytes: footprint.result_payloads_bytes,
            versions_bytes: footprint.versions_bytes,
            replacements_bytes: footprint.replacements_bytes,
            structured_app_rows_bytes: footprint.structured_app_rows_bytes,
            total_heap_bytes: footprint.total_heap_bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    include!("peer/tests.rs");
}
