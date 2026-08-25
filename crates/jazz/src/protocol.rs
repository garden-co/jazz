//! Simulation-first wire vocabulary for sync messages, commit payloads, view
//! updates, catalogue messages, and migration lens publication. This module owns
//! serializable shapes that cross node or facade boundaries; storage encoders
//! live in [`crate::node::codec`], transaction semantics in [`crate::tx`], and
//! query AST semantics in [`crate::query`]. It connects the node layer to peers,
//! tests, and the `Db` facade without owning validation or persistence.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

use groove::large_values::Locator;
use groove::records::{OwnedRecord, Value};

use crate::ids::{
    AuthorSubject, MigrationLensId, NodeUuid, RowUuid, SchemaLineagePublicationId, SchemaVersionId,
};
use crate::query::{BindingId, Query, RelationQuery, ShapeId};
use crate::schema::{JazzSchema, TableSchema};
use crate::time::GlobalTime;
use crate::time::TxTime;
use crate::tools::{ObjectId, OutputOccurrenceId, ResultKey};
use crate::tx::{DeletionEvent, DurabilityTier, Fate, Snapshot, Transaction, TxId};

/// Messages exchanged between Jazz nodes.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum SyncMessage {
    /// Auxiliary, non-canonical requests for immutable Groove chunks that are
    /// absent from the receiver's local chunk storage.
    ChunkRequestBatch(ChunkRequestBatch),
    /// Auxiliary responses to a chunk request batch. These carry storage
    /// objects, never row facts or authorization grants.
    ChunkResponseBatch(ChunkResponseBatch),
    /// Trusted backend assertion of process-local auth claims for a write subject.
    SessionClaims {
        /// Identity these claims describe.
        identity: AuthorSubject,
        /// Claims used by policy evaluation for this identity.
        claims: BTreeMap<String, Value>,
    },
    /// Upstream commit unit awaiting authority fate.
    CommitUnit {
        /// Transaction payload.
        tx: Transaction,
        /// Row versions in the commit.
        versions: Vec<VersionRecord>,
    },
    /// Downstream fate update for a transaction.
    FateUpdate {
        /// Transaction being updated.
        tx_id: TxId,
        /// New fate.
        fate: Fate,
        /// Assigned global timestamp, when accepted.
        global_time: Option<crate::time::GlobalTime>,
        /// Sender's observed durability tier, when it chooses to make a claim.
        durability: Option<DurabilityTier>,
    },
    /// Register a query shape.
    RegisterShape {
        /// Content-addressed shape id.
        shape_id: ShapeId,
        /// Versioned AST payload.
        ast: ShapeAst,
        /// Registration options.
        opts: RegisterShapeOptions,
    },
    /// Attach a usage-site subscription to a registered query shape.
    Subscribe(Subscribe),
    /// Reject one usage-site subscription without closing the peer connection.
    SubscribeRejected {
        /// Usage-site subscription that was not accepted.
        subscription: SubscriptionKey,
        /// Stable rejection class plus diagnostic detail.
        reason: SubscribeRejectReason,
    },
    /// Detach a usage-site subscription.
    Unsubscribe {
        /// Usage-site subscription to detach.
        subscription: SubscriptionKey,
    },
    /// Publish an immutable schema-version payload.
    PublishSchema {
        /// Authenticated catalogue admin.
        author: AuthorSubject,
        /// Schema payload.
        schema: Box<SchemaVersion>,
    },
    /// Atomically publish a non-genesis schema with its lineage-defining lens.
    PublishSchemaWithLens {
        /// Authenticated catalogue admin.
        author: AuthorSubject,
        /// Database-wide authoritative catalogue ordering position.
        catalogue_seq: u64,
        /// Complete schema-and-lineage publication bundle.
        publication: Box<SchemaLineagePublication>,
    },
    /// Publish an immutable migration lens payload.
    PublishLens {
        /// Authenticated catalogue admin.
        author: AuthorSubject,
        /// Lens payload.
        lens: MigrationLens,
    },
    /// Set the current write-schema pointer.
    SetCurrentWriteSchema {
        /// Authenticated catalogue admin.
        author: AuthorSubject,
        /// Core-ordered pointer payload.
        pointer: CurrentWriteSchema,
    },
    /// Catalogue-lane acknowledgement.
    CatalogueAck(CatalogueAck),
    /// Downstream current-row view update.
    ViewUpdate(ViewUpdatePayload),
    /// Repair-lane request for exact row-version payloads referenced by known-state dedup.
    FetchRowVersions {
        /// Exact version identities requested by the receiver.
        requests: Vec<RowVersionRef>,
    },
    /// Repair-lane response carrying canonical row-version payloads.
    RowVersionPayloads {
        /// Version bundles visible to the requesting link identity.
        version_bundles: Vec<VersionBundle>,
    },
    /// Trusted upstream catalogue metadata required to decode immutable
    /// authored-version payloads before their view update arrives.
    CatalogueSnapshot(Box<CatalogueSnapshot>),
    /// One-shot permission preflight. The authenticated link identity is the
    /// subject; identity and claims are intentionally absent from the payload.
    PermissionAdviceRequest {
        /// Client-generated opaque id, unique among requests on this live link.
        request_id: PermissionAdviceRequestId,
        /// Hypothetical operation to evaluate without mutation.
        action: PermissionAdviceAction,
    },
    /// One-shot permission preflight result. No supporting rows or denial
    /// reason are carried across this boundary.
    PermissionAdviceResponse {
        /// Opaque id copied from the request.
        request_id: PermissionAdviceRequestId,
        /// Final serving-authority result, or `Unknown` when unavailable.
        advice: PermissionAdvice,
    },
    /// Register and hydrate a support view for one authorization scope.
    ///
    /// Appended to preserve every pre-existing postcard enum discriminant.
    /// This wraps the existing subscription pipeline rather than creating a
    /// second query transport, and is feature-gated for old peers.
    AuthorizationScopeSubscribe {
        /// Ordinary shape/binding subscription carrying the support view.
        subscribe: Subscribe,
        /// Scope and non-secret operation purpose of that support view.
        purpose: AuthorizationScopePurpose,
    },
    /// Authority proof emitted after the matching support `ViewUpdate`.
    AuthorizationScopeReceipt {
        /// Support view that the receiver must apply before accepting proof.
        subscription: SubscriptionKey,
        /// Bound authority receipt.
        receipt: AuthorizationScopeReceipt,
    },
    /// Minimal request for an authority-owned authorization support scope.
    ///
    /// The caller supplies only an opaque correlation id and the hypothetical
    /// action.  In particular it cannot select a shape, binding, scope key, or
    /// authenticated subject.
    AuthorizationScopeIntent {
        /// Opaque request correlation chosen by the client.
        request_id: PermissionAdviceRequestId,
        /// Candidate operation; all support scope details are authority-derived.
        action: PermissionAdviceAction,
    },
    /// One authority-selected support clause for an authorization intent.
    /// `view` is an ordinary `ViewUpdate`, wrapped only to carry its opaque
    /// request and server-chosen scope metadata.
    AuthorizationScopeView {
        /// Opaque request correlation from the matching intent.
        request_id: PermissionAdviceRequestId,
        /// Authority-derived scope identity.
        key: AuthorizationSupportScopeKey,
        /// Zero-based authority clause ordinal.
        clause_index: u16,
        /// Total number of authority clauses in this hydration.
        clause_count: u16,
        /// Ordinary settlement-bearing `ViewUpdate` payload.
        ///
        /// This deliberately is not a `SyncMessage`: an authority scope view
        /// has exactly one wrapper around a view update, never another scope
        /// wrapper.
        view: ViewUpdatePayload,
    },
    /// Aggregate proof for every clause sent in an authority scope view set.
    AuthorizationScopeAggregateReceipt {
        /// Opaque request correlation from the matching intent.
        request_id: PermissionAdviceRequestId,
        /// Aggregate authority proof after every clause view.
        receipt: AuthorizationScopeReceipt,
    },
    /// The admitted authority cannot currently hydrate an intent.  This is a
    /// conservative terminal result: clients resolve the corresponding advice
    /// as `Unknown` and park normal authority work.
    AuthorizationScopeUnavailable {
        /// Opaque request correlation from the matching intent.
        request_id: PermissionAdviceRequestId,
    },
    /// Decision for an action with no policy-support clauses.  It deliberately
    /// carries no support rows, shape identifiers, or binding identifiers.
    AuthorizationScopeDecision {
        /// Opaque request correlation from the matching intent.
        request_id: PermissionAdviceRequestId,
        /// Final authority result for the zero-support action.
        advice: PermissionAdvice,
    },
    /// Begin a root-first push upload with its complete immutable descriptor.
    ChunkUploadStart(ChunkUploadStart),
    /// Supply one bounded set of nodes requested by the receiver.
    ChunkUploadNodes(ChunkUploadNodes),
    /// Receiver acknowledgement for a pushed upload.
    ChunkUploadResult(ChunkUploadResult),
}

/// Shared payload for ordinary and authorization-scope view updates.
///
/// [`SyncMessage::ViewUpdate`] carries it directly, while
/// [`SyncMessage::AuthorizationScopeView`] adds scope metadata around the same
/// payload. Keeping the payload separate from `SyncMessage` makes recursive
/// authorization-scope wrappers impossible to construct and decode.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ViewUpdatePayload {
    /// Target subscription whose result set this update changes.
    pub subscription: SubscriptionKey,
    /// Authority cut through which this view has settled.
    pub settled_through: GlobalTime,
    /// Whether the receiver must replace its current result membership.
    pub reset_result_set: bool,
    /// Compact carriers for versions referenced by this update.
    pub version_carriers: Vec<VersionCarrier>,
    /// Explicit version bundles required by this update.
    pub version_bundles: Vec<VersionBundle>,
    /// Per-peer payload coverage and authorization progress.
    pub peer_payload_inventory: PeerPayloadInventory,
    /// Result members added by the update.
    pub result_member_adds: Vec<ResultMemberEntry>,
    /// Result members removed by the update.
    pub result_member_removes: Vec<ResultMemberEntry>,
    /// Terminal-owned structural result edits.
    pub terminal_operations: Vec<groove::ivm::TerminalOperation>,
    /// Program facts added by this update.
    pub program_fact_adds: Vec<ProgramFactEntry>,
    /// Program facts removed by this update.
    pub program_fact_removes: Vec<ProgramFactEntry>,
}

impl ViewUpdatePayload {
    /// Extracts the shared payload from an ordinary view-update message.
    pub fn from_view_update(message: SyncMessage) -> Option<Self> {
        match message {
            SyncMessage::ViewUpdate(payload) => Some(payload),
            _ => None,
        }
    }

    /// Wraps this payload in an ordinary view-update message.
    pub fn into_view_update(self) -> SyncMessage {
        SyncMessage::ViewUpdate(self)
    }
}

/// Bounded batch of exact immutable-chunk requests on one peer link.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ChunkRequestBatch {
    /// Exact requests coalesced for one transport frame.
    #[serde(deserialize_with = "deserialize_chunk_requests")]
    pub requests: Vec<ChunkRequestEntry>,
}

fn deserialize_chunk_requests<'de, D>(deserializer: D) -> Result<Vec<ChunkRequestEntry>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct ChunkRequestsVisitor;

    impl<'de> serde::de::Visitor<'de> for ChunkRequestsVisitor {
        type Value = Vec<ChunkRequestEntry>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(
                formatter,
                "at most {} chunk requests",
                crate::protocol_limits::MAX_CHUNK_REQUEST_BATCH_ENTRIES
            )
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let declared = sequence.size_hint();
            if declared.is_some_and(|count| {
                count > crate::protocol_limits::MAX_CHUNK_REQUEST_BATCH_ENTRIES
            }) {
                return Err(<A::Error as serde::de::Error>::custom(
                    "chunk request batch exceeds cardinality limit",
                ));
            }
            let mut requests = Vec::with_capacity(
                declared
                    .unwrap_or_default()
                    .min(crate::protocol_limits::MAX_CHUNK_REQUEST_BATCH_ENTRIES),
            );
            while let Some(request) = sequence.next_element()? {
                if requests.len() >= crate::protocol_limits::MAX_CHUNK_REQUEST_BATCH_ENTRIES {
                    return Err(<A::Error as serde::de::Error>::custom(
                        "chunk request batch exceeds cardinality limit",
                    ));
                }
                requests.push(request);
            }
            Ok(requests)
        }
    }

    deserializer.deserialize_seq(ChunkRequestsVisitor)
}

/// One hop-local request. `remaining_hops` is decremented before forwarding.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ChunkRequestEntry {
    /// Identifier meaningful only on this peer hop.
    pub request_id: u64,
    /// Exact random 256-bit retrieval capability. Storage adapters derive
    /// their private key layout from this value internally.
    pub locator: Locator,
    /// Hash Groove must verify before accepting returned bytes.
    pub expected_hash: [u8; 32],
    /// Maximum remaining forwarding edges.
    pub remaining_hops: u8,
}

/// Bounded batch of replies addressed by the request id allocated on this hop.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ChunkResponseBatch {
    /// Replies coalesced for one transport frame.
    pub responses: Vec<ChunkResponseEntry>,
}

/// One hop-local immutable-chunk reply.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ChunkResponseEntry {
    /// Request identifier allocated by the receiver of this response.
    pub request_id: u64,
    /// Storage result for the exact requested locator and hash.
    pub result: ChunkResponse,
}

/// Result of one auxiliary immutable-chunk request.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ChunkResponse {
    /// Exact stored bytes; Groove still verifies the expected hash.
    Found(Vec<u8>),
    /// This route cannot supply the requested object.
    Unavailable,
    /// The route may become available after the suggested delay.
    Retryable {
        /// Minimum suggested retry delay.
        retry_after_ms: u32,
    },
}

/// Root-first upload announcement. The descriptor, already carried by the
/// later transaction, is the protocol identity.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ChunkUploadStart {
    /// Exact immutable value whose root the receiver checks first.
    pub value_ref: groove::large_values::LargeValueRef,
}

/// A bounded collection of receiver-requested immutable Groove nodes.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ChunkUploadNodes {
    /// Descriptor whose current missing frontier requested these nodes.
    pub value_ref: groove::large_values::LargeValueRef,
    /// Authenticated immutable nodes, bounded by the semantic frame limit.
    pub chunks: Vec<groove::large_values::StagedChunk>,
}

/// Hop-local upload outcome. A rejected upload must be retried from its first
/// batch; no referencing row may be sent before `Staged`.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ChunkUploadResult {
    /// Descriptor whose derived receiver state changed.
    pub value_ref: groove::large_values::LargeValueRef,
    /// Current receiver outcome.
    pub status: ChunkUploadStatus,
}

/// Receiver outcome for one push-upload step.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ChunkUploadStatus {
    /// Authenticated nodes still absent from local Groove storage.
    Need(Vec<groove::large_values::NodeRef>),
    /// Groove derived graph closure and created a persisted retainer claim.
    Staged,
    /// Jazz rejected incoming bytes under its deployment policy.
    RateLimited,
    /// The descriptor or a supplied node was invalid.
    Rejected,
}

/// Opaque identity for one permission-advice exchange.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct PermissionAdviceRequestId(pub [u8; 16]);

/// Hypothetical operation sent to a trusted serving authority.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum PermissionAdviceAction {
    /// Insert a candidate row.
    Insert {
        /// Table name.
        table: String,
        /// Candidate cells after client-side value encoding.
        cells: BTreeMap<String, Value>,
    },
    /// Read one current row.
    Read {
        /// Table name.
        table: String,
        /// Row id.
        row: RowUuid,
    },
    /// Update one current row.
    Update {
        /// Table name.
        table: String,
        /// Row id.
        row: RowUuid,
        /// Candidate patch. It is carried for forward-compatible exact
        /// update-check evaluation and is never echoed in the response.
        patch: BTreeMap<String, Value>,
    },
    /// Delete one current row.
    Delete {
        /// Table name.
        table: String,
        /// Row id.
        row: RowUuid,
    },
}

/// The policy operation proven by an authorization-scope receipt.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize,
)]
pub enum AuthorizationScopeOperation {
    /// Read-policy evaluation.
    Read,
    /// Insert-check evaluation.
    Insert,
    /// Update-using and update-check evaluation.
    Update,
    /// Delete-using evaluation.
    Delete,
}

/// Stable, action-specific identity for one authority-hydrated policy scope.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize)]
pub struct AuthorizationSupportScopeKey {
    /// Compiled policy support shape, including the selected operation clause.
    pub support_shape_digest: [u8; 32],
    /// Authenticated subject, never caller-supplied on a serving link.
    pub subject: AuthorSubject,
    /// Canonical digest of authenticated claims.
    pub claims_digest: [u8; 32],
    /// Compiled policy shape and selected policy epoch.
    pub policy_digest: [u8; 32],
}

/// Ephemeral candidate-specific key used only for final evaluation.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct AuthorizationOperationKey {
    /// Exact operation to evaluate.
    pub operation: AuthorizationScopeOperation,
    /// Protected table.
    pub table: String,
    /// Target row when applicable.
    pub row: Option<RowUuid>,
    /// Canonical candidate/patch digest when applicable.
    pub candidate_digest: [u8; 32],
}

/// Minimal caller intent for a regular subscription opened as authorization
/// support. The authority derives the scope key and operation itself from this
/// intent, its authenticated link identity, and the registered shape/binding.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct AuthorizationScopePurpose {
    /// Candidate operation whose policy support is being hydrated.
    pub action: PermissionAdviceAction,
}

/// Authority-issued receipt proving one scope was hydrated through its stated cut.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct AuthorizationScopeReceipt {
    /// Scope covered by this receipt.
    pub key: AuthorizationSupportScopeKey,
    /// Authenticated authority identity that issued this receipt.
    pub authority: [u8; 16],
    /// Authenticated subscriber link to which this proof is restricted.
    pub link: AuthorSubject,
    /// Authority-local connection/process epoch.
    pub authority_epoch: u64,
    /// Authenticated-claims revision paired with this proof.
    pub claims_revision: u64,
    /// Policy/schema epoch used to compile the scope.
    pub policy_epoch: u64,
    /// Complete authoritative history cut reflected by the support view. Like
    /// other `settled_through` cursors, this is durable history evidence; the
    /// separately scoped authority epoch supplies connection liveness.
    pub settled_through: GlobalTime,
    /// Authority authorization generation paired with that cut.
    pub authorization_progress: u64,
}

/// Advisory result of a permission preflight.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum PermissionAdvice {
    /// A trusted serving authority determined that the operation is allowed.
    Allowed,
    /// A trusted serving authority determined that the operation is denied.
    Denied,
    /// No trusted serving authority produced a final decision.
    Unknown,
}

/// Ordered schema lineage metadata shipped ahead of authored row payloads.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct CatalogueSnapshot {
    /// Every immutable schema payload known to the sender.
    pub schemas: Vec<SchemaVersion>,
    /// Active non-genesis lineage publications in catalogue order.
    pub lineages: Vec<(u64, SchemaLineagePublication)>,
    /// Sender's active write-schema pointer.
    pub current_write_schema: CurrentWriteSchema,
}

impl SyncMessage {
    /// Optional wire capabilities required to serialize this semantic message.
    ///
    /// Kept on the semantic type so every codec caller uses one exhaustive
    /// classification rather than accidentally sending a future enum variant
    /// to an older peer.
    pub fn required_wire_features(&self) -> crate::wire::WireFeatures {
        match self {
            Self::AuthorizationScopeSubscribe { .. } | Self::AuthorizationScopeReceipt { .. } => {
                crate::wire::FEATURE_AUTHORIZATION_SCOPE_RECEIPTS
            }
            Self::AuthorizationScopeIntent { .. }
            | Self::AuthorizationScopeView { .. }
            | Self::AuthorizationScopeAggregateReceipt { .. }
            | Self::AuthorizationScopeUnavailable { .. }
            | Self::AuthorizationScopeDecision { .. } => {
                crate::wire::FEATURE_AUTHORIZATION_SCOPE_VIEWS
            }
            Self::ChunkRequestBatch(_)
            | Self::ChunkResponseBatch(_)
            | Self::ChunkUploadStart(_)
            | Self::ChunkUploadNodes(_)
            | Self::ChunkUploadResult(_) => crate::wire::FEATURE_AUXILIARY_CHUNKS,
            _ => crate::wire::FEATURE_NONE,
        }
    }

    /// Validate any packed view-update carrier runs in this message.
    pub fn validate_version_carriers(&self) -> Result<(), VersionBundleRunError> {
        self.carried_view_update().map_or(Ok(()), |view| {
            validate_version_carrier_runs(&view.version_carriers)
        })
    }

    fn carried_view_update(&self) -> Option<&ViewUpdatePayload> {
        match self {
            Self::ViewUpdate(view) | Self::AuthorizationScopeView { view, .. } => Some(view),
            _ => None,
        }
    }

    /// Expand packed view-update carriers into `version_bundles` for legacy paths/tests.
    pub fn expand_version_carriers_for_receive(mut self) -> Result<Self, VersionBundleRunError> {
        if let Self::ViewUpdate(ViewUpdatePayload {
            version_carriers,
            version_bundles,
            ..
        }) = &mut self
        {
            version_bundles.extend(expand_version_carriers(version_carriers)?);
            version_carriers.clear();
        }
        Ok(self)
    }
}

fn validate_version_carrier_runs(
    version_carriers: &[VersionCarrier],
) -> Result<(), VersionBundleRunError> {
    for carrier in version_carriers {
        if let VersionCarrier::Run(run) = carrier {
            run.validate()?;
        }
    }
    Ok(())
}

/// Exact row-version identity used by known-state repair requests.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize)]
pub struct RowVersionRef {
    /// Table containing the row.
    pub table: groove::Intern<String>,
    /// Stable row id.
    pub row_uuid: RowUuid,
    /// Transaction HLC time.
    pub tx_time: TxTime,
    /// Transaction node id in wire form.
    pub tx_node_id: NodeUuid,
}

impl RowVersionRef {
    /// Construct an exact row-version reference.
    pub fn new(table: impl Into<String>, row_uuid: RowUuid, tx_id: TxId) -> Self {
        Self {
            table: groove::Intern::new(table.into()),
            row_uuid,
            tx_time: tx_id.time,
            tx_node_id: tx_id.node,
        }
    }

    /// Transaction id addressed by this row-version reference.
    pub fn tx_id(&self) -> TxId {
        TxId::new(self.tx_time, self.tx_node_id)
    }
}

/// Payload coverage that the sender believes the peer already has.
///
/// This inventory is peer-scoped, not subscription-scoped. Today it can only
/// reference full transaction payloads; future tiers can add row-version and
/// view-complete coverage without overloading tx-level knowledge.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct PeerPayloadInventory {
    /// Transactions whose full version payload has already been shipped to the
    /// peer and can be referenced by tx id. Partial mergeable or view-scoped
    /// exclusive coverage must remain explicit `VersionBundle` payloads until
    /// the wire protocol grows finer-grained inventory refs.
    pub complete_tx_payloads: Vec<TxId>,
    /// Server-stamped authorization generation for the binding view carried by
    /// this update. It qualifies a subsequent fast known-state declaration.
    pub authorization_progress: Option<u64>,
    /// Authority explicitly published a fail-closed opening snapshot while
    /// authorization coverage is unavailable. This snapshot is observable but
    /// remains pending until a later authoritative reset.
    #[serde(default)]
    pub opening_pending: bool,
}

/// One immutable row-version payload carried by a committed transaction.
///
/// The record serializes as `(table, bytes)`; the receiver resolves the wire
/// descriptor from its local schema by table name. v0 requires sender and
/// receiver descriptors to match exactly. Schema changes therefore require a
/// protocol/schema negotiation layer before mixed-version sync.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct VersionRecord {
    table: groove::Intern<String>,
    schema_version: SchemaVersionId,
    /// Exact branch coordinate of this version's branch-local row.
    #[serde(default)]
    branch_key: BranchKey,
    record: OwnedRecord,
    /// `None` denotes a legacy or lens-translated payload whose authored
    /// presence is unavailable; consumers must conservatively treat every
    /// present payload cell as authored.
    authored_columns: Option<BTreeSet<String>>,
}

impl VersionRecord {
    /// Construct a wire version record from encoded bytes and the table schema.
    pub fn new(
        table: impl Into<String>,
        schema_version: SchemaVersionId,
        record: OwnedRecord,
    ) -> Self {
        Self {
            table: groove::Intern::new(table.into()),
            schema_version,
            branch_key: BranchKey::default(),
            record,
            authored_columns: None,
        }
    }

    pub(crate) fn with_branch_key(mut self, branch_key: BranchKey) -> Self {
        self.branch_key = branch_key;
        self
    }

    /// Exact branch coordinate of this version.
    pub fn branch_key(&self) -> &BranchKey {
        &self.branch_key
    }

    pub(crate) fn with_authored_columns(
        mut self,
        authored_columns: Option<BTreeSet<String>>,
    ) -> Self {
        self.authored_columns = authored_columns;
        self
    }

    pub(crate) fn authored_columns(&self) -> Option<&BTreeSet<String>> {
        self.authored_columns.as_ref()
    }

    /// Encode a wire record directly from typed row payload parts.
    pub fn encode(
        table: &TableSchema,
        schema_version: SchemaVersionId,
        row_uuid: RowUuid,
        parents: Vec<TxId>,
        created_by: AuthorSubject,
        created_at_ms: u64,
        updated_by: AuthorSubject,
        updated_at_ms: u64,
        cells_by_position: &[Option<Value>],
        deletion: Option<DeletionEvent>,
    ) -> Result<Self, groove::records::Error> {
        // This path is for data birth only; stored rows project to wire bytes without decoding.
        let descriptor = table.wire_record_descriptor();
        let values = [
            Value::Uuid(row_uuid.0),
            Value::Array(parents.into_iter().map(tx_id_value).collect()),
            Value::String(created_by.canonical().to_owned()),
            Value::U64(created_at_ms),
            Value::String(updated_by.canonical().to_owned()),
            Value::U64(updated_at_ms),
            Value::Nullable(deletion.map(|deletion| {
                Box::new(Value::EnumTag(match deletion {
                    DeletionEvent::Deleted => 0,
                    DeletionEvent::Restored => 1,
                }))
            })),
        ]
        .into_iter()
        .chain(table.columns.iter().enumerate().map(|(idx, _column)| {
            Value::Nullable(
                cells_by_position
                    .get(idx)
                    .and_then(Clone::clone)
                    .map(Box::new),
            )
        }))
        .collect::<Vec<_>>();
        let raw = descriptor.create(&values)?;
        Ok(Self::new(
            table.name.clone(),
            schema_version,
            OwnedRecord::new(raw, descriptor),
        ))
    }

    /// Encode a wire record from cells keyed by application column name.
    pub fn from_cells<V: Into<Value> + Clone>(
        table: &TableSchema,
        schema_version: SchemaVersionId,
        row_uuid: RowUuid,
        parents: Vec<TxId>,
        created_by: AuthorSubject,
        created_at_ms: u64,
        updated_by: AuthorSubject,
        updated_at_ms: u64,
        cells: &BTreeMap<String, V>,
        deletion: Option<DeletionEvent>,
    ) -> Result<Self, groove::records::Error> {
        let positional = table
            .columns
            .iter()
            .map(|column| cells.get(&column.name).cloned().map(Into::into))
            .collect::<Vec<_>>();
        Self::encode(
            table,
            schema_version,
            row_uuid,
            parents,
            created_by,
            created_at_ms,
            updated_by,
            updated_at_ms,
            &positional,
            deletion,
        )
    }

    /// Table containing the row.
    pub fn table(&self) -> &str {
        self.table.as_str()
    }

    /// Schema version used to encode this row payload.
    pub fn schema_version(&self) -> SchemaVersionId {
        self.schema_version
    }

    /// Encoded wire record.
    pub fn record(&self) -> &OwnedRecord {
        &self.record
    }

    /// Stable row identity.
    pub fn row_uuid(&self) -> RowUuid {
        RowUuid(
            self.record
                .borrowed()
                .get_uuid(WireRowRecord::FIELD_ROW_UUID_IDX)
                .expect("valid wire row uuid"),
        )
    }

    /// Direct parent transaction ids.
    pub fn parents(&self) -> Vec<TxId> {
        tx_ids_from_value(
            self.record
                .borrowed()
                .get_idx(WireRowRecord::FIELD_PARENTS_IDX)
                .expect("valid wire parents"),
        )
        .expect("valid wire parents")
    }

    /// Deletion-register event, if any.
    pub fn deletion(&self) -> Option<DeletionEvent> {
        deletion_from_value(
            self.record
                .borrowed()
                .get_idx(WireRowRecord::FIELD__DELETION_IDX)
                .expect("valid wire deletion"),
        )
        .expect("valid wire deletion")
    }

    /// Original author for this logical row.
    pub fn created_by(&self) -> AuthorSubject {
        AuthorSubject::from_canonical(
            self.record
                .borrowed()
                .get_str(WireRowRecord::FIELD_CREATED_BY_IDX)
                .expect("valid wire created_by"),
        )
        .expect("canonical wire created_by")
    }

    /// Original creation timestamp for this logical row in Unix milliseconds.
    pub fn created_at_ms(&self) -> u64 {
        self.record
            .borrowed()
            .get_u64(WireRowRecord::FIELD_CREATED_AT_IDX)
            .expect("valid wire created_at_ms")
    }

    /// Author of this row version.
    pub fn updated_by(&self) -> AuthorSubject {
        AuthorSubject::from_canonical(
            self.record
                .borrowed()
                .get_str(WireRowRecord::FIELD_UPDATED_BY_IDX)
                .expect("valid wire updated_by"),
        )
        .expect("canonical wire updated_by")
    }

    /// Update timestamp for this row version in Unix milliseconds.
    pub fn updated_at_ms(&self) -> u64 {
        self.record
            .borrowed()
            .get_u64(WireRowRecord::FIELD_UPDATED_AT_IDX)
            .expect("valid wire updated_at_ms")
    }

    /// Cell value by application-schema column position.
    pub fn cell_at(&self, column_position: usize) -> Option<Value> {
        self.record
            .borrowed()
            .get_idx(WireRowRecord::USER_CELLS + column_position)
            .expect("valid wire cell")
            .nullable_value()
            .expect("valid nullable wire cell")
    }

    /// Cell value by application-schema column position, treating columns not
    /// present in the wire payload as absent.
    pub(crate) fn optional_cell_at(&self, column_position: usize) -> Option<Value> {
        let field = WireRowRecord::USER_CELLS + column_position;
        if field >= self.record.descriptor().fields().len() {
            return None;
        }
        self.record
            .borrowed()
            .get_idx(field)
            .ok()?
            .nullable_value()
            .ok()
            .flatten()
    }

    pub(crate) fn application_cell_count(&self) -> usize {
        self.record
            .descriptor()
            .fields()
            .len()
            .saturating_sub(WireRowRecord::USER_CELLS)
    }
}

trait NullableValue {
    fn nullable_value(self) -> Result<Option<Value>, &'static str>;
}

impl NullableValue for Value {
    fn nullable_value(self) -> Result<Option<Value>, &'static str> {
        match self {
            Value::Nullable(None) => Ok(None),
            Value::Nullable(Some(value)) => Ok(Some(*value)),
            _ => Err("nullable value expected"),
        }
    }
}

impl PartialOrd for VersionRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for VersionRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        self.table()
            .cmp(other.table())
            .then_with(|| self.schema_version.cmp(&other.schema_version))
            .then_with(|| self.branch_key.cmp(&other.branch_key))
            .then_with(|| self.record.raw().cmp(other.record.raw()))
            .then_with(|| self.authored_columns.cmp(&other.authored_columns))
    }
}

groove::define_record! {
    struct WireRowRecord {
        0 => row_uuid: RowUuid,
        1 => parents: ParentRefs,
        2 => created_by: AuthorSubject,
        3 => created_at: u64,
        4 => updated_by: AuthorSubject,
        5 => updated_at: u64,
        6 => _deletion: Option<Value>,
        .. user_cells,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ParentRefs(Vec<TxId>);

impl groove::records::RecordField for ParentRefs {
    fn read(
        record: &groove::records::BorrowedRecord<'_>,
        idx: usize,
    ) -> Result<Self, groove::records::Error> {
        tx_ids_from_value(record.get_idx(idx)?)
            .map(Self)
            .map_err(|_| groove::records::Error::TypeMismatch {
                expected: groove::records::ValueType::Array(Box::new(
                    groove::records::ValueType::Tuple(vec![
                        groove::records::ValueType::U64,
                        groove::records::ValueType::Uuid,
                    ]),
                )),
            })
    }

    fn to_value(&self) -> Value {
        Value::Array(self.0.iter().map(|parent| tx_id_value(*parent)).collect())
    }

    const COLUMN_KIND: groove::records::FieldKind = groove::records::FieldKind::Array;
}

fn tx_ids_from_value(value: Value) -> Result<Vec<TxId>, &'static str> {
    match value {
        Value::Array(values) => values.into_iter().map(tx_id_from_value).collect(),
        _ => Err("parents"),
    }
}

fn tx_id_from_value(value: Value) -> Result<TxId, &'static str> {
    match value {
        Value::Tuple(values) if values.len() == 2 => {
            let mut values = values.into_iter();
            let Value::U64(time) = values.next().expect("len checked") else {
                return Err("tx id time");
            };
            let Value::Uuid(node) = values.next().expect("len checked") else {
                return Err("tx id node");
            };
            Ok(TxId::new(TxTime(time), NodeUuid(node)))
        }
        _ => Err("tx id tuple"),
    }
}

fn tx_id_value(tx_id: TxId) -> Value {
    Value::Tuple(vec![Value::U64(tx_id.time.0), Value::Uuid(tx_id.node.0)])
}

fn deletion_from_value(value: Value) -> Result<Option<DeletionEvent>, &'static str> {
    match value {
        Value::Nullable(None) => Ok(None),
        Value::Nullable(Some(value)) => match *value {
            Value::EnumTag(0) => Ok(Some(DeletionEvent::Deleted)),
            Value::EnumTag(1) => Ok(Some(DeletionEvent::Restored)),
            _ => Err("deletion"),
        },
        _ => Err("deletion"),
    }
}

/// Transaction plus row-version payload and the upstream state observed with it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum VersionBundleScope {
    /// The bundle carries every write authored by the transaction.
    CompleteTransaction,
    /// The bundle carries only the writes admitted by one selected view.
    ///
    /// For this scope `Transaction::n_total_writes` is deliberately redacted to
    /// the number of versions in this bundle. It is not the authored transaction
    /// cardinality and MUST NOT establish complete-payload coverage.
    #[default]
    ViewScoped,
}

/// Transaction plus row-version payload and the upstream state observed with it.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct VersionBundle {
    /// Transaction payload for the versions.
    pub tx: Transaction,
    /// Row versions carried by the transaction.
    pub versions: Vec<VersionRecord>,
    /// Whether the payload is complete or selected for one view.
    pub scope: VersionBundleScope,
    /// Fate known when the bundle was shipped.
    pub fate: Fate,
    /// Global timestamp known when shipped.
    pub global_time: Option<GlobalTime>,
    /// Durability known when shipped.
    pub durability: DurabilityTier,
}

/// Borrowed view of one version-bundle carrier body.
#[derive(Clone, Copy, Debug)]
pub struct VersionBundleRef<'a> {
    /// Transaction payload for the versions.
    pub tx: &'a Transaction,
    /// Row versions carried by the transaction.
    pub versions: &'a [VersionRecord],
    /// Whether the payload is complete or selected for one view.
    pub scope: VersionBundleScope,
    /// Fate known when the bundle was shipped.
    pub fate: &'a Fate,
    /// Global timestamp known when shipped.
    pub global_time: Option<GlobalTime>,
    /// Durability known when shipped.
    pub durability: DurabilityTier,
}

impl<'a> VersionBundleRef<'a> {
    /// Materialize this borrowed view into the legacy owned bundle shape.
    pub fn to_owned_bundle(self) -> VersionBundle {
        VersionBundle {
            tx: self.tx.clone(),
            versions: self.versions.to_vec(),
            scope: self.scope,
            fate: self.fate.clone(),
            global_time: self.global_time,
            durability: self.durability,
        }
    }
}

impl VersionBundle {
    /// Borrow this singleton bundle as a carrier body.
    pub fn as_ref(&self) -> VersionBundleRef<'_> {
        VersionBundleRef {
            tx: &self.tx,
            versions: &self.versions,
            scope: self.scope,
            fate: &self.fate,
            global_time: self.global_time,
            durability: self.durability,
        }
    }
}

/// One row-version carrier in a view-update stream.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum VersionCarrier {
    /// Existing singleton carrier. This is semantically a run of length 1.
    Bundle(VersionBundle),
    /// Packed run of adjacent row-version bodies with shared defaults.
    Run(VersionBundleRun),
}

impl VersionCarrier {
    /// Expand this carrier to the singleton bundle form used by L1 apply paths.
    pub fn expand(&self) -> Result<Vec<VersionBundle>, VersionBundleRunError> {
        match self {
            Self::Bundle(bundle) => Ok(vec![bundle.clone()]),
            Self::Run(run) => run.expand(),
        }
    }

    /// Borrow this carrier as one or more bundle bodies without expanding.
    pub fn bundle_refs(&self) -> Result<Vec<VersionBundleRef<'_>>, VersionBundleRunError> {
        match self {
            Self::Bundle(bundle) => Ok(vec![bundle.as_ref()]),
            Self::Run(run) => run.bundle_refs(),
        }
    }
}

/// Shared header plus row-version bodies for adjacent view-update carriers.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct VersionBundleRun {
    /// Shared/default metadata for the run.
    pub header: VersionBundleRunHeader,
    /// Packed row-version bodies. Each body expands to one `VersionBundle`.
    pub bodies: Vec<VersionBundleRunBody>,
    /// Per-body metadata overrides for rows that deviate from the header.
    pub overrides: Vec<VersionBundleRunOverride>,
}

impl VersionBundleRun {
    /// Build one run from adjacent singleton bundles.
    pub fn from_adjacent_singletons(
        bundles: &[VersionBundle],
    ) -> Result<Self, VersionBundleRunError> {
        let Some(first) = bundles.first() else {
            return Err(VersionBundleRunError::EmptyRun);
        };
        let table = common_run_table(bundles);
        let bodies = bundles
            .iter()
            .map(|bundle| VersionBundleRunBody {
                versions: bundle.versions.clone(),
            })
            .collect::<Vec<_>>();
        let overrides = bundles
            .iter()
            .enumerate()
            .filter_map(|(index, bundle)| {
                let override_ = VersionBundleRunOverride {
                    body_index: index as u32,
                    tx: (bundle.tx != first.tx).then(|| bundle.tx.clone()),
                    scope: (bundle.scope != first.scope).then_some(bundle.scope),
                    fate: (bundle.fate != first.fate).then(|| bundle.fate.clone()),
                    global_time: (bundle.global_time != first.global_time)
                        .then_some(bundle.global_time),
                    durability: (bundle.durability != first.durability)
                        .then_some(bundle.durability),
                };
                override_.has_overrides().then_some(override_)
            })
            .collect::<Vec<_>>();
        let run = Self {
            header: VersionBundleRunHeader {
                table,
                tx: first.tx.clone(),
                scope: first.scope,
                body_count: bodies.len() as u32,
                fate: first.fate.clone(),
                global_time: first.global_time,
                durability: first.durability,
            },
            bodies,
            overrides,
        };
        run.validate()?;
        Ok(run)
    }

    /// Validate metadata that cannot be enforced by postcard shape decoding.
    pub fn validate(&self) -> Result<(), VersionBundleRunError> {
        let declared = self.header.body_count as usize;
        let actual = self.bodies.len();
        if declared == 0 {
            return Err(VersionBundleRunError::EmptyRun);
        }
        if declared != actual {
            return Err(VersionBundleRunError::BodyCountMismatch { declared, actual });
        }

        let mut seen = BTreeSet::new();
        for override_ in &self.overrides {
            let index = override_.body_index as usize;
            if index >= declared {
                return Err(VersionBundleRunError::OverrideIndexOutOfRange {
                    index,
                    body_count: declared,
                });
            }
            if !seen.insert(index) {
                return Err(VersionBundleRunError::DuplicateOverride { index });
            }
        }

        if let Some(table) = &self.header.table {
            for body in &self.bodies {
                for version in &body.versions {
                    if version.table() != table.as_str() {
                        return Err(VersionBundleRunError::TableMismatch {
                            expected: table.to_string(),
                            actual: version.table().to_owned(),
                        });
                    }
                }
            }
        }

        Ok(())
    }

    /// Expand the run into today's singleton `VersionBundle` carriers.
    pub fn expand(&self) -> Result<Vec<VersionBundle>, VersionBundleRunError> {
        self.validate()?;
        let mut overrides = BTreeMap::new();
        for override_ in &self.overrides {
            overrides.insert(override_.body_index as usize, override_);
        }

        Ok(self
            .bodies
            .iter()
            .enumerate()
            .map(|(index, body)| {
                let override_ = overrides.get(&index).copied();
                VersionBundle {
                    tx: override_
                        .and_then(|override_| override_.tx.clone())
                        .unwrap_or_else(|| self.header.tx.clone()),
                    versions: body.versions.clone(),
                    scope: override_
                        .and_then(|override_| override_.scope)
                        .unwrap_or(self.header.scope),
                    fate: override_
                        .and_then(|override_| override_.fate.clone())
                        .unwrap_or_else(|| self.header.fate.clone()),
                    global_time: override_
                        .and_then(|override_| override_.global_time)
                        .unwrap_or(self.header.global_time),
                    durability: override_
                        .and_then(|override_| override_.durability)
                        .unwrap_or(self.header.durability),
                }
            })
            .collect())
    }

    /// Borrow the run bodies with header defaults and body overrides applied.
    pub fn bundle_refs(&self) -> Result<Vec<VersionBundleRef<'_>>, VersionBundleRunError> {
        self.validate()?;
        let mut overrides = BTreeMap::new();
        for override_ in &self.overrides {
            overrides.insert(override_.body_index as usize, override_);
        }

        Ok(self
            .bodies
            .iter()
            .enumerate()
            .map(|(index, body)| {
                let override_ = overrides.get(&index).copied();
                VersionBundleRef {
                    tx: override_
                        .and_then(|override_| override_.tx.as_ref())
                        .unwrap_or(&self.header.tx),
                    versions: &body.versions,
                    scope: override_
                        .and_then(|override_| override_.scope)
                        .unwrap_or(self.header.scope),
                    fate: override_
                        .and_then(|override_| override_.fate.as_ref())
                        .unwrap_or(&self.header.fate),
                    global_time: override_
                        .and_then(|override_| override_.global_time)
                        .unwrap_or(self.header.global_time),
                    durability: override_
                        .and_then(|override_| override_.durability)
                        .unwrap_or(self.header.durability),
                }
            })
            .collect())
    }
}

/// Shared/default metadata for a packed version-bundle run.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct VersionBundleRunHeader {
    /// Shared table context when every carried row-version belongs to one table.
    pub table: Option<groove::Intern<String>>,
    /// Default transaction payload for each body.
    pub tx: Transaction,
    /// Default payload scope for each body.
    pub scope: VersionBundleScope,
    /// Declared number of bodies; must match `VersionBundleRun::bodies`.
    pub body_count: u32,
    /// Default fate for each body.
    pub fate: Fate,
    /// Default global timestamp for each body.
    pub global_time: Option<GlobalTime>,
    /// Default durability tier for each body.
    pub durability: DurabilityTier,
}

/// Row-version payload body inside a packed run.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct VersionBundleRunBody {
    /// Row versions carried by this body.
    pub versions: Vec<VersionRecord>,
}

/// Per-body override for metadata that differs from the run header.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct VersionBundleRunOverride {
    /// Zero-based index into `VersionBundleRun::bodies`.
    pub body_index: u32,
    /// Transaction override for this body.
    pub tx: Option<Transaction>,
    /// Payload-scope override for this body.
    pub scope: Option<VersionBundleScope>,
    /// Fate override for this body.
    pub fate: Option<Fate>,
    /// Global timestamp override for this body. `Some(None)` overrides to absent.
    pub global_time: Option<Option<GlobalTime>>,
    /// Durability override for this body.
    pub durability: Option<DurabilityTier>,
}

impl VersionBundleRunOverride {
    fn has_overrides(&self) -> bool {
        self.tx.is_some()
            || self.scope.is_some()
            || self.fate.is_some()
            || self.global_time.is_some()
            || self.durability.is_some()
    }
}

/// Validation failures for malformed packed version-bundle runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VersionBundleRunError {
    /// Runs must carry at least one body.
    EmptyRun,
    /// The declared body count did not match the actual body vector length.
    BodyCountMismatch {
        /// Header-declared body count.
        declared: usize,
        /// Actual number of run bodies.
        actual: usize,
    },
    /// An override referenced a body that does not exist.
    OverrideIndexOutOfRange {
        /// Referenced body index.
        index: usize,
        /// Number of bodies in the run.
        body_count: usize,
    },
    /// More than one override referenced the same body.
    DuplicateOverride {
        /// Duplicated body index.
        index: usize,
    },
    /// A run declared shared table context but carried a different table.
    TableMismatch {
        /// Header table context.
        expected: String,
        /// Table found in a body version.
        actual: String,
    },
}

impl std::fmt::Display for VersionBundleRunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyRun => write!(f, "version-bundle run has no bodies"),
            Self::BodyCountMismatch { declared, actual } => write!(
                f,
                "version-bundle run body_count {declared} did not match {actual} bodies"
            ),
            Self::OverrideIndexOutOfRange { index, body_count } => write!(
                f,
                "version-bundle run override index {index} out of range for {body_count} bodies"
            ),
            Self::DuplicateOverride { index } => {
                write!(
                    f,
                    "version-bundle run has duplicate override for body {index}"
                )
            }
            Self::TableMismatch { expected, actual } => write!(
                f,
                "version-bundle run table context {expected} did not match body table {actual}"
            ),
        }
    }
}

impl std::error::Error for VersionBundleRunError {}

/// Build packed runs from adjacent singleton bundles.
pub fn build_version_bundle_runs_from_singletons(
    bundles: &[VersionBundle],
) -> Result<Vec<VersionBundleRun>, VersionBundleRunError> {
    if bundles.is_empty() {
        return Ok(Vec::new());
    }
    Ok(vec![VersionBundleRun::from_adjacent_singletons(bundles)?])
}

/// Build the outbound carrier stream for adjacent singleton bundles.
pub fn build_version_carriers_from_singletons(
    bundles: Vec<VersionBundle>,
) -> Result<Vec<VersionCarrier>, VersionBundleRunError> {
    if force_singleton_version_carriers() || bundles.len() <= 1 {
        return Ok(bundles.into_iter().map(VersionCarrier::Bundle).collect());
    }
    Ok(vec![VersionCarrier::Run(
        VersionBundleRun::from_adjacent_singletons(&bundles)?,
    )])
}

fn force_singleton_version_carriers() -> bool {
    #[cfg(test)]
    if FORCE_SINGLETON_VERSION_CARRIERS_FOR_TESTS.load(AtomicOrdering::Relaxed) {
        return true;
    }
    std::env::var_os("JAZZ_FORCE_SINGLETON_VERSION_CARRIERS").is_some()
}

#[cfg(test)]
static FORCE_SINGLETON_VERSION_CARRIERS_FOR_TESTS: AtomicBool = AtomicBool::new(false);

#[cfg(test)]
pub(crate) fn set_force_singleton_version_carriers_for_tests(enabled: bool) {
    FORCE_SINGLETON_VERSION_CARRIERS_FOR_TESTS.store(enabled, AtomicOrdering::Relaxed);
}

/// Expand a carrier stream into singleton bundles.
pub fn expand_version_carriers(
    carriers: &[VersionCarrier],
) -> Result<Vec<VersionBundle>, VersionBundleRunError> {
    let mut bundles = Vec::new();
    for carrier in carriers {
        bundles.extend(carrier.expand()?);
    }
    Ok(bundles)
}

fn common_run_table(bundles: &[VersionBundle]) -> Option<groove::Intern<String>> {
    let mut table = None::<&str>;
    for version in bundles.iter().flat_map(|bundle| &bundle.versions) {
        match table {
            None => table = Some(version.table()),
            Some(current) if current == version.table() => {}
            Some(_) => return None,
        }
    }
    table.map(|table| groove::Intern::new(table.to_owned()))
}

/// Wire handle for one usage-site subscription.
///
/// This key addresses `Subscribe`, `ViewUpdate`, and `Unsubscribe` messages on
/// one link. Its `binding_id` is the subscriber-chosen usage-site handle, not
/// necessarily the canonical binding id derived from the query's bound values.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct SubscriptionKey {
    /// Registered query shape.
    pub shape_id: ShapeId,
    /// Usage-site subscription id. Historically this was often the
    /// deterministic binding id; settled state must use [`BindingViewKey`]
    /// instead.
    pub binding_id: BindingId,
    /// Serving-options identity for this usage site.
    pub read_view: ReadViewKey,
}

/// Canonical settled-state key for one query binding in one read view.
///
/// Unlike [`SubscriptionKey`], this is not a wire subscription handle. It is
/// keyed by the actual canonical binding values and serving-options identity, so
/// multiple usage-site subscriptions for the same binding share one settled
/// result/fact state.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct BindingViewKey {
    /// Registered query shape.
    pub shape_id: ShapeId,
    /// Deterministic binding id derived from canonical binding values.
    pub binding_id: BindingId,
    /// Serving-options identity.
    pub read_view: ReadViewKey,
}

impl BindingViewKey {
    /// Create a canonical binding-view key.
    pub fn new(shape_id: ShapeId, binding_id: BindingId, read_view: ReadViewKey) -> Self {
        Self {
            shape_id,
            binding_id,
            read_view,
        }
    }

    /// Treat a wire subscription key as already canonical.
    ///
    /// Use this only for internal whole-table/coverage paths whose subscription
    /// key was intentionally constructed from canonical binding values.
    pub fn from_canonical_subscription_key(subscription: SubscriptionKey) -> Self {
        Self::new(
            subscription.shape_id,
            subscription.binding_id,
            subscription.read_view,
        )
    }
}

/// Shared coverage group for equivalent query bindings.
#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct CoverageKey {
    /// Registered query shape.
    pub shape_id: ShapeId,
    /// Deterministic binding id derived from canonical binding values.
    pub binding_id: BindingId,
    /// Registration options that affect the view or its upstream routing.
    pub opts: RegisterShapeOptions,
}

/// Versioned query AST carried by shape registration.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ShapeAst {
    /// Wire AST version.
    pub version: u32,
    /// Schema version this shape was authored against.
    pub schema_version: SchemaVersionId,
    /// Registered shape body.
    pub body: ShapeBody,
}

/// Facade syntax accepted by shape registration.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum ShapeBody {
    /// Ordinary root-table query.
    Query(Query),
    /// Output-changing relation query, normalized by the query compiler.
    Relation(RelationQuery),
}

impl ShapeAst {
    /// v0 query AST version.
    pub const VERSION: u32 = 0;

    /// Wrap a query AST in the current protocol version.
    pub fn new(query: Query, schema_version: SchemaVersionId) -> Self {
        Self {
            version: Self::VERSION,
            schema_version,
            body: ShapeBody::Query(query),
        }
    }

    /// Wrap a relation query AST in the current protocol version.
    pub fn new_relation(query: RelationQuery, schema_version: SchemaVersionId) -> Self {
        Self {
            version: Self::VERSION,
            schema_version,
            body: ShapeBody::Relation(query),
        }
    }

    /// Borrow the ordinary query body, if this shape uses that facade syntax.
    pub fn query(&self) -> Option<&Query> {
        match &self.body {
            ShapeBody::Query(query) => Some(query),
            ShapeBody::Relation(_) => None,
        }
    }

    /// Wrap a validated query in the current protocol version.
    pub fn from_validated(shape: &crate::query::ValidatedQuery) -> Self {
        Self::new(shape.query().clone(), shape.schema_version())
    }
}

/// Shape registration options.
#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct RegisterShapeOptions {
    /// Durability tier the subscriber wants this shape served at.
    #[serde(default = "default_register_shape_tier")]
    pub tier: DurabilityTier,
    /// Semantic read-view request for this shape registration.
    #[serde(default)]
    pub read_view: ReadViewSpec,
    /// Whether the serving node may register matching coverage with its own upstream.
    #[serde(default = "default_propagate_upstream")]
    pub propagate_upstream: bool,
}

impl Default for RegisterShapeOptions {
    fn default() -> Self {
        Self {
            tier: default_register_shape_tier(),
            read_view: ReadViewSpec::default(),
            propagate_upstream: default_propagate_upstream(),
        }
    }
}

impl RegisterShapeOptions {
    /// Whether this request uses the only read view currently executable.
    pub fn has_default_read_view(&self) -> bool {
        self.read_view.is_default()
    }

    /// Derive the authoritative read-view key from the full semantic options.
    pub fn read_view_key(&self) -> ReadViewKey {
        ReadViewKey::from_register_shape_options(self)
    }
}

fn default_register_shape_tier() -> DurabilityTier {
    DurabilityTier::Global
}

fn default_propagate_upstream() -> bool {
    true
}

/// Semantic read-view request carried over the wire before local resolution.
#[derive(
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Deserialize,
    serde::Serialize,
)]
pub struct ReadViewSpec {
    /// Which branch/snapshot family this read observes.
    pub source: ReadViewSourceSpec,
}

impl ReadViewSpec {
    /// Whether this is the current/default read view implemented by execution.
    pub fn is_default(&self) -> bool {
        self == &Self::default()
    }

    /// Select a live head branch, optionally composed over one live or frozen base.
    pub fn branch_view(head: BranchSelector, base: Option<BranchViewBase>) -> Self {
        Self {
            source: ReadViewSourceSpec::BranchView { head, base },
        }
    }
}

/// Stable identity for read/serving options used by subscription coverage grouping.
///
/// Despite the historical name, this key is derived from the full
/// [`RegisterShapeOptions`], including durability tier, semantic read view, and
/// upstream-routing intent.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Deserialize,
    serde::Serialize,
)]
pub struct ReadViewKey {
    /// Canonical id of the resolved read view. Nil means the legacy
    /// current/global default.
    pub id: uuid::Uuid,
}

impl ReadViewKey {
    /// Derive a stable key for one registration request.
    pub fn from_register_shape_options(opts: &RegisterShapeOptions) -> Self {
        let canonical = opts.canonical();
        if canonical == RegisterShapeOptions::default() {
            return Self::default();
        }
        let bytes = postcard::to_allocvec(&canonical)
            .expect("register shape options are postcard encodable");
        Self {
            id: uuid::Uuid::new_v5(&READ_VIEW_NAMESPACE, &bytes),
        }
    }
}

impl RegisterShapeOptions {
    fn canonical(&self) -> Self {
        let mut canonical = self.clone();
        canonical.read_view.canonicalize();
        canonical
    }
}

impl ReadViewSpec {
    fn canonicalize(&mut self) {
        self.source.canonicalize();
    }
}

impl ReadViewSourceSpec {
    fn canonicalize(&mut self) {}
}

/// Canonical named values for one schema-wide branch selector.
#[derive(
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Deserialize,
    serde::Serialize,
)]
pub struct BranchSelector {
    /// Canonically encoded typed values keyed by branch-column name.
    pub values: BTreeMap<String, BranchColumnValue>,
}

impl BranchSelector {
    /// Construct a selector from named branch-column values.
    pub fn new(values: impl IntoIterator<Item = (impl Into<String>, Value)>) -> Self {
        Self {
            values: values
                .into_iter()
                .map(|(name, value)| (name.into(), BranchColumnValue::from(value)))
                .collect(),
        }
    }
}

impl BranchViewBase {
    /// Use the live current contents of a base branch.
    pub fn current(branch: BranchSelector) -> Self {
        Self::Current(branch)
    }

    /// Freeze a base branch at an application-resolved snapshot reference.
    pub fn snapshot(branch: BranchSelector, snapshot: SnapshotRef) -> Self {
        Self::Snapshot { branch, snapshot }
    }
}

/// Canonical wire/storage encoding of one branch-column value.
#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct BranchColumnValue(pub Vec<u8>);

impl From<Value> for BranchColumnValue {
    fn from(value: Value) -> Self {
        Self(postcard::to_allocvec(&value).expect("branch column values are encodable"))
    }
}

impl BranchColumnValue {
    /// Decode the value for validation and ordinary column projection.
    pub fn decode(&self) -> Result<Value, postcard::Error> {
        postcard::from_bytes(&self.0)
    }
}

/// Exact, table-projected branch coordinate carried by every row version.
#[derive(
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Deserialize,
    serde::Serialize,
)]
pub struct BranchKey {
    /// Values ordered by branch-column name.
    pub values: Vec<(String, BranchColumnValue)>,
}

impl BranchKey {
    /// Canonical bytes used as the physical branch-local row-key prefix.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        postcard::to_allocvec(self).expect("branch keys are encodable")
    }

    /// Decode a persisted exact branch key.
    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Self, postcard::Error> {
        postcard::from_bytes(bytes)
    }
}

/// Optional base composed underneath the live head of a branch view.
#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub enum BranchViewBase {
    /// A base that continues to observe current writes.
    Current(BranchSelector),
    /// A base frozen at one resolved snapshot.
    Snapshot {
        /// Branch key read at the frozen cut.
        branch: BranchSelector,
        /// Historic frontier shared by every source in the view.
        snapshot: SnapshotRef,
    },
}

/// Wire source selected by a read view.
#[derive(
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Deserialize,
    serde::Serialize,
)]
pub enum ReadViewSourceSpec {
    /// Current default branch/source.
    #[default]
    Current,
    /// A live branch head, optionally composed over one live or frozen base.
    BranchView {
        /// Named values for the requested head coordinate.
        head: BranchSelector,
        /// Optional single fallback source.
        base: Option<BranchViewBase>,
    },
    /// Snapshot ref resolved by the receiving node.
    Snapshot {
        /// Historic frontier to read.
        snapshot: SnapshotRef,
    },
}

/// Dotted snapshot ref used by historic read views.
#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct SnapshotRef {
    /// Node that owns the local snapshot prefix.
    pub owner: NodeUuid,
    /// Contiguous global base visible at snapshot time.
    pub global_base: GlobalTime,
    /// Owner-local HLC prefix visible at snapshot time.
    pub local_base: TxTime,
    /// Individual transaction dots above the frontier.
    #[serde(default)]
    pub dots: Vec<TxId>,
}

impl From<Snapshot> for SnapshotRef {
    fn from(snapshot: Snapshot) -> Self {
        Self {
            owner: snapshot.owner,
            global_base: snapshot.global_base,
            local_base: snapshot.local_base,
            dots: snapshot.dots,
        }
    }
}

/// Usage-site subscription attach for one registered shape.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Subscribe {
    /// Shape whose binding set changes.
    pub shape_id: ShapeId,
    /// Usage-site subscription address.
    pub subscription: SubscriptionKey,
    /// Binding values in shape parameter order.
    pub values: Vec<Value>,
    /// Optional fast known-state declaration for this usage-site subscription.
    pub known_state: Option<KnownStateDeclaration>,
}

/// Known-state declaration echoed by a subscriber on resubscribe.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum KnownStateDeclaration {
    /// Fast optimistic declaration for the current-membership view.
    Fast {
        /// Completeness class this declaration claims.
        completeness: KnownStateCompleteness,
        /// Durable known-state cursor being echoed for payload dedup/repair;
        /// never an active-authority settlement receipt.
        position: GlobalTime,
    },
    /// Fast declaration qualified by the authorization state under which the
    /// receiver applied it.
    FastWithAuthorizationProgress {
        /// Completeness class this declaration claims.
        completeness: KnownStateCompleteness,
        /// Durable known-state cursor being echoed for payload dedup/repair;
        /// never an active-authority settlement receipt.
        position: GlobalTime,
        /// Server-stamped authorization generation echoed by the receiver.
        authorization_progress: u64,
    },
    /// Exact declaration of row-version payloads currently held by the receiver.
    ExactVersionSet {
        /// Explicit version refs the receiver can satisfy without a body.
        versions: Vec<RowVersionRef>,
    },
}

/// Known-state declaration completeness class.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum KnownStateCompleteness {
    /// The subscriber has an unevicted fast current membership view through the
    /// declared settled position.
    FastCurrentMembership,
}

/// Reason a serving peer rejected one subscription attach.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum SubscribeRejectReason {
    /// The shape/read-view cannot currently be maintained by the serving peer.
    UnsupportedShapeCapability {
        /// Human-readable diagnostic. Not part of semantic compatibility.
        detail: String,
    },
    /// The shape is valid, but its schema has not yet reached this runtime.
    ShapeRegistrationPendingCatalogueAdmission,
    /// The serving peer failed while resolving or maintaining the subscription.
    ///
    /// This deliberately carries no server error detail: schema names, policy
    /// expressions, and storage state are not safe to disclose to every peer.
    ServerFailure {
        /// Stable, client-safe classification of the server-side failure.
        code: SubscribeServerFailureCode,
    },
}

/// Client-safe classes for server-side subscription failures.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum SubscribeServerFailureCode {
    /// The requested table was not present in the server's schema.
    TableNotFound,
    /// The server could not resolve the requested schema version or shape.
    SchemaResolution,
    /// Query validation failed on the serving peer.
    QueryValidation,
    /// Query lowering failed on the serving peer.
    QueryLowering,
    /// The serving peer could not evaluate the subscription's policy.
    PolicyEvaluation,
    /// A server-side failure did not fit a more specific safe class.
    Internal,
}

/// Legacy-compatible table-qualified current content row entry:
/// `(table, row_uuid, content_tx_id)`.
pub type ResultRowEntry = (groove::Intern<String>, RowUuid, TxId);

/// Opaque replacement discriminator for a synthetic result member.
///
/// Aggregate result rows have no revision or version. The runtime needs a
/// token solely to pair a retracted aggregate record with its replacement;
/// callers cannot inspect or construct the token as a meaningful value.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize)]
pub struct SyntheticReplacementToken(Vec<u8>);

impl std::fmt::Debug for SyntheticReplacementToken {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SyntheticReplacementToken(..)")
    }
}

impl SyntheticReplacementToken {
    pub(crate) fn from_encoded_record(value: Vec<u8>) -> Self {
        Self(value)
    }
}

#[cfg(test)]
mod synthetic_replacement_token_tests {
    use super::SyntheticReplacementToken;

    #[test]
    fn replacement_token_does_not_expose_a_plausible_revision_value() {
        let token = SyntheticReplacementToken::from_encoded_record(vec![6]);
        assert_eq!(format!("{token:?}"), "SyntheticReplacementToken(..)");
    }
}

/// Protocol-visible result member.
///
/// A member identifies one terminal output of a lowered query program. Ordinary
/// current-row views use [`ResultMemberEntry::Row`] with a compatibility
/// [`ResultRowEntry`] projection, but the member also carries enough optional
/// identity to represent deleted-row, historical, branch, and schema-projected
/// rows without creating a second result-set protocol.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize)]
pub enum ResultMemberEntry {
    /// Real table row membership.
    Row(RealRowMemberEntry),
    /// Synthetic result row, such as aggregate output.
    Synthetic {
        /// Logical synthetic result kind. This is a label, not identity.
        table: String,
        /// Stable identity derived from the aggregate group key.
        row: Vec<u8>,
        /// Opaque runtime-only discriminator for replacement pairing.
        replacement: SyntheticReplacementToken,
    },
    /// Relation/path tuple membership.
    PathTuple {
        /// Path identity.
        path: String,
        /// Source row table.
        source_table: groove::Intern<String>,
        /// Source row.
        source_row: RowUuid,
        /// Target table.
        target_table: groove::Intern<String>,
        /// Target row.
        target_row: RowUuid,
        /// Optional edge/correlation identity for multipath relations.
        edge_id: Option<Vec<u8>>,
        /// Stable tuple revision.
        revision: Vec<u8>,
    },
    /// Real row whose occurrence needs typed derivation discriminators.
    /// Appended after every legacy variant so their postcard tags stay exact.
    TypedRow {
        /// Compatibility row payload and legacy ordered source-row identity.
        row: RealRowMemberEntry,
        /// Versioned full output occurrence identity.
        occurrence_key: ResultKey,
    },
}

/// Real table row membership, including both the ordinary current-content
/// compatibility identity and the extra branch columns needed by historic,
/// branch/prefix, include-deleted, and schema-projected reads.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize)]
pub struct RealRowMemberEntry {
    /// Logical table.
    pub table: groove::Intern<String>,
    /// Row identity.
    pub row_uuid: RowUuid,
    /// Stable identity of the rendered output occurrence. This is the root
    /// row for ordinary single-source output and the ordered source tuple for
    /// a flat join. It deliberately does not include content-version fields,
    /// so replacements retain their output address.
    pub occurrence_id: Option<OutputOccurrenceId>,
    /// Visible content transaction, when this member has a content row.
    #[serde(default)]
    pub content_tx: Option<TxId>,
    /// Which register/layer this member represents.
    #[serde(default)]
    pub layer: ResultRowLayer,
    /// Deletion-register transaction when membership is tombstone-aware.
    #[serde(default)]
    pub deletion_tx: Option<TxId>,
    /// Source/read frontier this member was produced from.
    #[serde(default)]
    pub source: ResultRowSource,
    /// Resolved read-view key for this member.
    #[serde(default)]
    pub read_view: ReadViewKey,
    /// Schema version after lens/projection, when known on the result member.
    #[serde(default)]
    pub schema_version: Option<SchemaVersionId>,
    /// Branch/prefix discriminator when it participates in member identity.
    #[serde(default)]
    pub branch_or_prefix: Option<Vec<u8>>,
    /// Optional stable digest of the visible member row.
    #[serde(default)]
    pub row_digest: Option<Vec<u8>>,
    /// Batch/transaction grouping identity for batch-centric visibility.
    #[serde(default)]
    pub batch: Option<TxId>,
    /// Settled global position of this member's visible current winner, when
    /// known. Unfated/local members carry `None` and are never eligible for
    /// fast known-state body skipping.
    #[serde(default)]
    pub settle_position: Option<GlobalTime>,
}

impl RealRowMemberEntry {
    /// Build the ordinary current-content row member used by current row views.
    pub fn current_content(row: ResultRowEntry) -> Self {
        let (table, row_uuid, content_tx) = row;
        Self {
            table,
            row_uuid,
            occurrence_id: Some(OutputOccurrenceId::single_source(ObjectId::from_uuid(
                row_uuid.0,
            ))),
            content_tx: Some(content_tx),
            layer: ResultRowLayer::Content,
            deletion_tx: None,
            source: ResultRowSource::Current,
            read_view: ReadViewKey::default(),
            schema_version: None,
            branch_or_prefix: None,
            row_digest: None,
            batch: None,
            settle_position: None,
        }
    }

    /// Attach the known global settle position for this member.
    pub fn with_settle_position(mut self, settle_position: Option<GlobalTime>) -> Self {
        self.settle_position = settle_position;
        self
    }

    /// Attach the stable rendered-output address supplied by the maintained
    /// terminal. Join contributors remain in declared source order.
    pub fn with_occurrence_id(mut self, occurrence_id: OutputOccurrenceId) -> Self {
        self.occurrence_id = Some(occurrence_id);
        self
    }

    /// Attach the rendered-output revision. Flat joins use this to distinguish
    /// a source-content replacement while retaining the same occurrence id.
    pub fn with_row_digest(mut self, row_digest: Vec<u8>) -> Self {
        self.row_digest = Some(row_digest);
        self
    }

    /// Stable output occurrence identity. Old persisted members without this
    /// field are normalized to their legacy single-source identity.
    pub fn output_occurrence_id(&self) -> OutputOccurrenceId {
        self.occurrence_id.clone().unwrap_or_else(|| {
            OutputOccurrenceId::single_source(ObjectId::from_uuid(self.row_uuid.0))
        })
    }

    /// Return the ordinary current-content projection when available.
    pub fn row_projection(&self) -> Option<ResultRowEntry> {
        self.content_tx
            .map(|tx| (self.table.clone(), self.row_uuid, tx))
    }
}

/// Version/register layer represented by a real-row result member.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Deserialize,
    serde::Serialize,
)]
pub enum ResultRowLayer {
    /// Visible content register.
    #[default]
    Content,
    /// Deletion register/tombstone.
    Deletion,
    /// Membership is determined by content-or-deletion identity.
    ContentOrDeletion,
}

/// Source/read frontier that produced a real-row result member.
#[derive(
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Deserialize,
    serde::Serialize,
)]
pub enum ResultRowSource {
    /// Current default source.
    #[default]
    Current,
    /// Historic snapshot ref.
    Snapshot {
        /// Snapshot frontier.
        snapshot: SnapshotRef,
    },
    /// Historic cut by global timestamp.
    HistoryCut {
        /// Global timestamp frontier.
        global_time: GlobalTime,
    },
    /// Merge/composition of several source alternatives.
    Merge {
        /// Source alternatives.
        inputs: Vec<ResultRowSource>,
    },
    /// Schema/lens projection over a base source.
    LensProjection {
        /// Projected schema version.
        schema_version: SchemaVersionId,
        /// Base source before projection.
        base: Box<ResultRowSource>,
    },
    /// Local/open overlay over another source.
    Overlay {
        /// Overlay transaction.
        tx: TxId,
        /// Base source under the overlay.
        base: Box<ResultRowSource>,
    },
}

impl ResultMemberEntry {
    /// Construct an ordinary row membership entry.
    pub fn row(entry: ResultRowEntry) -> Self {
        Self::Row(RealRowMemberEntry::current_content(entry))
    }

    /// Return the logical table name when this member belongs to a table-like
    /// output. Synthetic rows use their synthetic table/relation name.
    pub fn table_name(&self) -> Option<&str> {
        match self {
            Self::Row(entry) | Self::TypedRow { row: entry, .. } => Some(entry.table.as_str()),
            Self::Synthetic { table, .. } => Some(table.as_str()),
            Self::PathTuple { target_table, .. } => Some(target_table.as_str()),
        }
    }

    /// Return the real-row member payload, when this member is a real row.
    pub fn as_real_row(&self) -> Option<&RealRowMemberEntry> {
        match self {
            Self::Row(entry) | Self::TypedRow { row: entry, .. } => Some(entry),
            Self::Synthetic { .. } | Self::PathTuple { .. } => None,
        }
    }

    /// Return the stable rendered-output address for a real-row member.
    pub fn output_occurrence_id(&self) -> Option<OutputOccurrenceId> {
        match self {
            Self::TypedRow { occurrence_key, .. } => Some(occurrence_key.as_occurrence().clone()),
            _ => self
                .as_real_row()
                .map(RealRowMemberEntry::output_occurrence_id),
        }
    }

    /// Return the ordinary current-content projection when this member has one.
    pub fn as_row(&self) -> Option<ResultRowEntry> {
        match self {
            Self::Row(entry) | Self::TypedRow { row: entry, .. } => entry.row_projection(),
            Self::Synthetic { .. } | Self::PathTuple { .. } => None,
        }
    }

    /// Consume the ordinary row entry when this member is row-shaped.
    pub fn into_row(self) -> Option<ResultRowEntry> {
        match self {
            Self::Row(entry) | Self::TypedRow { row: entry, .. } => entry.row_projection(),
            Self::Synthetic { .. } | Self::PathTuple { .. } => None,
        }
    }
}

impl From<ResultRowEntry> for ResultMemberEntry {
    fn from(entry: ResultRowEntry) -> Self {
        Self::row(entry)
    }
}

impl From<RealRowMemberEntry> for ResultMemberEntry {
    fn from(entry: RealRowMemberEntry) -> Self {
        let occurrence_key = entry
            .occurrence_id
            .as_ref()
            .filter(|occurrence| occurrence.has_typed_discriminators())
            .cloned()
            .map(ResultKey::from_occurrence);
        match occurrence_key {
            Some(occurrence_key) => Self::TypedRow {
                row: entry,
                occurrence_key,
            },
            None => Self::Row(entry),
        }
    }
}

impl PartialEq<ResultRowEntry> for ResultMemberEntry {
    fn eq(&self, other: &ResultRowEntry) -> bool {
        self.as_row() == Some(*other)
    }
}

impl PartialEq<ResultMemberEntry> for ResultRowEntry {
    fn eq(&self, other: &ResultMemberEntry) -> bool {
        other == self
    }
}

/// One typed non-row fact emitted by a maintained view.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize)]
pub enum ProgramFactEntry {
    /// Payload bytes for a non-versioned result member, such as aggregate/window output.
    ResultPayload(ResultMemberPayloadEntry),
    /// A relation edge between two materialized rows.
    RelationEdge(RelationEdgeEntry),
    /// Coverage for one correlated path expansion.
    PathCorrelationCoverage(PathCorrelationCoverageEntry),
    /// Source/table coverage fact.
    SourceCoverage(SourceCoverageEntry),
    /// Settled read-frontier fact.
    ReadFrontierSettled(ReadFrontierSettledEntry),
    /// Complete transaction payload coverage fact.
    CompleteTxPayloadCoverage(CompleteTxPayloadCoverageEntry),
    /// View-complete exclusive transaction coverage fact.
    ViewCompleteExclusiveCoverage(ViewCompleteExclusiveCoverageEntry),
    /// Policy decision fact.
    PolicyDecision(PolicyDecisionEntry),
    /// Content/deletion/replacement version witness.
    VersionWitness(VersionWitnessEntry),
    /// Policy dependency witness.
    PolicyWitness(PolicyWitnessEntry),
    /// Contributing member/batch provenance.
    ContributingMembers(ContributingMembersEntry),
    /// Predicate-read validation fact.
    PredicateRead(PredicateReadEntry),
    /// Predicate output-set fact.
    PredicateOutputSet(PredicateOutputSetEntry),
    /// Point row-read validation fact.
    PointRead(PointReadEntry),
}

/// Compatibility alias while current code still imports the previous name.
pub type ViewFactEntry = ProgramFactEntry;

/// Non-versioned result payload keyed by a typed result member.
///
/// Ordinary real rows travel via `VersionBundle`; synthetic/aggregate/window
/// outputs use this fact to keep member identity and row bytes in the same
/// typed program-output stream.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize)]
pub struct ResultMemberPayloadEntry {
    /// Member whose payload is encoded here.
    pub member: ResultMemberEntry,
    /// Descriptor or schema identity for decoding `record`.
    pub descriptor: Vec<u8>,
    /// Custom row-record encoded payload bytes.
    pub record: Vec<u8>,
}

/// Relation edge fact emitted by query payloads.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize)]
pub struct RelationEdgeEntry {
    /// Logical path or relation name.
    pub path: String,
    /// Source row table.
    pub source_table: groove::Intern<String>,
    /// Source row id.
    pub source_row: RowUuid,
    /// Target row table.
    pub target_table: groove::Intern<String>,
    /// Target row id.
    pub target_row: RowUuid,
    /// Edge kind, when this is more specific than a plain include/join edge.
    #[serde(default)]
    pub kind: Option<RelationEdgeKind>,
    /// Source version identity, when edge membership depends on a concrete version.
    #[serde(default)]
    pub source_version: Option<RowVersionRefEntry>,
    /// Target version identity, when edge membership depends on a concrete version.
    #[serde(default)]
    pub target_version: Option<RowVersionRefEntry>,
    /// Recursive depth for reachability/gather paths.
    #[serde(default)]
    pub depth: Option<u32>,
    /// Multipath/edge id when several edges connect the same source/target.
    #[serde(default)]
    pub edge_id: Option<Vec<u8>>,
    /// Union/policy branch alternative.
    #[serde(default)]
    pub branch: Option<Vec<u8>>,
    /// Terminal role for intermediate/frontier/output relation rows.
    #[serde(default)]
    pub role: Option<RelationEdgeRole>,
    /// Stable edge order when order affects the maintained output.
    #[serde(default)]
    pub order: Option<Vec<u8>>,
    /// Whether this edge is a materialized match or a hole/null placeholder.
    #[serde(default)]
    pub hole_state: Option<PathHoleState>,
}

/// Concrete row-version reference used by facts.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize)]
pub struct RowVersionRefEntry {
    /// Transaction containing the version.
    pub tx: TxId,
    /// Schema version carried by the version row, when available.
    #[serde(default)]
    pub schema_version: Option<SchemaVersionId>,
    /// Version/register layer.
    #[serde(default)]
    pub layer: ResultRowLayer,
    /// Batch/transaction grouping identity.
    #[serde(default)]
    pub batch: Option<TxId>,
    /// Branch/prefix discriminator.
    #[serde(default)]
    pub branch_or_prefix: Option<Vec<u8>>,
    /// Optional visible row digest.
    #[serde(default)]
    pub row_digest: Option<Vec<u8>>,
}

/// Version/replacement witness for payload and removal materialization.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize)]
pub struct VersionWitnessEntry {
    /// Witness role, such as payload, replacement, or deletion.
    pub role: String,
    /// Witnessed version.
    pub version: RowVersionRefEntry,
    /// Result member this witness serves, when scoped to one member.
    pub member: Option<ResultMemberEntry>,
}

/// Policy dependency witness fact.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize)]
pub struct PolicyWitnessEntry {
    /// Protected result member or row.
    pub protected: ResultMemberEntry,
    /// Policy path/branch identity.
    pub policy_path: String,
    /// Witness version proving or revoking visibility.
    pub witness: RowVersionRefEntry,
    /// Dependency edge kind.
    pub edge_kind: Option<RelationEdgeKind>,
}

/// Derived-output provenance fact.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize)]
pub struct ContributingMembersEntry {
    /// Derived result member.
    pub result: ResultMemberEntry,
    /// Contributing member.
    pub contributor: ResultMemberEntry,
    /// Optional contributing transaction/batch.
    pub batch: Option<TxId>,
    /// Optional contribution role.
    pub role: Option<String>,
}

/// Predicate-read validation fact.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize)]
pub struct PredicateReadEntry {
    /// Validation side: base or now.
    pub role: PredicateOutputSetRoleEntry,
    /// Shape id.
    pub shape_id: ShapeId,
    /// Binding id.
    pub binding_id: BindingId,
    /// Encoded predicate/range identity.
    pub predicate: Vec<u8>,
    /// Encoded read frontier or snapshot point.
    pub frontier: Vec<u8>,
}

/// Point row-read validation fact.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize)]
pub struct PointReadEntry {
    /// Whether the row was present in this read.
    pub present: bool,
    /// Logical table.
    pub table: groove::Intern<String>,
    /// Row identity.
    pub row: RowUuid,
    /// Concrete version read, when present.
    pub version: Option<RowVersionRefEntry>,
    /// Shape id.
    pub shape_id: ShapeId,
    /// Binding id.
    pub binding_id: BindingId,
}

/// Relation edge kind.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub enum RelationEdgeKind {
    /// Include edge.
    Include,
    /// Join edge.
    Join,
    /// Relation traversal edge.
    Relation,
    /// Recursive frontier/reachability edge.
    Recursive,
    /// Policy dependency edge.
    Policy,
}

/// Role of a relation edge in the maintained program.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub enum RelationEdgeRole {
    /// Internal edge only.
    Intermediate,
    /// Frontier/worklist edge.
    Frontier,
    /// Edge contributes directly to output membership.
    Terminal,
}

/// Placeholder state for optional relation/include paths.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub enum PathHoleState {
    /// Concrete matched edge.
    Matched,
    /// Placeholder for an absent optional target.
    Hole,
}

/// Correlation coverage fact for relation/path materialization.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize)]
pub struct PathCorrelationCoverageEntry {
    /// Logical path or relation name.
    pub path: String,
    /// Source row table.
    pub source_table: groove::Intern<String>,
    /// Source row id.
    pub source_row: RowUuid,
    /// Canonical encoded correlation key for the path expansion.
    pub correlation_key: Vec<u8>,
    /// Whether this correlation is complete for the subscription read view.
    pub complete: bool,
}

/// Source/table coverage fact.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize)]
pub struct SourceCoverageEntry {
    /// Logical source id/path encoded for the wire.
    pub source: String,
    /// Logical table.
    pub table: groove::Intern<String>,
    /// Optional covered row.
    pub row: Option<RowUuid>,
    /// Canonical encoded coverage/range key.
    pub coverage: Vec<u8>,
}

/// Settled read-frontier fact.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize)]
pub struct ReadFrontierSettledEntry {
    /// Scope this frontier settles.
    pub scope: String,
    /// Durability tier that settled.
    pub tier: DurabilityTier,
    /// Optional ordered stream identity.
    pub stream: Option<String>,
    /// Canonical encoded frontier position.
    pub frontier: Vec<u8>,
}

/// Complete transaction payload coverage fact.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize)]
pub struct CompleteTxPayloadCoverageEntry {
    /// Covered transaction/batch.
    pub tx: TxId,
    /// Durability tier at which the payload is complete.
    pub tier: DurabilityTier,
    /// Canonical payload digest.
    pub payload_digest: Vec<u8>,
}

/// View-complete exclusive transaction coverage fact.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize)]
pub struct ViewCompleteExclusiveCoverageEntry {
    /// Covered transaction/batch.
    pub tx: TxId,
    /// View/source scope.
    pub scope: String,
    /// Optional result member this coverage is complete for.
    pub result: Option<ResultMemberEntry>,
    /// Durability tier at which this view coverage is complete.
    pub tier: DurabilityTier,
    /// Digest of members covered by this view/result.
    pub covered_members_digest: Vec<u8>,
}

/// Tri-state policy decision fact.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize)]
pub struct PolicyDecisionEntry {
    /// Decision identity inside the program.
    pub decision: Vec<u8>,
    /// Decision outcome.
    pub outcome: PolicyDecisionOutcomeEntry,
    /// Optional machine-readable reason.
    pub reason: Option<String>,
}

/// Wire policy decision outcome.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize)]
pub enum PolicyDecisionOutcomeEntry {
    /// Policy grants the operation.
    Allowed,
    /// Policy denies the operation.
    Denied,
    /// Caller did not provide input required by the policy.
    IndeterminateRequiresInput {
        /// Missing input name/category.
        input: String,
    },
    /// The local node has not observed enough source/frontier coverage.
    RequiresCoverage {
        /// Coverage scope required before the decision can be known.
        scope: String,
        /// Canonical encoded frontier requirement.
        frontier: Vec<u8>,
    },
}

/// Predicate output-set fact.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize)]
pub struct PredicateOutputSetEntry {
    /// Validation side: base or now.
    pub role: PredicateOutputSetRoleEntry,
    /// Logical table.
    pub table: groove::Intern<String>,
    /// Row identity.
    pub row: RowUuid,
    /// Version identity compared by validation.
    pub version: RowVersionRefEntry,
    /// Shape id.
    pub shape_id: ShapeId,
    /// Binding id.
    pub binding_id: BindingId,
}

/// Predicate output-set comparison side.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize,
)]
pub enum PredicateOutputSetRoleEntry {
    /// Base snapshot side.
    Base,
    /// Validation/current side.
    Now,
}

/// Namespace used for migration-lens UUIDv5 ids.
pub const MIGRATION_LENS_NAMESPACE: uuid::Uuid =
    uuid::uuid!("5d13f9cb-8a10-5e0f-9a58-e56630a1dc22");

/// Namespace used for atomic schema-lineage publication UUIDv5 ids.
pub const SCHEMA_LINEAGE_PUBLICATION_NAMESPACE: uuid::Uuid =
    uuid::uuid!("a1b3ff15-9358-52e0-baa8-f384b1d5db1c");

/// Namespace used for semantic read-view UUIDv5 ids.
pub const READ_VIEW_NAMESPACE: uuid::Uuid = uuid::uuid!("1a87cf70-f8f0-5ae7-a574-1f9b5e4517f1");

/// Compiled in-memory schema version whose durable/wire form is the public schema.
#[derive(Clone, Debug, PartialEq)]
pub struct SchemaVersion {
    /// Content-addressed id, equal to `schema.version_id()`.
    pub id: SchemaVersionId,
    /// Compiled runtime schema. Serialization emits its retained public schema.
    pub schema: JazzSchema,
}

#[derive(serde::Deserialize, serde::Serialize)]
struct SchemaVersionWire {
    id: SchemaVersionId,
    /// Canonical public schema and PolicyExpr values encoded as JSON so public
    /// Value remains portable across both JSON storage and postcard transport
    /// serializers.
    public_schema_json: Vec<u8>,
}

fn durable_public_schema_json(schema: &JazzSchema) -> Result<Vec<u8>, String> {
    serde_json::to_vec(schema.public_schema()).map_err(|error| error.to_string())
}

fn compile_public_schema_json(bytes: &[u8]) -> Result<JazzSchema, String> {
    crate::tools::public_schema_convert::decode_public_schema_json(bytes)
}

impl serde::Serialize for SchemaVersion {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let public_schema_json =
            durable_public_schema_json(&self.schema).map_err(serde::ser::Error::custom)?;
        let wire = SchemaVersionWire {
            id: self.id,
            public_schema_json,
        };
        wire.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for SchemaVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let SchemaVersionWire {
            id,
            public_schema_json,
        } = SchemaVersionWire::deserialize(deserializer)?;
        let schema =
            compile_public_schema_json(&public_schema_json).map_err(serde::de::Error::custom)?;
        // Keep identity validation at catalogue admission/open, where callers
        // receive the established domain error rather than a codec error.
        Ok(Self { id, schema })
    }
}

/// Atomic catalogue payload that admits one non-genesis schema.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct SchemaLineagePublication {
    /// Content-addressed identity of this complete bundle.
    pub id: SchemaLineagePublicationId,
    /// New immutable schema payload.
    pub schema: SchemaVersion,
    /// Lineage-defining lens from one already-admitted schema.
    pub lens: MigrationLens,
    /// Target tables that begin fresh physical lineages.
    pub new_tables: Vec<String>,
    /// Source tables intentionally absent from the target schema.
    pub dropped_tables: Vec<String>,
}

impl SchemaLineagePublication {
    /// Construct an atomic schema-lineage publication payload.
    pub fn new(
        schema: SchemaVersion,
        lens: MigrationLens,
        new_tables: impl IntoIterator<Item = impl Into<String>>,
        dropped_tables: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        let mut publication = Self {
            id: SchemaLineagePublicationId(uuid::Uuid::nil()),
            schema,
            lens,
            new_tables: new_tables.into_iter().map(Into::into).collect(),
            dropped_tables: dropped_tables.into_iter().map(Into::into).collect(),
        };
        publication.id = publication.content_id();
        publication
    }

    /// Return the content-addressed id implied by this payload.
    pub fn content_id(&self) -> SchemaLineagePublicationId {
        let mut bytes = Vec::new();
        put_str(&mut bytes, "jazz-schema-lineage-publication-v1");
        put_bytes(
            &mut bytes,
            &serde_json::to_vec(&self.schema).expect("schema publication serializes"),
        );
        put_bytes(
            &mut bytes,
            &serde_json::to_vec(&self.lens).expect("lineage lens serializes"),
        );
        let mut new_tables = self.new_tables.clone();
        new_tables.sort();
        put_len(&mut bytes, new_tables.len());
        for table in new_tables {
            put_str(&mut bytes, &table);
        }
        let mut dropped_tables = self.dropped_tables.clone();
        dropped_tables.sort();
        put_len(&mut bytes, dropped_tables.len());
        for table in dropped_tables {
            put_str(&mut bytes, &table);
        }
        SchemaLineagePublicationId(uuid::Uuid::new_v5(
            &SCHEMA_LINEAGE_PUBLICATION_NAMESPACE,
            &bytes,
        ))
    }
}

impl SchemaVersion {
    /// Construct a schema-version payload from a compiled schema.
    ///
    /// Durable or wire serialization requires the schema to retain the public
    /// source attached by the public-schema compiler.
    pub fn new(schema: JazzSchema) -> Self {
        Self {
            id: schema.version_id(),
            schema,
        }
    }
}

/// Published bidirectional migration lens.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct MigrationLens {
    /// Content-addressed lens id.
    pub id: MigrationLensId,
    /// Source schema version.
    pub source: SchemaVersionId,
    /// Target schema version.
    pub target: SchemaVersionId,
    /// Per-table lens definitions.
    pub table_lenses: Vec<TableLens>,
}

impl MigrationLens {
    /// Construct a migration lens and derive its content-addressed id.
    pub fn new(
        source: SchemaVersionId,
        target: SchemaVersionId,
        table_lenses: Vec<TableLens>,
    ) -> Self {
        let mut lens = Self {
            id: MigrationLensId(uuid::Uuid::nil()),
            source,
            target,
            table_lenses,
        };
        lens.id = lens.content_id();
        lens
    }

    /// Return the content-addressed id implied by this payload.
    pub fn content_id(&self) -> MigrationLensId {
        MigrationLensId(uuid::Uuid::new_v5(
            &MIGRATION_LENS_NAMESPACE,
            &canonical_lens_bytes(self),
        ))
    }
}

fn canonical_lens_bytes(lens: &MigrationLens) -> Vec<u8> {
    let mut bytes = Vec::new();
    put_str(&mut bytes, "jazz-migration-lens-v1");
    bytes.extend_from_slice(lens.source.as_bytes());
    bytes.extend_from_slice(lens.target.as_bytes());
    put_len(&mut bytes, lens.table_lenses.len());
    for table_lens in &lens.table_lenses {
        put_str(&mut bytes, &table_lens.source_table);
        put_str(&mut bytes, &table_lens.target_table);
        put_len(&mut bytes, table_lens.ops.len());
        for op in &table_lens.ops {
            put_lens_op(&mut bytes, op);
        }
    }
    bytes
}

fn put_lens_op(bytes: &mut Vec<u8>, op: &LensOp) {
    match op {
        LensOp::RenameTable { from, to } => {
            bytes.push(0);
            put_str(bytes, from);
            put_str(bytes, to);
        }
        LensOp::RenameColumn { from, to } => {
            bytes.push(1);
            put_str(bytes, from);
            put_str(bytes, to);
        }
        LensOp::CopyColumn { from, to } => {
            bytes.push(2);
            put_str(bytes, from);
            put_str(bytes, to);
        }
        LensOp::AddColumn { column, default } => {
            bytes.push(3);
            put_str(bytes, column);
            put_value(bytes, default);
        }
        LensOp::DropColumn {
            column,
            backwards_default,
        } => {
            bytes.push(4);
            put_str(bytes, column);
            put_value(bytes, backwards_default);
        }
        LensOp::TransformColumn { column, transform } => {
            bytes.push(5);
            put_str(bytes, column);
            put_str(bytes, transform);
        }
        LensOp::RejectSourceDelta { reason } => {
            bytes.push(6);
            put_str(bytes, reason);
        }
    }
}

fn put_value(bytes: &mut Vec<u8>, value: &Value) {
    match value {
        Value::U8(value) => {
            bytes.push(0);
            bytes.push(*value);
        }
        Value::U16(value) => {
            bytes.push(1);
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        Value::U32(value) => {
            bytes.push(2);
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        Value::U64(value) => {
            bytes.push(3);
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        Value::I32(value) => {
            bytes.push(14);
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        Value::I64(value) => {
            bytes.push(13);
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        Value::F64(value) => {
            bytes.push(4);
            bytes.extend_from_slice(&value.to_bits().to_le_bytes());
        }
        Value::Bool(value) => {
            bytes.push(5);
            bytes.push(u8::from(*value));
        }
        Value::String(value) => {
            bytes.push(6);
            put_str(bytes, value);
        }
        Value::Bytes(value) => {
            bytes.push(7);
            put_bytes(bytes, value);
        }
        Value::Uuid(value) => {
            bytes.push(8);
            bytes.extend_from_slice(value.as_bytes());
        }
        Value::EnumTag(value) => {
            bytes.push(9);
            bytes.push(*value);
        }
        Value::Tuple(values) => {
            bytes.push(10);
            put_values(bytes, values);
        }
        Value::Array(values) => {
            bytes.push(11);
            put_values(bytes, values);
        }
        Value::Nullable(value) => {
            bytes.push(12);
            match value {
                Some(value) => {
                    bytes.push(1);
                    put_value(bytes, value);
                }
                None => bytes.push(0),
            }
        }
        Value::Record(_) => {
            panic!("record-valued values have no v3 protocol encoding")
        }
        Value::Enum(_) => {
            panic!(
                "union-valued values are an internal Groove representation, not a Jazz protocol value"
            )
        }
        Value::Large(value) => {
            bytes.push(15);
            let encoded = groove::large_values::encode_stored_scalar(
                value.kind,
                &groove::large_values::StoredScalar::Chunked(value.clone()),
            )
            .expect("admitted large descriptor has canonical encoding");
            put_bytes(bytes, &encoded);
        }
    }
}

fn put_values(bytes: &mut Vec<u8>, values: &[Value]) {
    put_len(bytes, values.len());
    for value in values {
        put_value(bytes, value);
    }
}

fn put_str(bytes: &mut Vec<u8>, value: &str) {
    put_bytes(bytes, value.as_bytes());
}

fn put_bytes(bytes: &mut Vec<u8>, value: &[u8]) {
    put_len(bytes, value.len());
    bytes.extend_from_slice(value);
}

fn put_len(bytes: &mut Vec<u8>, len: usize) {
    let len = u32::try_from(len).expect("canonical lens component exceeds u32");
    bytes.extend_from_slice(&len.to_le_bytes());
}

/// Lens operations for one logical table.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct TableLens {
    /// Source logical table.
    pub source_table: String,
    /// Target logical table.
    pub target_table: String,
    /// Ordered lens operations.
    pub ops: Vec<LensOp>,
}

/// v0 migration lens operation set.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum LensOp {
    /// Rename a table.
    RenameTable {
        /// Source table name.
        from: String,
        /// Target table name.
        to: String,
    },
    /// Rename a column.
    RenameColumn {
        /// Source column name.
        from: String,
        /// Target column name.
        to: String,
    },
    /// Copy a column.
    CopyColumn {
        /// Source column name.
        from: String,
        /// Target column name.
        to: String,
    },
    /// Add a target column with a forward default.
    AddColumn {
        /// Target column name.
        column: String,
        /// Forward default value.
        default: Value,
    },
    /// Drop a source column with a reverse default.
    DropColumn {
        /// Source column name.
        column: String,
        /// Backwards default used when translating from target to source.
        backwards_default: Value,
    },
    /// Built-in transform placeholder. Evaluation lands in a later slice.
    TransformColumn {
        /// Column being transformed.
        column: String,
        /// Append-only built-in transform registry key.
        transform: String,
    },
    /// Declare source deltas rejected by this lens.
    RejectSourceDelta {
        /// Human-readable rejection reason.
        reason: String,
    },
}

/// Core-ordered current write-schema pointer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct CurrentWriteSchema {
    /// Monotone catalogue revision assigned by the core/admin lane.
    pub revision: u64,
    /// Current schema for canonical writes.
    pub schema: SchemaVersionId,
}

/// Acknowledgement for catalogue lane messages.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct CatalogueAck {
    /// Applied catalogue revision, if the message carried one.
    pub revision: Option<u64>,
    /// Published schema id, if any.
    pub schema: Option<SchemaVersionId>,
    /// Published lens id, if any.
    pub lens: Option<MigrationLensId>,
    /// True when the receiver installed or already had the value.
    pub applied: bool,
}

/// Local embedding API events.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LocalEvent {
    /// Local table mutation.
    Mutate {
        /// Mutated table.
        table: String,
    },
    /// Open an exclusive transaction.
    OpenTx,
    /// Write inside an exclusive transaction.
    WriteInTx {
        /// Transaction id.
        tx: TxId,
    },
    /// Commit an exclusive transaction.
    CommitTx {
        /// Transaction id.
        tx: TxId,
    },
    /// Abandon an exclusive transaction.
    AbandonTx {
        /// Transaction id.
        tx: TxId,
    },
    /// Run a query.
    Query {
        /// Query reference.
        query_ref: u64,
    },
    /// Subscribe a query.
    Subscribe {
        /// Query reference.
        query_ref: u64,
        /// Query binding result set.
        subscription: SubscriptionKey,
    },
    /// Remove a subscription.
    Unsubscribe {
        /// Query binding result set.
        subscription: SubscriptionKey,
    },
}

/// A single input to a node state machine.
#[derive(Clone, Debug, PartialEq)]
pub enum Event {
    /// Sync input.
    Sync(SyncMessage),
    /// Local input.
    Local(LocalEvent),
}

/// Values emitted by a node after handling one event.
#[derive(Clone, Debug, PartialEq)]
pub enum OutboxMessage {
    /// Sync output.
    Sync(SyncMessage),
    /// Query result notification.
    QueryResult {
        /// Query reference.
        query_ref: u64,
    },
    /// Subscription change notification.
    SubscriptionNotification {
        /// Query binding result set.
        subscription: SubscriptionKey,
    },
    /// Transaction fate notification.
    TxFate {
        /// Transaction id.
        tx_id: TxId,
        /// Observed fate.
        fate: Fate,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::public_schema::{
        ColumnType as PublicColumnType, PolicyExpr, SchemaBuilder, TablePolicies,
        TableSchemaBuilder,
    };
    use groove::schema::{ColumnSchema, ColumnType};

    fn schema_id(byte: u8) -> SchemaVersionId {
        SchemaVersionId::from_bytes([byte; 16])
    }

    #[test]
    fn schema_version_persists_policy_source_and_recompiles_on_decode() {
        let source = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("title", PublicColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::True)),
            )
            .build();
        let compiled = crate::schema::JazzSchema::new(&source).expect("source schema compiles");
        let version = SchemaVersion::new(compiled);

        let stored = serde_json::to_string(&version).expect("source schema serializes");
        let SchemaVersionWire {
            public_schema_json, ..
        } = serde_json::from_str(&stored).expect("decode stored schema envelope");
        let stored_source_json =
            String::from_utf8(public_schema_json).expect("public schema JSON is UTF-8");
        let stored_source: serde_json::Value =
            serde_json::from_str(&stored_source_json).expect("public schema JSON decodes");
        assert!(
            stored_source["tables"].is_object(),
            "schema-version payloads preserve the public Schema JSON envelope"
        );
        assert!(stored_source_json.contains("\"policies\""));
        assert!(stored_source_json.contains("\"type\":\"True\""));
        assert!(!stored_source_json.contains("read_policy"));
        assert!(!stored_source_json.contains("write_policies"));

        let bytes = postcard::to_allocvec(&version).expect("source schema crosses the wire");
        let decoded: SchemaVersion =
            postcard::from_bytes(&bytes).expect("source schema recompiles on decode");
        assert_eq!(decoded, version);
        assert_eq!(
            decoded.schema.public_schema(),
            version.schema.public_schema()
        );
    }

    #[test]
    fn schema_version_serialization_trusts_the_retained_source_without_recompiling() {
        let source = SchemaBuilder::new()
            .table(TableSchemaBuilder::new("todos").column("title", PublicColumnType::Text))
            .build();
        let compiled = crate::schema::JazzSchema::new(&source).expect("source schema compiles");
        let mut version = SchemaVersion::new(compiled);

        // A test-only mutation makes recompilation observably disagree with the
        // runtime value. Wire serialization must remain a source encoding step;
        // recompiling here puts schema-size work on every propagated write.
        version.schema.runtime_mut_for_testing().tables.clear();

        let encoded = postcard::to_allocvec(&version)
            .expect("serialization trusts the source established at construction");
        let decoded: SchemaVersion = postcard::from_bytes(&encoded).expect("source recompiles");
        assert_eq!(decoded.schema.public_schema(), &source);
        assert_eq!(decoded.schema.tables().len(), 1);
    }

    #[test]
    fn branch_view_keys_isolate_siblings_and_live_from_frozen_bases() {
        let selector = |byte| {
            BranchSelector::new([("branch", Value::Uuid(uuid::Uuid::from_bytes([byte; 16])))])
        };
        let key = |read_view| {
            RegisterShapeOptions {
                read_view,
                ..RegisterShapeOptions::default()
            }
            .read_view_key()
        };
        let live =
            ReadViewSpec::branch_view(selector(1), Some(BranchViewBase::Current(selector(2))));
        let sibling =
            ReadViewSpec::branch_view(selector(3), Some(BranchViewBase::Current(selector(2))));
        let frozen = ReadViewSpec::branch_view(
            selector(1),
            Some(BranchViewBase::snapshot(
                selector(2),
                SnapshotRef {
                    owner: NodeUuid::from_bytes([4; 16]),
                    global_base: GlobalTime(5),
                    local_base: TxTime(6),
                    dots: Vec::new(),
                },
            )),
        );

        assert_ne!(key(live.clone()), key(sibling));
        assert_ne!(key(live), key(frozen));
    }

    #[test]
    fn result_member_transport_preserves_typed_union_occurrence() {
        let root = ObjectId::from_uuid(uuid::Uuid::from_bytes([1; 16]));
        let joined = ObjectId::from_uuid(uuid::Uuid::from_bytes([2; 16]));
        let occurrence =
            OutputOccurrenceId::with_union_arms(root, [joined], [(0, "direct".to_owned())])
                .unwrap();
        let member: ResultMemberEntry = RealRowMemberEntry::current_content((
            groove::Intern::new("todos".to_owned()),
            RowUuid(*root.uuid()),
            TxId::new(TxTime(1), NodeUuid::from_bytes([3; 16])),
        ))
        .with_occurrence_id(occurrence.clone())
        .into();
        let bytes = postcard::to_allocvec(&member).unwrap();
        let decoded: ResultMemberEntry = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.output_occurrence_id(), Some(occurrence));
    }

    #[test]
    fn legacy_row_member_postcard_golden_decodes() {
        const LEGACY: &[u8] = &[
            0, 1, 116, 16, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 16, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 2, 16, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
            3, 0, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ];
        let member = ResultMemberEntry::Row(RealRowMemberEntry::current_content((
            groove::Intern::new("t".to_owned()),
            RowUuid::from_bytes([1; 16]),
            TxId::new(TxTime(2), NodeUuid::from_bytes([3; 16])),
        )));
        assert_eq!(postcard::to_allocvec(&member).unwrap(), LEGACY);
        let decoded: ResultMemberEntry = postcard::from_bytes(LEGACY).unwrap();
        assert_eq!(decoded, member);
    }

    #[test]
    fn version_record_ordering_distinguishes_authored_presence() {
        let table = TableSchema::new("todos", [ColumnSchema::new("title", ColumnType::String)]);
        let base = VersionRecord::from_cells(
            &table,
            schema_id(1),
            RowUuid::from_bytes([1; 16]),
            Vec::new(),
            AuthorSubject::SYSTEM,
            1,
            AuthorSubject::SYSTEM,
            1,
            &BTreeMap::from([("title".to_owned(), Value::String("x".to_owned()))]),
            None,
        )
        .unwrap();
        let authored = base
            .clone()
            .with_authored_columns(Some(BTreeSet::from(["title".to_owned()])));

        assert_ne!(base, authored);
        assert_ne!(base.cmp(&authored), Ordering::Equal);
    }

    fn sample_lens() -> MigrationLens {
        MigrationLens::new(
            schema_id(1),
            schema_id(2),
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "tasks".to_owned(),
                ops: vec![
                    LensOp::RenameTable {
                        from: "todos".to_owned(),
                        to: "tasks".to_owned(),
                    },
                    LensOp::RenameColumn {
                        from: "title".to_owned(),
                        to: "name".to_owned(),
                    },
                    LensOp::AddColumn {
                        column: "status".to_owned(),
                        default: Value::String("open".to_owned()),
                    },
                ],
            }],
        )
    }

    #[test]
    fn migration_lens_content_id_uses_canonical_payload_not_id_field() {
        let lens = sample_lens();
        let mut same_payload = lens.clone();
        same_payload.id = MigrationLensId::from_bytes([0x99; 16]);

        assert_eq!(lens.content_id(), same_payload.content_id());
    }

    #[test]
    fn migration_lens_content_id_changes_when_structural_field_changes() {
        let lens = sample_lens();
        let mut changed = lens.clone();
        changed.table_lenses[0].ops[2] = LensOp::AddColumn {
            column: "status".to_owned(),
            default: Value::String("closed".to_owned()),
        };

        assert_ne!(lens.content_id(), changed.content_id());
    }
}
