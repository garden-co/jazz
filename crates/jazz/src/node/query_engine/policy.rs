use super::*;

/// Concrete policy request. These are not fake query shapes.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum PolicyDecisionRequest {
    /// Existing row visibility check.
    CanRead(PolicyReadCandidate),
    /// Proposed write check.
    CanWrite(PolicyWriteCandidate),
}

/// Concrete row/read target for read-policy decisions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PolicyReadCandidate {
    /// Logical table.
    pub(crate) table: String,
    /// Row identity.
    pub(crate) row: RowUuid,
    /// Concrete row-state target being authorized.
    pub(crate) target: PolicyReadTarget,
    /// Query-visible scope that led to this read check.
    pub(crate) visibility: RowVisibility,
}

/// Version/deletion target for read-policy decisions.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum PolicyReadTarget {
    /// Authorize whatever version is visible in the selected read view.
    CurrentAtReadView,
    /// Authorize this concrete content version.
    ContentVersion(TxId),
    /// Authorize this concrete deletion-register version.
    DeletionVersion(TxId),
}

/// Proposed write candidate for dry-run policy decisions.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PolicyWriteCandidate {
    /// Write target and transaction context being authorized.
    pub(crate) context: WriteEvaluationContext,
    /// Logical table.
    pub(crate) table: String,
    /// Operation metadata available to policy lowering.
    pub(crate) metadata: BTreeMap<String, Value>,
    /// Concrete write operation.
    pub(crate) op: PolicyWriteOp,
}

/// Concrete write operation for policy probing.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum PolicyWriteOp {
    /// Insert a new row. Row allocation happens before policy probing unless
    /// policy is explicitly id-independent.
    Insert {
        /// Row identity semantics.
        row: PolicyRowId,
        /// New row contents.
        new: VersionedCellValues,
    },
    /// Update or upsert an existing row.
    Update {
        /// Row identity.
        row: RowUuid,
        /// Existing row state required by policy.
        base: VersionedWriteBase,
        /// Proposed cell change.
        change: WriteChange,
    },
    /// Delete a row.
    Delete {
        /// Row identity.
        row: RowUuid,
        /// Existing row state required by policy.
        base: VersionedWriteBase,
    },
    /// Restore a previously deleted row, optionally with an after-image.
    Restore {
        /// Row identity.
        row: RowUuid,
        /// Existing row state required by policy.
        base: VersionedWriteBase,
        /// Optional restored after-image.
        after: Option<VersionedCellValues>,
    },
}

/// Context that makes a write-policy probe a concrete branch/transaction
/// decision, not just a table-row predicate.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct WriteEvaluationContext {
    /// Data branch/prefix receiving the write.
    pub(crate) data_branch: BranchId,
    /// Schema-family branch whose policy/schema head is authoritative.
    pub(crate) schema_family_branch: BranchId,
    /// Mergeability/isolation regime being authorized.
    pub(crate) tx_kind: TxKind,
    /// Runtime authority boundary evaluating the write.
    pub(crate) authority: WriteAuthorityScope,
}

/// Authority boundary for write-policy evaluation.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum WriteAuthorityScope {
    /// Local optimistic client probe.
    LocalClient,
    /// Edge authority validating a client commit before upstream fate.
    EdgeAuthority,
    /// Global authority assigning final fate.
    GlobalAuthority,
    /// Branch-owned operation such as branch squash or branch metadata write.
    BranchAuthority(BranchId),
}

/// Row identity semantics for policy probes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum PolicyRowId {
    /// Caller supplied the row id before policy evaluation.
    Row(RowUuid),
    /// Probe is only valid when policy does not inspect row identity.
    IdIndependent,
}

/// Existing row state used by write policy checks.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum VersionedWriteBase {
    /// Insert with no existing row.
    Absent,
    /// Resolve old row for the candidate row from the selected read view.
    ResolveFromReadView,
    /// Caller provided a live row with version identity.
    ProvidedLive {
        /// Existing application cells.
        values: VersionedCellValues,
        /// Content version identity, when known.
        content_version: Option<TxId>,
    },
    /// Caller provided a deleted row with optional last visible content and
    /// deletion version.
    ProvidedDeleted {
        /// Last visible application cells, when available.
        values: Option<VersionedCellValues>,
        /// Last content version identity, when known.
        content_version: Option<TxId>,
        /// Deletion-register version identity, when known.
        deletion_version: Option<TxId>,
    },
}

/// Proposed cell change for update/upsert-style policy checks.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum WriteChange {
    /// Patch semantics: omitted fields keep their current value. Supplied
    /// value keys are the changed columns.
    Patch(VersionedCellValues),
    /// After-image semantics: supplied values are the complete proposed row.
    AfterImage(VersionedCellValues),
}

/// Cell values tagged with the schema used to interpret them.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct VersionedCellValues {
    /// Schema version used to interpret the values.
    pub(crate) schema: SchemaVersionId,
    /// Application cells.
    pub(crate) values: BTreeMap<String, Value>,
}

/// Identity, claims, and policy mode used by policy augmentation.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum PolicyContext {
    /// Internal/system reads bypass row-level policy.
    System,
    /// Authenticated identity plus trusted server/session claims.
    Identity {
        /// Missing-policy behavior.
        mode: PolicyEnforcementMode,
        /// Identity whose permissions are being evaluated.
        permission_subject: AuthorId,
        /// Trusted claims available to policy queries.
        claims: BTreeMap<String, Value>,
        /// Author recorded on writes, when it differs from the permission subject.
        attribution: Option<AuthorId>,
    },
    /// A policy-authorization subplan evaluates claim-dependent policy logic
    /// for an identity, but its own source reads are system-authorized to avoid
    /// recursively applying the same row policy to the policy proof.
    AuthorizationSubplan {
        /// Missing-policy behavior.
        mode: PolicyEnforcementMode,
        /// Identity whose permissions are being evaluated.
        permission_subject: AuthorId,
        /// Trusted claims available to policy queries.
        claims: BTreeMap<String, Value>,
        /// Author recorded on writes, when it differs from the permission subject.
        attribution: Option<AuthorId>,
    },
}

/// Missing-policy behavior.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum PolicyEnforcementMode {
    /// Local/offline runtimes without a compiled policy bundle remain usable.
    PermissiveLocal,
    /// Enforcing runtimes fail closed for missing explicit policy.
    Enforcing,
}

/// Stable policy identity used to decide whether compiled programs can share work.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum PolicySharingKey {
    /// Internal/system policy bypass.
    System,
    /// Authenticated policy context. Claim values are runtime parameters; this
    /// key records only the claim paths that the lowered graph depends on.
    Identity {
        /// Missing-policy behavior.
        mode: PolicyEnforcementMode,
        /// Identity whose permissions are being evaluated.
        permission_subject: AuthorId,
        /// Identity recorded on writes, when it differs from the permission subject.
        attribution: Option<AuthorId>,
        /// Trusted claim paths referenced by the lowered graph.
        claim_paths: BTreeSet<ClaimPath>,
    },
}
