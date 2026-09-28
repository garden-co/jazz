use super::*;

/// Row-set output request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowSetOutputRequest {
    /// App-facing rows requested from the shared row-set program, if any.
    pub app_rows: Option<AppRowOutputRequest>,
    /// Internal facts requested by sync, transaction validation, policy
    /// dependency tracking, or binding-route maintenance.
    pub facts: BTreeSet<ProgramFactKey>,
}

/// App-facing row payload request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppRowOutputRequest {
    /// Whether this is an app boundary terminal. Internal policy predicates
    /// consume only filtered row identity and binding routes, without
    /// materializing unrelated candidate payloads.
    pub public_terminal: bool,
    /// Public payload projection requested at the app boundary.
    pub projection: PayloadProjection,
}

/// Public app-row payload projection.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum PayloadProjection {
    /// Use the projection implied by the normalized shape root.
    ShapeDefault,
    /// Explicit output aliases from a relation projection.
    Relation(Vec<crate::query::RelationProjectColumn>),
    /// Explicit nested app projection tree.
    Tree(AppProjectionTree),
}

/// Explicit nested app-row projection tree.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AppProjectionTree {
    /// Root public field projection.
    pub fields: FieldProjection,
    /// Nested path/relation projections.
    pub paths: Vec<AppPathProjection>,
}

/// Field projection for app rows.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum FieldProjection {
    /// Materialize all public fields available at the read schema.
    All,
    /// Materialize this explicit field set.
    Fields(BTreeSet<String>),
}

/// Nested path/relation projection.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AppPathProjection {
    /// Internal path identity.
    pub path: ProgramPathId,
    /// Public field name used in app rows.
    pub field: String,
    /// Path cardinality.
    pub cardinality: PathCardinality,
    /// Child public field projection.
    pub fields: FieldProjection,
    /// Nested child path projections.
    pub children: Vec<AppPathProjection>,
    /// What app projection does when hidden relation facts show incomplete/null
    /// child coverage.
    pub hole_policy: PathHolePolicy,
}

/// App path cardinality.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PathCardinality {
    /// At most one child.
    One,
    /// Many children.
    Many,
}

/// App-row handling for missing/incomplete path rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PathHolePolicy {
    /// Keep the parent and materialize null/empty placeholders.
    KeepParentWithHoles,
    /// Drop parent rows whose required path is incomplete.
    DropIncompleteParent,
}

/// Stable key for safe sharing of compiled semantic work.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ProgramSharingKey {
    /// Binding-independent normalized row-set shape id.
    pub shape_id: ShapeId,
    /// Resolved read/source identity.
    pub reads: ResolvedReadSet,
    /// Policy identity and claims.
    pub policy: PolicySharingKey,
}

/// Stable key for one semantic program instance before usage-site subscription
/// handle assignment.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ProgramInstanceKey {
    /// Shared compiled semantic work.
    pub program: ProgramSharingKey,
    /// Binding values identity for this instance.
    pub binding_id: BindingId,
}

/// Stable output identity. This can vary independently from
/// [`ProgramSharingKey`] when two consumers share one semantic graph but need
/// different sinks/facts.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ProgramOutputKey {
    /// Canonical identity derived from [`RowSetOutputRequest`]. The request is
    /// the semantic source of truth; this key is only for cache/output sharing.
    pub fingerprint: Vec<u8>,
}

/// Stable fact-output identity.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ProgramFactKey {
    /// Row ids authorized by a policy proof subplan.
    AuthorizedRows,
    /// Root result-set membership rows.
    ResultMembership,
    /// Relation edge rows.
    RelationEdges,
    /// Per-path correlation/cardinality coverage rows.
    PathCorrelationCoverage,
    /// Complete source-closure receipts for a scope.
    ProgramSourceCoverage(CoverageScope),
    /// Settled read-frontier signal for a concrete scope/frontier.
    ReadFrontierSettled(CoverageFrontier),
    /// Full transaction payload coverage for one concrete batch/transaction.
    CompleteTxPayloadCoverage {
        /// Concrete batch identity.
        batch: BatchId,
        /// Requested durability tier.
        tier: DurabilityTier,
    },
    /// View-complete exclusive transaction coverage for one result or scope.
    ViewCompleteExclusiveCoverage {
        /// View/source scope.
        view: CoverageScope,
        /// Result whose contributing members define completeness, if result-scoped.
        result: Option<ResultId>,
        /// Requested durability tier.
        tier: DurabilityTier,
    },
    /// Content/deletion/replacement version witnesses.
    VersionWitnesses,
    /// Replacement candidates for rows removed from a maintained result set.
    ReplacementWitnesses,
    /// Tri-state dry-run policy decision facts.
    PolicyDecision {
        /// Decision identity within the normalized row-set program.
        decision: PolicyDecisionFactKey,
    },
    /// Policy dependency witnesses.
    PolicyWitnesses,
    /// Internal contributing member/batch provenance for derived outputs.
    ContributingMembers,
    /// Predicate-read facts for exclusive transaction validation.
    PredicateReads,
    /// Concrete predicate output set used by exclusive validation.
    PredicateOutputSet {
        /// Which side of the validation comparison this terminal emits.
        role: PredicateOutputSetRole,
    },
    /// Point row-read facts.
    #[doc(hidden)]
    PointReads { present: bool },
}

/// Stable identity for one policy-decision terminal fact.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PolicyDecisionFactKey {
    /// Decision role inside the normalized row-set program.
    pub role: PolicyDecisionRole,
    /// Canonical decision/candidate fingerprint.
    pub fingerprint: Vec<u8>,
}

/// Coarse policy-decision role.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PolicyDecisionRole {
    /// Existing-row visibility check.
    Read,
    /// Proposed insert/update/delete/restore.
    Write,
}
