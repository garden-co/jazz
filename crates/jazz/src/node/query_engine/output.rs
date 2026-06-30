use super::*;

/// Row-set output request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RowSetOutputRequest {
    /// App-facing rows requested from the shared row-set program, if any.
    pub(crate) app_rows: Option<AppRowOutputRequest>,
    /// Internal facts requested by sync, transaction validation, policy
    /// dependency tracking, or binding-route maintenance.
    pub(crate) facts: BTreeSet<ProgramFactKey>,
}

/// App-facing row payload request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AppRowOutputRequest {
    /// Public payload projection requested at the app boundary.
    pub(crate) projection: PayloadProjection,
    /// Large-value materialization requested for app rows.
    pub(crate) large_values: Vec<LargeValueRequest>,
}

/// Public app-row payload projection.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum PayloadProjection {
    /// Use the projection implied by the normalized shape root.
    ShapeDefault,
    /// Explicit nested app projection tree.
    Tree(AppProjectionTree),
}

/// Explicit nested app-row projection tree.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct AppProjectionTree {
    /// Root public field projection.
    pub(crate) fields: FieldProjection,
    /// Nested path/relation projections.
    pub(crate) paths: Vec<AppPathProjection>,
}

/// Field projection for app rows.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum FieldProjection {
    /// Materialize all public fields available at the read schema.
    All,
    /// Materialize this explicit field set.
    Fields(BTreeSet<String>),
}

/// Nested path/relation projection.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct AppPathProjection {
    /// Internal path identity.
    pub(crate) path: ProgramPathId,
    /// Public field name used in app rows.
    pub(crate) field: String,
    /// Path cardinality.
    pub(crate) cardinality: PathCardinality,
    /// Child public field projection.
    pub(crate) fields: FieldProjection,
    /// Nested child path projections.
    pub(crate) children: Vec<AppPathProjection>,
    /// What app projection does when hidden relation facts show incomplete/null
    /// child coverage.
    pub(crate) hole_policy: PathHolePolicy,
    /// Large-value materialization requested for this path child.
    pub(crate) large_values: Vec<LargeValueRequest>,
}

/// App path cardinality.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum PathCardinality {
    /// At most one child.
    One,
    /// Many children.
    Many,
}

/// App-row handling for missing/incomplete path rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum PathHolePolicy {
    /// Keep the parent and materialize null/empty placeholders.
    KeepParentWithHoles,
    /// Drop parent rows whose required path is incomplete.
    DropIncompleteParent,
}

/// Large-value materialization requested relative to the enclosing result/path.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct LargeValueRequest {
    /// Column names.
    pub(crate) columns: BTreeSet<String>,
    /// Materialization mode.
    pub(crate) materialization: LargeValueMaterialization,
    /// Byte ranges requested for materialization.
    pub(crate) ranges: LargeValueRangeRequest,
}

/// Large-value materialization mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum LargeValueMaterialization {
    /// Inline bytes into the row-record payload.
    Inline,
    /// Return handles only.
    Handle,
    /// Return authorized extent descriptors.
    Extents,
}

/// Large-value byte range request.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum LargeValueRangeRequest {
    /// Entire value.
    WholeValue,
    /// Explicit ranges.
    Ranges(Vec<ByteRange>),
}

/// Byte range.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ByteRange {
    /// Inclusive start byte.
    pub(crate) start: u64,
    /// Exclusive end byte, or `None` for EOF.
    pub(crate) end: Option<u64>,
}

/// Stable key for safe sharing of compiled semantic work.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ProgramSharingKey {
    /// Binding-independent normalized row-set shape id.
    pub(crate) shape_id: ShapeId,
    /// Resolved read/source identity.
    pub(crate) reads: ResolvedReadSet,
    /// Policy identity and claims.
    pub(crate) policy: PolicySharingKey,
}

/// Stable key for one semantic program instance before usage-site subscription
/// handle assignment.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ProgramInstanceKey {
    /// Shared compiled semantic work.
    pub(crate) program: ProgramSharingKey,
    /// Binding values identity for this instance.
    pub(crate) binding_id: BindingId,
}

/// Stable output identity. This can vary independently from
/// [`ProgramSharingKey`] when two consumers share one semantic graph but need
/// different sinks/facts.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ProgramOutputKey {
    /// Canonical identity derived from [`RowSetOutputRequest`]. The request is
    /// the semantic source of truth; this key is only for cache/output sharing.
    pub(crate) fingerprint: Vec<u8>,
}

/// Stable fact-output identity.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum ProgramFactKey {
    /// Root result-set membership rows.
    ResultMembership,
    /// Relation edge rows.
    RelationEdges,
    /// Per-path correlation/cardinality coverage rows.
    PathCorrelationCoverage,
    /// Source/table coverage facts for a scope.
    SourceCoverage(CoverageScope),
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
    PointReads { present: bool },
    /// Authorized/materialized large-value extents.
    LargeValueExtents {
        /// Owner source.
        owner: ResultId,
        /// Requested large-value materialization.
        request: LargeValueRequest,
    },
}

/// Stable identity for one policy-decision terminal fact.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct PolicyDecisionFactKey {
    /// Decision role inside the normalized row-set program.
    pub(crate) role: PolicyDecisionRole,
    /// Canonical decision/candidate fingerprint.
    pub(crate) fingerprint: Vec<u8>,
}

/// Coarse policy-decision role.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum PolicyDecisionRole {
    /// Existing-row visibility check.
    Read,
    /// Proposed insert/update/delete/restore.
    Write,
}
