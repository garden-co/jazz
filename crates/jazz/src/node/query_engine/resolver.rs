use super::*;

/// Logical source request made by query, policy, or fact lowering.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SourceRequest {
    /// Logical source requested by the compiler.
    pub(crate) source: SourceId,
    /// Query-visible row scope expected from this source.
    pub(crate) visibility: RowVisibility,
    /// Authorization semantics that must be applied to this source before it
    /// participates in the program.
    pub(crate) authorization: SourceAuthorizationRequest,
    /// Structural row metadata required by all consumers of this source.
    pub(crate) requirements: SourceRequirements,
}

/// Source authorization requested by query-engine lowering.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) enum SourceAuthorizationRequest {
    /// System/internal program. The source is already authorized by the caller.
    #[default]
    System,
    /// User-visible source filtered by the active policy context.
    PolicyFiltered {
        /// Identity whose row-level read permission gates the source.
        permission_subject: AuthorId,
        /// Query-engine-owned authorization plan for the protected source.
        plan: PolicyAuthorizationPlan,
    },
}

/// Logical authorization requirement for one protected source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PolicyAuthorizationPlan {
    /// Protected source whose rows are gated by this policy proof.
    pub(crate) protected_source: SourceId,
    /// Decision role requested for the protected source.
    pub(crate) role: PolicyDecisionRole,
    /// Row id field in the protected source graph.
    pub(crate) protected_row_field: String,
    /// Binding-source shape shared with the enclosing prepared program.
    pub(crate) binding_source_shape: Option<String>,
    /// User params from the enclosing prepared program that must be present in
    /// the shared binding descriptor.
    pub(crate) binding_user_params: BTreeMap<String, ColumnType>,
}

/// Orthogonal source row requirements derived from app output and requested
/// facts. This avoids resolver behavior switches such as "policy source" or
/// "delivery source"; every need is explicit.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct SourceRequirements {
    /// Public/app fields needed by output projection.
    pub(crate) app_fields: FieldRequirement,
    /// Internal metadata needed by facts, sync, transaction validation, and
    /// policy witnesses.
    pub(crate) metadata: BTreeSet<SourceMetadataRequirement>,
}

/// Internal source metadata requirement.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum SourceMetadataRequirement {
    /// Include version identity fields for payload/replacement witnesses.
    VersionWitnesses,
    /// Include deletion-register/deletion-marker state.
    DeletionMarkers,
    /// Include batch/member identity and digest fields.
    BatchMembership,
    /// Include coverage/index-range fields.
    Coverage,
    /// Include predicate/point-read validation metadata.
    ValidationReads,
    /// Include policy dependency witness fields.
    PolicyWitnesses,
    /// Include one public provenance field.
    Provenance(ProvenanceField),
}

/// Public field requirement for a source.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) enum FieldRequirement {
    /// No app-facing fields are required from this source.
    #[default]
    None,
    /// Every public field in the read schema is required.
    All,
    /// Only these public fields are required.
    Fields(BTreeSet<String>),
}

/// Resolver that turns logical Jazz source requests into concrete groove inputs.
pub(crate) trait SourceResolver {
    /// Resolve a source request into a concrete groove graph and row shape.
    fn resolve_source(
        &mut self,
        request: &SourceRequest,
    ) -> Result<ResolvedSource, SourceResolutionError>;
}

/// Concrete source selected for one logical source request.
#[derive(Clone, Debug)]
pub(crate) struct ResolvedSource {
    /// Logical table schema after schema/lens resolution.
    pub(crate) table_schema: TableSchema,
    /// Concrete groove graph source.
    pub(crate) graph: GraphBuilder,
    /// Canonical row shape emitted by the source graph.
    pub(crate) row_shape: SourceRowShape,
}

/// Canonical row shape emitted by source resolution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SourceRowShape {
    /// Logical source emitted by this source.
    pub(crate) source: SourceId,
    /// Descriptor of the record emitted by this source.
    pub(crate) descriptor: RecordDescriptor,
    /// Field containing row identity.
    pub(crate) row_uuid_field: String,
    /// Internal metadata fields emitted by this source, keyed by the matching
    /// requirement.
    pub(crate) metadata: BTreeMap<SourceMetadataRequirement, SourceMetadataFields>,
}

/// Concrete source metadata fields emitted for one requirement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SourceMetadataFields {
    /// Version identity fields for payload/replacement witnesses.
    VersionWitnesses {
        /// Schema version field.
        schema_version_field: String,
        /// Content transaction time field.
        tx_time_field: String,
        /// Content transaction node field.
        tx_node_field: String,
        /// Branch/prefix identity field, when present.
        branch_or_prefix_field: Option<String>,
    },
    /// Deletion-register/deletion-marker fields.
    DeletionMarkers {
        /// Field distinguishing deleted/live/restored state.
        deletion_state_field: String,
        /// Deletion transaction time field, when present.
        deletion_tx_time_field: Option<String>,
        /// Deletion transaction node field, when present.
        deletion_tx_node_field: Option<String>,
    },
    /// Batch/member identity and digest fields.
    BatchMembership {
        /// Batch identity field.
        batch_id_field: String,
        /// Branch/prefix identity field, when present.
        branch_or_prefix_field: Option<String>,
        /// Visible row/member digest field.
        row_digest_field: String,
        /// Field distinguishing direct, accepted transaction, and staging batches.
        batch_kind_field: String,
    },
    /// Coverage/index-range fields.
    Coverage {
        /// Coverage field emitted by the source.
        coverage_field: String,
    },
    /// Predicate/point-read validation metadata.
    ValidationReads {
        /// Observed/base snapshot field.
        snapshot_field: String,
    },
    /// Policy dependency witness fields.
    PolicyWitnesses {
        /// Policy clause/path field.
        policy_path_field: String,
        /// Dependency edge kind field.
        edge_kind_field: String,
    },
    /// Public provenance field.
    Provenance {
        /// Field emitted by the source for this provenance value.
        field: String,
    },
}

/// Source resolution failure that must not fall back to a different engine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SourceResolutionError {
    /// Source request that failed.
    pub(crate) request: Box<SourceRequest>,
    /// Explicit unsupported source shape.
    pub(crate) gap: SourceGap,
}

/// Source-resolution gap.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SourceGap {
    /// Storage source for a historical global cut cannot yet be built.
    HistoricalStorageCut,
    /// Snapshot source includes local overlays or dots not yet represented.
    SnapshotRef,
    /// Schema/lens fanout or projection cannot yet be represented.
    SchemaProjection,
    /// Branch overlay source cannot yet be represented.
    BranchOverlay,
    /// Transaction overlay source cannot yet be represented.
    TransactionReadOverlay,
    /// Required sync/topology coverage cannot yet be established.
    Coverage,
}

/// Capability status for an unsupported requested program.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct CapabilityReport {
    /// Unsupported pieces. An empty list means the requested program is supported.
    pub(crate) gaps: Vec<UnsupportedReason>,
    /// Human-readable debugging and test artifact for the failed lowering.
    pub(crate) explain: ExplainPlan,
}

/// Result type for query-engine capability checks. The report is intentionally
/// rich enough for design/test diagnostics, so keep it boxed at API boundaries.
pub(crate) type CapabilityResult<T> = Result<T, Box<CapabilityReport>>;

impl CapabilityReport {
    /// Whether the requested program can run on the unified lowering path.
    pub(crate) fn is_supported(&self) -> bool {
        self.gaps.is_empty()
    }
}

/// Reason a request is not yet supported by unified lowering.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum UnsupportedReason {
    /// Source/frontier/schema view is not yet representable.
    Source(SourceGap),
    /// Query/relation operator is not yet represented.
    Operator(String),
    /// Requested output fact is not yet emitted.
    Output(Box<ProgramFactKey>),
    /// Policy composition is not yet lowered.
    Policy(String),
    /// Runtime contract is not yet connected to the lowered graph.
    Runtime(String),
}

/// Debug artifact for query-engine tests and design audits.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ExplainPlan {
    /// Normalized input summary.
    pub(crate) input: String,
    /// Source/frontier decisions.
    pub(crate) read: Vec<String>,
    /// Policy rewrite decisions.
    pub(crate) policy: Vec<String>,
    /// Output/fact decisions.
    pub(crate) output: Vec<String>,
    /// Capability decisions.
    pub(crate) capabilities: Vec<String>,
    /// Physical graph summaries.
    pub(crate) physical: Vec<String>,
}
