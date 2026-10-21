use super::*;
use std::{future::Future, pin::Pin};

/// A lookup whose keys come from a constant or maintained relation.
/// Fields name the matching columns in both the source graph and key relation.
/// The source owner selects a compatible physical access path or rejects it.
#[derive(Clone, Debug, PartialEq)]
pub struct SourceLookup {
    /// Matching source fields, in key order.
    pub fields: Vec<String>,
    /// Constant or maintained rows with those named fields.
    pub keys: GraphBuilder,
}

/// Logical source request made by query, policy, or fact lowering.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceRequest {
    /// Logical source requested by the compiler.
    pub source: SourceId,
    /// Query-visible row scope expected from this source.
    pub visibility: RowVisibility,
    /// Authorization semantics that must be applied to this source before it
    /// participates in the program.
    pub authorization: SourceAuthorizationRequest,
    /// Structural row metadata required by all consumers of this source.
    pub requirements: SourceRequirements,
}

/// Source authorization requested by query-engine lowering.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum SourceAuthorizationRequest {
    /// System/internal program. The source is already authorized by the caller.
    #[default]
    System,
    /// User-visible source filtered by the active policy context.
    PolicyFiltered {
        /// Identity whose row-level read permission gates the source.
        permission_subject: AuthorSubject,
        /// Query-engine-owned authorization plan for the protected source.
        plan: PolicyAuthorizationPlan,
    },
    /// A membership-only authorization proof used while compiling another
    /// table's policy. Unlike `PolicyFiltered`, this never re-enters ordinary
    /// user-visible source resolution.
    PolicyProof {
        /// Identity whose row-level read permission gates the proof source.
        permission_subject: AuthorSubject,
        /// Query-engine-owned authorization plan for the proof source.
        plan: PolicyAuthorizationPlan,
    },
}

/// Logical authorization requirement for one protected source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolicyAuthorizationPlan {
    /// Protected source whose rows are gated by this policy proof.
    pub protected_source: SourceId,
    /// Decision role requested for the protected source.
    pub role: PolicyDecisionRole,
    /// Row id field in the protected source graph.
    pub protected_row_field: String,
    /// Binding-source shape shared with the enclosing prepared program.
    pub binding_source_shape: Option<String>,
    /// User params from the enclosing prepared program that must be present in
    /// the shared binding descriptor.
    pub binding_user_params: BTreeMap<String, ColumnType>,
    /// Typed claim slots from the enclosing prepared program. Nested policy
    /// plans share this binding descriptor, so they must retain claims used by
    /// an ancestor policy branch as claims, rather than reclassifying them as
    /// ordinary parameters.
    pub binding_claim_params: BTreeMap<String, ProgramClaimParam>,
}

/// Orthogonal source row requirements derived from app output and requested
/// facts. This avoids resolver behavior switches such as "policy source" or
/// "delivery source"; every need is explicit.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct SourceRequirements {
    /// Public/app fields needed by output projection.
    pub app_fields: FieldRequirement,
    /// Internal metadata needed by facts, sync, transaction validation, and
    /// policy witnesses.
    pub metadata: BTreeSet<SourceMetadataRequirement>,
}

/// Internal source metadata requirement.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SourceMetadataRequirement {
    /// Include version identity fields on source rows.
    VersionWitnesses,
    /// Include nullable global settle position for current winners.
    SettlePosition,
    /// Provide canonical content-version payload rows for witness terminals.
    VersionPayloads,
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
    /// Project one nested structured-author field only when requested.
    AuthorPath(String),
    /// Expose the current winning deletion's authored timestamp.
    DeletedAt,
}

/// Public field requirement for a source.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub enum FieldRequirement {
    /// No app-facing fields are required from this source.
    #[default]
    None,
    /// Every public field in the read schema is required.
    All,
    /// Only these public fields are required.
    Fields(BTreeSet<String>),
}

/// Async preparation boundary that turns logical Jazz source requests into
/// owned, declarative Groove inputs before synchronous lowering begins.
///
/// Implementations may currently capture frozen snapshots or prepare physical
/// metadata. They must not execute the query being lowered. Live data loading
/// and residency belong to Groove evaluation, not this trait.
///
/// This is not the Groove runtime source resolver. New code must keep this
/// trait on the preparation side of `lower_resolved_query_program`.
pub trait SourceGraphPreparer {
    /// Preparation failures, including operational errors from the source owner.
    type Error;

    /// Whether this source needs keys supplied by a join input.
    fn requires_key_lookup(&self, _request: &SourceRequest) -> bool {
        false
    }

    /// Whether a join key can bound this source using an available access path.
    fn supports_lookup(&self, _request: &SourceRequest, _field: &str) -> bool {
        false
    }

    /// Prepare a source bounded by the supplied lookup.
    fn prepare_source_graph_with_lookup<'a>(
        &'a mut self,
        request: &'a SourceRequest,
        _lookup: SourceLookup,
    ) -> Pin<Box<dyn Future<Output = Result<ResolvedSource, Self::Error>> + 'a>> {
        self.prepare_source_graph(request)
    }

    /// Prepare one source request into a concrete Groove graph and row shape.
    fn prepare_source_graph<'a>(
        &'a mut self,
        request: &'a SourceRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ResolvedSource, Self::Error>> + 'a>>;
}

/// Concrete source selected for one logical source request.
#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedSource {
    /// Catalogue-owned IDs for logical columns in the selected read schema.
    pub stored_column_ids: BTreeMap<String, crate::ids::PhysicalColumnId>,
    /// Logical table schema after schema/lens resolution. It carries the
    /// table's compiled policies, which grow with the policy's branch count,
    /// so lowering shares it rather than copying it into every source
    /// occurrence and lowered step.
    pub table_schema: std::sync::Arc<TableSchema>,
    /// Concrete groove graph source.
    pub graph: GraphBuilder,
    /// Canonical row shape emitted by the source graph.
    pub row_shape: SourceRowShape,
    /// Hidden routing fields emitted by the source graph outside the app row
    /// descriptor.
    pub routing_fields: BTreeSet<String>,
    /// The public row is a view-relative rendering rather than necessarily
    /// the immutable content version named by its witness.
    pub requires_result_payload: bool,
    /// Content version rows for the same source, when version witnesses are
    /// requested explicitly.
    pub content_version: Option<ContentVersionSource>,
    /// Deletion register rows for the same source, when requested explicitly.
    pub deletion_register: Option<DeletionRegisterSource>,
    /// Current authorized deleted-row preimage for this same source
    /// occurrence. The deletion terminal semijoins its raw register witness
    /// against this graph, so a tombstone is never authorization by itself.
    pub authorized_deletion_preimage: Option<AuthorizedDeletionPreimage>,
}

/// The authorization proof for a deletion is route-scoped, not only row-scoped.
/// Keep the route contract with its graph so reusable programs cannot borrow
/// another binding's proof after projecting down to the deletion version key.
#[derive(Clone, Debug, PartialEq)]
pub struct AuthorizedDeletionPreimage {
    #[doc(hidden)]
    pub graph: GraphBuilder,
    #[doc(hidden)]
    pub routing_fields: BTreeSet<String>,
}

/// Concrete content-version source selected by node-side source resolution.
#[derive(Clone, Debug, PartialEq)]
pub struct ContentVersionSource {
    /// Graph emitting current content history rows with canonical storage fields.
    pub graph: GraphBuilder,
    /// Field containing row identity.
    pub row_uuid_field: String,
}

/// Concrete deletion-register source selected by node-side source resolution.
#[derive(Clone, Debug, PartialEq)]
pub struct DeletionRegisterSource {
    /// Graph emitting current deletion-register rows with canonical storage fields.
    pub graph: GraphBuilder,
    /// Field containing row identity.
    pub row_uuid_field: String,
}

/// Canonical row shape emitted by source resolution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceRowShape {
    /// Logical source emitted by this source.
    pub source: SourceId,
    /// Descriptor of the record emitted by this source.
    pub descriptor: RecordDescriptor,
    /// Complete identity of one occurrence emitted by this source.
    pub occurrence_key: SourceOccurrenceKey,
    /// Internal metadata fields emitted by this source, keyed by the matching
    /// requirement.
    pub metadata: BTreeMap<SourceMetadataRequirement, SourceMetadataFields>,
}

/// Field mapping for one source occurrence. The row UUID remains available for
/// application IDs, policy checks and storage lookups, but is not by itself the
/// identity of a historical occurrence. This definition travels with the source
/// and result schema; scalar fields are expanded only for graph operations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SourceOccurrenceKey {
    /// A projected historical occurrence, carried as one `(row, time, node)` tuple.
    Packed {
        /// Field containing the complete source occurrence.
        field: String,
    },
    /// A current row has one occurrence regardless of its content version.
    Current {
        /// Field containing the logical row UUID.
        row: String,
    },
    /// Each retained content transaction is a separate occurrence.
    History {
        /// Field containing the logical row UUID.
        row: String,
        /// Field containing the transaction's packed HLC timestamp.
        time: String,
        /// Field containing the originating node's portable UUID.
        node: String,
    },
}

impl SourceOccurrenceKey {
    /// Name the hidden identity field for one source in a flat join.
    pub fn joined_field(position: usize) -> String {
        format!("__flat_join_row_{position}")
    }

    /// Describe this identity after projection into one hidden field.
    pub fn projected(&self, field: impl Into<String>) -> Self {
        match self {
            Self::Current { .. } => Self::current(field),
            Self::History { .. } | Self::Packed { .. } => Self::Packed {
                field: field.into(),
            },
        }
    }

    /// Whether a field can carry a complete projected source identity.
    pub fn is_value_type(ty: &groove::records::ValueType) -> bool {
        *ty == groove::records::ValueType::Uuid || *ty == Self::packed_type()
    }

    fn packed_type() -> groove::records::ValueType {
        use groove::records::ValueType;
        ValueType::Tuple(vec![ValueType::Uuid, ValueType::U64, ValueType::Uuid])
    }

    /// Decode a hidden source identity; current sources use a UUID scalar.
    pub fn decode_value(
        value: groove::records::Value,
    ) -> Option<jazz_types::object::SourceOccurrenceId> {
        use groove::records::Value;
        use jazz_types::object::{ObjectId, SourceOccurrenceId};
        match value {
            Value::Uuid(row) => Some(SourceOccurrenceId::current(ObjectId::from_uuid(row))),
            Value::Tuple(values) => match values.as_slice() {
                [Value::Uuid(row), Value::U64(time), Value::Uuid(node)] => {
                    Some(SourceOccurrenceId::history(
                        ObjectId::from_uuid(*row),
                        crate::time::TxTime(*time),
                        crate::ids::NodeUuid(*node),
                    ))
                }
                _ => None,
            },
            _ => None,
        }
    }

    /// Decode this key through its declared physical fields.
    pub fn decode(
        &self,
        read: impl Fn(&str) -> Option<groove::records::Value>,
    ) -> Option<jazz_types::object::SourceOccurrenceId> {
        use groove::records::Value;
        Self::decode_value(match self {
            Self::Current { row } => match read(row)? {
                value @ Value::Uuid(_) => value,
                _ => return None,
            },
            Self::Packed { field } => match read(field)? {
                value @ Value::Tuple(_) => value,
                _ => return None,
            },
            Self::History { row, time, node } => {
                Value::Tuple(vec![read(row)?, read(time)?, read(node)?])
            }
        })
    }

    /// Project the whole identity, resolving source fields through the join layout.
    pub fn project(
        &self,
        output: String,
        resolve: impl Fn(&str) -> String,
    ) -> groove::ivm::ProjectField {
        use groove::ivm::{FieldRef, ProjectExpr, ProjectField};
        let mut field = ProjectField::named(output);
        field.expression = match self {
            Self::Current { row } => ProjectExpr::Field(FieldRef::name(resolve(row))),
            Self::Packed { field } => ProjectExpr::Field(FieldRef::name(resolve(field))),
            Self::History { row, time, node } => ProjectExpr::Tuple(
                [row, time, node]
                    .into_iter()
                    .map(|name| FieldRef::name(resolve(name)))
                    .collect(),
            ),
        };
        field
    }

    /// Declare the occurrence key of an ordinary current-row source.
    pub fn current(row: impl Into<String>) -> Self {
        Self::Current { row: row.into() }
    }

    /// Leading identity field: the row UUID for source keys, or the whole
    /// tuple carrier for a packed projected key.
    pub fn row_field(&self) -> &String {
        match self {
            Self::Current { row } | Self::History { row, .. } => row,
            Self::Packed { field } => field,
        }
    }

    /// Ordered physical fields for graph grouping, ordering and projection.
    pub fn fields(&self) -> Vec<(&String, groove::records::ValueType)> {
        use groove::records::ValueType;
        match self {
            Self::Packed { field } => vec![(field, Self::packed_type())],
            Self::Current { row } => vec![(row, ValueType::Uuid)],
            Self::History { row, time, node } => vec![
                (row, ValueType::Uuid),
                (time, ValueType::U64),
                (node, ValueType::Uuid),
            ],
        }
    }
}

/// Concrete source metadata fields emitted for one requirement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SourceMetadataFields {
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
    /// Nullable global settle position field.
    SettlePosition {
        /// Field containing the global timestamp where the member settled.
        settle_position_field: String,
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
pub struct SourceResolutionError {
    /// Source request that failed.
    pub request: Box<SourceRequest>,
    /// Explicit unsupported source shape.
    pub gap: SourceGap,
}

/// Source-resolution gap.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SourceGap {
    /// A scoped local-availability input could not be installed (including capacity).
    LocalAvailabilityInput,
    /// Recursive policy proof compilation revisited a table already on the
    /// proof stack. This must surface as a diagnostic rather than consuming
    /// the process stack.
    PolicyProofCycle {
        /// Revisited policy table.
        table: String,
        /// Stack depth at the attempted re-entry.
        depth: usize,
    },
    /// Storage source for a historical global cut cannot yet be built.
    HistoricalStorageCut,
    /// Snapshot source includes local overlays or dots not yet represented.
    SnapshotRef,
    /// Required frozen branch history is missing, rejected, or not complete here.
    SnapshotHistoryUnavailable,
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
pub struct CapabilityReport {
    /// Unsupported pieces. An empty list means the requested program is supported.
    pub gaps: Vec<UnsupportedReason>,
    /// Human-readable debugging and test artifact for the failed lowering.
    pub explain: ExplainPlan,
}

/// Result type for query-engine capability checks. The report is intentionally
/// rich enough for design/test diagnostics, so keep it boxed at API boundaries.
pub type CapabilityResult<T> = Result<T, Box<CapabilityReport>>;

impl CapabilityReport {
    /// A report with one runtime-contract gap and no explanation.
    pub(crate) fn runtime(message: impl Into<String>) -> Box<Self> {
        Self::single(UnsupportedReason::Runtime(message.into()))
    }

    /// A report with one unrepresented-operator gap and no explanation.
    pub(crate) fn operator(message: impl Into<String>) -> Box<Self> {
        Self::single(UnsupportedReason::Operator(message.into()))
    }

    fn single(gap: UnsupportedReason) -> Box<Self> {
        Box::new(Self {
            gaps: vec![gap],
            explain: ExplainPlan::default(),
        })
    }

    /// A report with one gap and the capability that explains it.
    pub(crate) fn explained(gap: UnsupportedReason, capability: impl Into<String>) -> Box<Self> {
        Box::new(Self {
            gaps: vec![gap],
            explain: ExplainPlan {
                capabilities: vec![capability.into()],
                ..ExplainPlan::default()
            },
        })
    }
}

impl ResolvedSource {
    /// Look up a source the lowering already resolved; `role` names it in the
    /// "{role} source {id:?} was not resolved" runtime gap.
    pub(crate) fn lookup<'a>(
        sources: &'a BTreeMap<SourceId, ResolvedSource>,
        id: &SourceId,
        role: &str,
    ) -> CapabilityResult<&'a Self> {
        sources.get(id).ok_or_else(|| {
            CapabilityReport::runtime(format!("{role} source {id:?} was not resolved"))
        })
    }
}

/// Reason a request is not yet supported by unified lowering.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UnsupportedReason {
    /// Source/frontier/schema view is not yet representable.
    Source(SourceGap),
    /// A policy/session claim was referenced but not present in the policy context.
    UnboundClaim(ClaimPath),
    /// Query/relation operator is not yet represented.
    Operator(String),
    /// Requested output fact is not yet emitted.
    Output(Box<ProgramFactKey>),
    /// Runtime contract is not yet connected to the lowered graph.
    Runtime(String),
}

/// Debug artifact for query-engine tests and design audits.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExplainPlan {
    /// Normalized input summary.
    pub input: String,
    /// Source/frontier decisions.
    pub read: Vec<String>,
    /// Policy rewrite decisions.
    pub policy: Vec<String>,
    /// Output/fact decisions.
    pub output: Vec<String>,
    /// Capability decisions.
    pub capabilities: Vec<String>,
    /// Physical graph summaries.
    pub physical: Vec<String>,
}
