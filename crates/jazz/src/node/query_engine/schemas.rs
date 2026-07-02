use super::*;

/// Typed output schemas emitted by one lowered graph.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ProgramOutputSchemas {
    /// Row-set app-row and fact outputs.
    RowSet(Vec<OutputTerminalSchema>),
}

/// Typed schema for one emitted output terminal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum OutputTerminalSchema {
    /// App-facing rows.
    AppRows(AppRowSchema),
    /// Internal fact rows.
    Fact(ProgramFactOutput),
}

/// App-facing row schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AppRowSchema {
    /// Descriptor for app-visible row records.
    pub(crate) descriptor: RecordDescriptor,
    /// Hidden fields retained by the graph and stripped before app delivery.
    pub(crate) hidden_fields: BTreeSet<String>,
}

/// Typed fact row schema plus the fact identity that requested it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProgramFactOutput {
    /// Requested fact identity.
    pub(crate) key: ProgramFactKey,
    /// Concrete terminal role emitted for this fact.
    pub(crate) terminal: ProgramFactTerminal,
    /// Emitted row schema.
    pub(crate) schema: ProgramFactSchema,
}

impl ProgramFactOutput {
    /// Return the stable fact key represented by this output schema.
    pub(crate) fn key(&self) -> ProgramFactKey {
        self.key.clone()
    }
}

/// Concrete terminal role emitted for a fact key.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum ProgramFactTerminal {
    /// Primary terminal for facts with a single output stream.
    #[default]
    Primary,
    /// Content-register version witnesses.
    VersionWitnessContent,
    /// Deletion-register version witnesses.
    VersionWitnessDeletion,
    /// Content-register replacement witnesses.
    ReplacementWitnessContent,
    /// Deletion-register replacement witnesses.
    ReplacementWitnessDeletion,
}

/// Schema-only fact output variants.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ProgramFactSchema {
    /// Row ids authorized by a policy proof subplan.
    AuthorizedRows(AuthorizedRowsSchema),
    /// Root result-set membership rows.
    ResultMembership(ResultMembershipSchema),
    /// Relation edge rows.
    RelationEdges(RelationEdgeSchema),
    /// Per-path correlation/cardinality coverage rows.
    PathCorrelationCoverage(PathCorrelationCoverageSchema),
    /// Source/table coverage facts.
    SourceCoverage(SourceCoverageSchema),
    /// Settled read-frontier/query coverage signal.
    ReadFrontierSettled(ReadFrontierSettledSchema),
    /// Full transaction payload coverage.
    CompleteTxPayloadCoverage(CompleteTxPayloadCoverageSchema),
    /// View-complete exclusive transaction coverage.
    ViewCompleteExclusiveCoverage(ViewCompleteExclusiveCoverageSchema),
    /// Content/deletion/replacement version witnesses needed for payload
    /// shipping and removal/change replacement.
    VersionWitnesses(VersionWitnessSchemas),
    /// Replacement candidates for maintained-view removals.
    ReplacementWitnesses(VersionWitnessSchemas),
    /// Tri-state dry-run policy decision.
    PolicyDecision(PolicyDecisionSchema),
    /// Policy dependency witnesses that can grant or revoke visibility.
    PolicyWitnesses(PolicyWitnessSchema),
    /// Internal contributing member/batch provenance for derived outputs.
    ContributingMembers(ContributingMembersSchema),
    /// Predicate-read facts for exclusive transaction validation.
    PredicateReads(PredicateReadFactSchema),
    /// Concrete predicate output rows compared during exclusive validation.
    PredicateOutputSet(PredicateOutputSetSchema),
    /// Point row-read facts for exclusive transaction validation.
    PointReads(PointReadFactSchema),
    /// Large-value authorization/materialization extents.
    LargeValueExtents(LargeValueExtentSchema),
}

/// Authorized row-id terminal row schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AuthorizedRowsSchema {
    /// Row identity field.
    pub(crate) row_field: String,
    /// Hidden routing fields retained for prepared/multisink partitioning.
    pub(crate) routing_param_fields: BTreeSet<String>,
}

/// Root result membership terminal row schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ResultMembershipSchema {
    /// Logical table field in the terminal row.
    pub(crate) table_field: String,
    /// Row identity field in the terminal row.
    pub(crate) row_field: String,
    /// Branch/prefix field, when branch/prefix participates in result
    /// identity.
    pub(crate) branch_or_prefix_field: Option<String>,
    /// Version fields that identify the result row in the maintained set.
    pub(crate) version: ResultMembershipVersionSchema,
    /// Nullable global settle position for the member's visible current winner.
    pub(crate) settle_position_field: Option<String>,
    /// Retained binding/routing parameter fields.
    pub(crate) routing_param_fields: BTreeSet<String>,
}

/// Lightweight row reference schema for internal facts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RowRefSchema {
    /// Logical source field.
    pub(crate) source_field: String,
    /// Logical table field.
    pub(crate) table_field: String,
    /// Row identity field.
    pub(crate) row_field: String,
}

/// Versioned row reference schema for internal facts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct VersionedRowRefSchema {
    /// Row reference.
    pub(crate) row: RowRefSchema,
    /// Version fields, when the fact needs a concrete visible version.
    pub(crate) version: Option<ResultMembershipVersionSchema>,
}

/// Version identity carried by a result membership row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ResultMembershipVersionSchema {
    /// Visible content-row identity: `(table, row, content_tx_id)`.
    Content(ContentVersionFields),
    /// Include-deleted root identity. Tombstones may have no visible content
    /// winner, so the deletion-register version participates in membership.
    ContentOrDeletion {
        /// Nullable visible content version fields.
        content: ContentVersionFields,
        /// Deletion-register version fields.
        deletion: VersionIdentityFields,
        /// Field carrying deleted vs live state.
        deletion_state_field: String,
    },
}

/// Fields that identify the visible content version of a result row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ContentVersionFields {
    /// Transaction HLC field.
    pub(crate) tx_time_field: String,
    /// Transaction node field.
    pub(crate) tx_node_field: String,
}

/// Include/join/relation edge row schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RelationEdgeSchema {
    /// Source row fields.
    pub(crate) source: VersionedRowRefSchema,
    /// Canonical include/join/relation node id field.
    pub(crate) path_field: String,
    /// Target row fields.
    pub(crate) target: VersionedRowRefSchema,
    /// Relation edge kind field, for distinguishing includes, joins, relation
    /// traversals, recursive frontier edges, and policy branches.
    pub(crate) kind_field: String,
    /// Recursive depth field, when emitted by gather/reachability.
    pub(crate) depth_field: Option<String>,
    /// Multipath/edge-id field, when a relation can produce multiple paths to
    /// the same target.
    pub(crate) edge_id_field: Option<String>,
    /// Union/branch alternative field, when emitted by union or policy branches.
    pub(crate) branch_field: Option<String>,
    /// Terminal role field, for distinguishing intermediate, frontier, and
    /// terminal-output edges.
    pub(crate) role_field: Option<String>,
    /// Stable edge order field, when ordering matters.
    pub(crate) order_field: Option<String>,
    /// Field distinguishing matched edges from hole/null placeholder edges.
    pub(crate) hole_state_field: Option<String>,
}

/// Per-path correlation/cardinality coverage row schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PathCorrelationCoverageSchema {
    /// Parent/root membership fields.
    pub(crate) parent: VersionedRowRefSchema,
    /// Path identity field.
    pub(crate) path_field: String,
    /// Correlation value or synthetic correlation key field.
    pub(crate) correlation_field: String,
    /// Number of expected children, when known.
    pub(crate) expected_count_field: Option<String>,
    /// Number of readable/materialized children.
    pub(crate) readable_count_field: String,
    /// Field distinguishing complete/incomplete/unknown child coverage.
    pub(crate) coverage_state_field: String,
}

/// Content/deletion terminal witness schemas.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct VersionWitnessSchemas {
    /// Witness role field: payload, replacement, deletion, etc.
    pub(crate) role_field: String,
    /// Content-register witness schema.
    pub(crate) content: Option<VersionWitnessSchema>,
    /// Deletion-register witness schema.
    pub(crate) deletion: Option<VersionWitnessSchema>,
}

/// One version-witness terminal row schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct VersionWitnessSchema {
    /// Record descriptor emitted by the terminal graph.
    pub(crate) descriptor: RecordDescriptor,
    /// Fields that identify the concrete row version.
    pub(crate) identity: VersionIdentityFields,
    /// Created-by author field.
    pub(crate) created_by_field: String,
    /// Created-at HLC field.
    pub(crate) created_at_field: String,
    /// Updated-by author field.
    pub(crate) updated_by_field: String,
    /// Updated-at HLC field.
    pub(crate) updated_at_field: String,
    /// Parent transaction set field.
    pub(crate) parents_field: String,
    /// Nullable deletion event field.
    pub(crate) deletion_field: String,
    /// Terminal field name for each app column.
    pub(crate) user_fields: BTreeMap<String, String>,
}

/// Field names needed to identify a concrete row version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct VersionIdentityFields {
    /// Logical table field.
    pub(crate) table_field: String,
    /// Row identity field.
    pub(crate) row_field: String,
    /// Transaction HLC field.
    pub(crate) tx_time_field: String,
    /// Transaction node field.
    pub(crate) tx_node_field: String,
    /// Batch id field for batch-centric visibility.
    pub(crate) batch_id_field: Option<String>,
    /// Branch/prefix field for branch and accepted-transaction visibility.
    pub(crate) branch_or_prefix_field: Option<String>,
    /// Visible member row digest field.
    pub(crate) row_digest_field: Option<String>,
    /// Schema version field.
    pub(crate) schema_field: String,
    /// Version layer field.
    pub(crate) layer_field: String,
}

/// Policy dependency witness terminal row schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PolicyWitnessSchema {
    /// Protected/root row whose visibility is affected.
    pub(crate) protected: VersionedRowRefSchema,
    /// Policy clause or branch field.
    pub(crate) policy_path_field: String,
    /// Witness row fields.
    pub(crate) witness: VersionedRowRefSchema,
    /// Dependency edge kind field, for join/reachability/branch gates.
    pub(crate) edge_kind_field: String,
}

/// Source coverage terminal row schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SourceCoverageSchema {
    /// Logical source field.
    pub(crate) source_field: String,
    /// Logical table field.
    pub(crate) table_field: String,
    /// Covered row field, when coverage is row-specific.
    pub(crate) row_field: Option<String>,
    /// Coverage mode or key-range field.
    pub(crate) coverage_field: String,
    /// Retained binding/routing parameter fields.
    pub(crate) routing_param_fields: BTreeSet<String>,
}

/// Settled read-frontier/query coverage row schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReadFrontierSettledSchema {
    /// Settled durability tier field.
    pub(crate) tier_field: String,
    /// Source/frontier fingerprint field.
    pub(crate) frontier_field: String,
    /// Ordered stream identity field, when settlement must be consumed after
    /// prior payloads on that stream.
    pub(crate) stream_field: Option<String>,
    /// Optional through-sequence/frontier field for ordered streams.
    pub(crate) through_field: Option<String>,
    /// Coverage scope field.
    pub(crate) scope_field: String,
    /// Retained binding/routing parameter fields.
    pub(crate) routing_param_fields: BTreeSet<String>,
}

/// Batch identity fields.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BatchIdentityFields {
    /// Canonical batch id field.
    pub(crate) batch_id_field: String,
    /// Batch owner/node field, when separated from the id encoding.
    pub(crate) batch_node_field: Option<String>,
}

/// Full transaction payload coverage fact schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CompleteTxPayloadCoverageSchema {
    /// Batch/transaction identity fields.
    pub(crate) batch: BatchIdentityFields,
    /// Durability tier field.
    pub(crate) tier_field: String,
    /// Payload digest field.
    pub(crate) payload_digest_field: String,
    /// Fate/settlement field.
    pub(crate) fate_field: String,
}

/// View-complete exclusive transaction coverage fact schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ViewCompleteExclusiveCoverageSchema {
    /// Batch/transaction identity fields.
    pub(crate) batch: BatchIdentityFields,
    /// View/source scope field.
    pub(crate) view_scope_field: String,
    /// Optional result identity whose contributing members define completeness.
    pub(crate) result: Option<ResultIdSchema>,
    /// Digest over members covered for this view/result.
    pub(crate) covered_members_digest_field: String,
    /// Durability tier field.
    pub(crate) tier_field: String,
    /// Fate/settlement field.
    pub(crate) fate_field: String,
}

/// Visible batch member identity fields.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct VisibleBatchMemberFields {
    /// Logical table field.
    pub(crate) table_field: String,
    /// Row identity field.
    pub(crate) row_field: String,
    /// Data branch/prefix field.
    pub(crate) data_branch_field: String,
    /// Row digest field.
    pub(crate) row_digest_field: String,
}

/// Internal contributing member/batch provenance for derived outputs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ContributingMembersSchema {
    /// Terminal result identity whose completeness depends on these members.
    pub(crate) result: ResultIdSchema,
    /// Source/path id field that contributed the member.
    pub(crate) contributor_field: String,
    /// Visible batch member fields.
    pub(crate) member: VisibleBatchMemberFields,
    /// Batch identity fields.
    pub(crate) batch: BatchIdentityFields,
    /// Digest over contributing members for this result/path.
    pub(crate) contributing_digest_field: String,
}

/// Row encoding for a result identity referenced by internal facts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ResultIdSchema {
    /// Real table row result.
    RealRow(VersionedRowRefSchema),
    /// Synthetic aggregate/window-like result.
    SyntheticRow(SyntheticResultMembershipSchema),
    /// Relation/path tuple result.
    PathTuple {
        /// Path identity field.
        path_field: String,
        /// Target row identity field.
        row_field: String,
        /// Stable tuple revision field.
        revision_field: String,
    },
}

/// Aggregate group terminal row schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AggregateResultSchema {
    /// Synthetic result membership for this aggregate group.
    pub(crate) synthetic: SyntheticResultMembershipSchema,
    /// Ordered stable group-key fields.
    pub(crate) group_key_fields: Vec<TypedOutputField>,
    /// Ordered aggregate value fields.
    pub(crate) value_fields: Vec<TypedOutputField>,
    /// Retained binding/routing parameter fields.
    pub(crate) routing_param_fields: BTreeSet<String>,
}

/// Synthetic result identity for aggregate/window-like rows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SyntheticResultMembershipSchema {
    /// Logical synthetic table/relation field.
    pub(crate) table_field: String,
    /// Stable synthetic row id field.
    pub(crate) row_field: String,
    /// Synthetic revision/version field used for replacement deltas.
    pub(crate) revision_field: String,
    /// Retained binding/routing parameter fields.
    pub(crate) routing_param_fields: BTreeSet<String>,
}

/// Typed output field.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct TypedOutputField {
    /// Field name.
    pub(crate) name: String,
    /// Field type.
    pub(crate) ty: ColumnType,
}

/// Ordered/ranked window terminal row schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WindowResultSchema {
    /// Result membership row represented in the window.
    pub(crate) result: ResultMembershipSchema,
    /// Ordered order-by value fields retained for recomputation.
    pub(crate) order_fields: Vec<TypedOutputField>,
    /// Internal retained window-position witness, when needed by the graph.
    pub(crate) position_witness_field: Option<String>,
}

/// Large-value authorization/materialization extent schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LargeValueExtentSchema {
    /// Row containing the large-value column.
    pub(crate) owner: VersionedRowRefSchema,
    /// Column name field.
    pub(crate) column_field: String,
    /// Byte range field.
    pub(crate) range_field: String,
    /// Content digest field.
    pub(crate) digest_field: String,
    /// Field distinguishing handle/chunk/inline materialization.
    pub(crate) materialization_field: String,
    /// App-row handle field linked to these authorized extents.
    pub(crate) handle_field: String,
    /// Durability/source tier field for the extent.
    pub(crate) tier_field: String,
    /// Source coverage field for the extent.
    pub(crate) source_coverage_field: String,
    /// Field distinguishing complete/incomplete extent coverage.
    pub(crate) completeness_field: String,
}

/// Predicate-read fact schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PredicateReadFactSchema {
    /// Logical table field.
    pub(crate) table_field: String,
    /// Shape id field recorded for each predicate read.
    pub(crate) shape_id_field: String,
    /// Binding id field recorded for each predicate read.
    pub(crate) binding_id_field: String,
    /// Field carrying canonical normalized shape bytes for validation without
    /// prior registration.
    pub(crate) canonical_shape_field: String,
    /// Binding value fields retained for validation without prior registration.
    pub(crate) binding_value_fields: BTreeSet<String>,
    /// Base snapshot/frontier field used to validate the predicate read.
    pub(crate) base_snapshot_field: String,
    /// Captured resolved read context field, including schema/lens/source
    /// identity for replay validation.
    pub(crate) resolved_read_key_field: String,
    /// Captured policy context/fingerprint field for replay validation.
    pub(crate) policy_key_field: String,
    /// Source visibility field used by the read.
    pub(crate) source_visibility_field: String,
}

/// Concrete predicate output row/version set used by exclusive validation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PredicateOutputSetSchema {
    /// Whether this terminal emits the base or comparison set.
    pub(crate) role: PredicateOutputSetRole,
    /// Logical table field.
    pub(crate) table_field: String,
    /// Row identity field.
    pub(crate) row_field: String,
    /// Content/deletion version identity fields compared by validation.
    pub(crate) version: ResultMembershipVersionSchema,
    /// Shape id field recorded for replay/debug.
    pub(crate) shape_id_field: String,
    /// Binding id field recorded for replay/debug.
    pub(crate) binding_id_field: String,
}

/// Which side of an exclusive predicate comparison a terminal emits.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum PredicateOutputSetRole {
    /// Output set observed at the transaction base snapshot.
    Base,
    /// Output set observed at validation time.
    Now,
}

/// Point row-read side-effect schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PointReadFactSchema {
    /// Logical table field.
    pub(crate) table_field: String,
    /// Row identity field.
    pub(crate) row_field: String,
    /// Presence state field.
    pub(crate) presence_field: String,
    /// Observed version field, when present.
    pub(crate) observed_version_field: Option<String>,
    /// Base snapshot/frontier field used to validate continued absence, when
    /// absent.
    pub(crate) base_snapshot_field: Option<String>,
}

/// Policy decision output schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PolicyDecisionSchema {
    /// Tri-state decision field.
    pub(crate) outcome_field: String,
    /// Optional field naming missing input for indeterminate probes.
    pub(crate) required_input_field: Option<String>,
    /// Optional reason/category field.
    pub(crate) reason_field: Option<String>,
    /// Witness fact schemas emitted alongside the decision.
    pub(crate) facts: Vec<ProgramFactOutput>,
}

/// Concrete dry-run policy outcome.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum PolicyDecisionOutcome {
    /// Policy grants the operation.
    Allowed,
    /// Policy denies the operation.
    Denied,
    /// The probe cannot decide without input the caller did not supply, for
    /// example a row id for a row-id-sensitive insert policy.
    IndeterminateRequiresInput,
    /// The probe cannot decide from the locally observed source/frontier.
    RequiresCoverage(CoverageFrontier),
}
