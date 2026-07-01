use super::*;

/// One validated API request before semantic lowering.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct QueryProgramRequest {
    /// Exact data views used for source resolution.
    pub(crate) reads: RequestedReadSet,
    /// Identity, claims, and policy mode used by policy augmentation.
    pub(crate) policy: PolicyContext,
    /// Normalized row-set input. Dry-run permission probes are represented as
    /// candidate/proposed-row sources plus `PolicyDecision` terminal facts, not
    /// as a second compiler body.
    pub(crate) input: RowSetProgramInput,
    /// App-facing rows and internal facts requested from the program.
    pub(crate) output: RowSetOutputRequest,
}

/// Normalizes every public query surface into the same row-set shape algebra.
pub(crate) trait RowSetNormalizer {
    /// Normalize ordinary table-rooted Jazz queries.
    fn normalize_query(&self, query: &Query) -> CapabilityResult<NormalizedRowSetShape>;

    /// Normalize output-changing relation queries such as `hopTo`/`gather`.
    fn normalize_relation_query(
        &self,
        query: &RelationQuery,
    ) -> CapabilityResult<NormalizedRowSetShape>;
}

/// Normalized row-set query plus this caller's binding values.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RowSetProgramInput {
    /// Binding-independent normalized query shape.
    pub(crate) shape: NormalizedRowSetShape,
    /// Binding values for this use. Bindings are route inputs, not compiled
    /// shape identity.
    pub(crate) binding: ProgramBinding,
}

/// Normalized binding values for one program instance.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ProgramBinding {
    /// Binding id derived from canonical binding values.
    pub(crate) id: BindingId,
    /// Values by parameter name.
    pub(crate) values: BTreeMap<String, Value>,
}

/// Binding-independent normalized query shape.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NormalizedRowSetShape {
    /// Opaque identity derived from canonicalized `root`, `result`, and
    /// `nodes`.
    pub(crate) identity: NormalizedShapeIdentity,
    /// Terminal expression node for this row-set program.
    pub(crate) root: RowSetNodeId,
    /// App/result identity emitted by the root node.
    pub(crate) result: ResultId,
    /// Extra sources that do not affect app-row membership directly but are
    /// part of maintained/sync payload closure.
    pub(crate) auxiliary_sources: BTreeSet<SourceId>,
    /// Reference/include closure paths that contribute maintained result
    /// membership and may gate root membership.
    pub(crate) closure_paths: Vec<ClosurePath>,
    /// Join-side rows that are part of the materialized maintained/sync
    /// payload when they contribute to a visible root result.
    pub(crate) join_contributions: Vec<JoinContribution>,
    /// Reachable access rows that contribute to a visible root result through
    /// a recursive closure.
    pub(crate) reachable_contributions: Vec<ReachableContribution>,
    /// Normalized expression DAG. Public query and relation surfaces both
    /// normalize here before lowering.
    pub(crate) nodes: BTreeMap<RowSetNodeId, RowSetExpr>,
}

/// One maintained/sync closure path rooted at the app result rows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ClosurePath {
    /// Stable path name for diagnostics/sinks.
    pub(crate) id: String,
    /// Why this closure path exists in the normalized query.
    pub(crate) kind: ClosurePathKind,
    /// Ordered reference hops.
    pub(crate) segments: Vec<ClosurePathSegment>,
    /// Whether and how this path gates root membership.
    pub(crate) root_gate: Option<ClosureRootGate>,
}

/// Root-membership gate semantics for explicit include paths.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClosureRootGate {
    /// Required includes demand that every non-null/non-empty reference value
    /// resolves through the rest of the path; null scalar refs and empty arrays
    /// are vacuously satisfied.
    Required,
    /// Inner includes additionally require at least one reference value at each
    /// path hop to resolve.
    Inner,
}

/// Semantic origin of a maintained/sync closure path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ClosurePathKind {
    /// Default one-hop root reference payload included when the user did not
    /// request explicit includes.
    ImplicitRootReference,
    /// User-requested include path. Its `gates_root` flag captures optional vs
    /// required/inner include semantics.
    ExplicitInclude,
}

/// One reference hop inside a closure path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ClosurePathSegment {
    /// Source occurrence containing the reference column.
    pub(crate) parent: SourceId,
    /// Target occurrence reached by the reference column.
    pub(crate) target: SourceId,
    /// Public source column name, without the internal `user_` prefix.
    pub(crate) source_field: String,
}

/// One join-side contribution payload rooted at the app result rows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct JoinContribution {
    /// Stable contribution name for diagnostics/sinks.
    pub(crate) id: String,
    /// Source occurrence for the contributing join rows.
    pub(crate) source: SourceId,
    /// Normalized node for the contributing join rows, including join filters.
    pub(crate) input: RowSetNodeId,
    /// Public join-row column that references the root result row id.
    pub(crate) root_ref_field: String,
}

/// One reachable-via access contribution rooted at the app result rows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReachableContribution {
    /// Stable contribution name for diagnostics/sinks.
    pub(crate) id: String,
    /// Access source occurrence for the contributing access rows.
    pub(crate) access_source: SourceId,
    /// Normalized access rows already joined against the recursive closure.
    pub(crate) access_input: RowSetNodeId,
    /// Public access-row column that references the root result row id.
    pub(crate) root_ref_field: String,
}

/// Derived identity for a normalized row-set shape.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NormalizedShapeIdentity {
    /// Content-addressed normalized shape id.
    pub(crate) shape_id: ShapeId,
    /// Canonical normalized IR bytes.
    pub(crate) canonical: Vec<u8>,
}

/// Stable node id inside a normalized row-set expression DAG.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct RowSetNodeId(pub(crate) String);

/// Stable identity for one logical source occurrence in a normalized program.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct SourceId {
    /// Logical table emitted by this source.
    pub(crate) table: String,
    /// Stable path/role inside the normalized query.
    pub(crate) path: SourcePath,
}

/// Stable source path inside a normalized row-set program.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct SourcePath {
    /// Stable path components inside the normalized query. Nested arrays,
    /// union branches, recursive subplans, and correlated children extend this
    /// path instead of creating a second source identity.
    pub(crate) components: Vec<SourceRole>,
}

/// Program-local path identity with explicit owning source context.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ProgramPathId {
    /// Source/root that owns the path. The path is the edge from this owner to
    /// `child`; there is no separate relative path namespace.
    pub(crate) owner: SourceId,
    /// Terminal child source whose rows form this path's targets.
    pub(crate) child: SourceId,
}

/// App/result identity emitted by a row-set program.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum ResultId {
    /// Real table rows. The row reference may come from one source occurrence,
    /// a union of real-row source alternatives, or a terminal projected row id;
    /// it remains a real row for policy, version witnesses, and large values.
    RealRow {
        /// Logical output table.
        table: String,
        /// Terminal row identity.
        row: ResultRowRef,
    },
    /// Synthetic relation/aggregate/window-like rows.
    SyntheticTuple {
        /// Stable synthetic identity contract.
        identity: SyntheticIdentitySpec,
    },
    /// Synthetic path/relation tuple rows.
    PathTuple {
        /// Include/join/relation path identity.
        path: ProgramPathId,
        /// Stable tuple revision expression.
        revision: Vec<NormalizedValueRef>,
    },
}

/// Real-row terminal identity independent of one source occurrence.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum ResultRowRef {
    /// Real rows supplied by one source occurrence.
    Source(SourceId),
    /// Real rows supplied by equivalent source alternatives, such as relation
    /// unions that still output the same logical table.
    SourceAlternatives(BTreeSet<SourceId>),
    /// Row id produced by a terminal projection. `source_discriminator` is
    /// retained when later witness/policy lowering must route back to a source
    /// alternative.
    Projected {
        /// Terminal row-id value.
        row: NormalizedValueRef,
        /// Optional source/branch discriminator value.
        source_discriminator: Option<NormalizedValueRef>,
    },
}

/// Stable identity contract for synthetic result rows.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct SyntheticIdentitySpec {
    /// Logical synthetic table/relation name.
    pub(crate) table: String,
    /// Columns that define stable row identity.
    pub(crate) key_columns: Vec<String>,
    /// Columns that define replacement/revision identity.
    pub(crate) revision_columns: Vec<String>,
}

/// Stable source role inside a normalized row-set program.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum SourceRole {
    /// Root query source.
    Root,
    /// Named relation/join/path alias.
    Alias(String),
    /// Recursive seed source.
    RecursiveSeed(String),
    /// Recursive step/frontier source.
    RecursiveStep(String),
    /// Correlated path child source.
    CorrelatedChild(String),
    /// Source introduced by policy augmentation. These sources use the same
    /// read view as app sources; they do not live in a separate policy-source
    /// universe.
    Policy(String),
}

/// Typed normalized row-set expression node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RowSetExpr {
    /// Logical source scan.
    Source {
        /// Logical source.
        source: SourceId,
        /// Query-visible deletion behavior for this source occurrence.
        visibility: RowVisibility,
    },
    /// Non-table rows supplied by binding parameters or inline scalar values.
    ValueSource {
        /// Stable binding-source shape name used when `mode` is runtime-bound.
        shape: String,
        /// Output columns carried by the value rows.
        columns: Vec<ValueSourceColumn>,
        /// Value source mode.
        mode: ValueSourceMode,
    },
    /// Recursive frontier rows bound inside a recursive step graph.
    FrontierSource {
        /// Frontier tuple identity.
        frontier: FrontierId,
        /// Output columns carried by the frontier.
        columns: Vec<ValueSourceColumn>,
    },
    /// Filter expression over row values, binding params, or trusted claims.
    Filter {
        /// Input node.
        input: RowSetNodeId,
        /// Predicate.
        predicate: PredicateExpr,
    },
    /// Join/relation edge.
    Join {
        /// Left input node.
        left: RowSetNodeId,
        /// Right input node.
        right: RowSetNodeId,
        /// Join mode.
        mode: JoinMode,
        /// Join predicate.
        on: PredicateExpr,
    },
    /// Recursive relation traversal such as `gather`.
    RecursiveRelation {
        /// Seed input node.
        seed: RowSetNodeId,
        /// Recursive step input node.
        step: RowSetNodeId,
        /// Frontier tuple bound inside the recursive step graph.
        frontier: FrontierId,
        /// Frontier key carried between iterations.
        frontier_key: NormalizedValueRef,
        /// Dedupe keys used to detect already-reached rows.
        dedupe_keys: Vec<NormalizedValueRef>,
        /// Recursion bound.
        bound: RecursionBound,
    },
    /// Union of branch/source alternatives.
    Union {
        /// Union inputs.
        inputs: Vec<UnionInput>,
    },
    /// Distinct rows by arbitrary normalized keys.
    Distinct {
        /// Input node.
        input: RowSetNodeId,
        /// Dedupe keys.
        keys: Vec<NormalizedValueRef>,
    },
    /// Project/alias tuple fields without changing the result identity
    /// contract. Relation `project` lowers here.
    Project {
        /// Input node.
        input: RowSetNodeId,
        /// Projected output columns.
        columns: Vec<RowProjection>,
    },
    /// Correlated path/relation projection lowered as relation edges plus
    /// correlation coverage facts.
    CorrelatedPathProjection {
        /// Input node.
        input: RowSetNodeId,
        /// Child row-set subgraph. The child can itself contain filters,
        /// joins, slices, and policy branches.
        child_input: RowSetNodeId,
        /// Internal path identity.
        path: ProgramPathId,
        /// Correlation predicate.
        correlation: PredicateExpr,
        /// Correlation coverage requirement.
        requirement: CorrelationRequirement,
    },
    /// Ordering.
    OrderBy {
        /// Input node.
        input: RowSetNodeId,
        /// Ordered keys.
        keys: Vec<OrderKey>,
    },
    /// Limit/offset or other finite-window operator.
    Slice {
        /// Input node.
        input: RowSetNodeId,
        /// Optional partition keys for per-parent/per-group windows. Empty
        /// means a global slice.
        partition_by: Vec<NormalizedValueRef>,
        /// Optional limit.
        limit: Option<u32>,
        /// Offset.
        offset: u32,
        /// Stable tie-breaker keys used for replacement/rank deltas.
        tie_breaker: Vec<NormalizedValueRef>,
        /// Optional retained rank/position output field.
        rank_output: Option<TypedOutputField>,
    },
    /// Aggregate group operator.
    Aggregate {
        /// Input node.
        input: RowSetNodeId,
        /// Grouping keys.
        group_by: Vec<NormalizedValueRef>,
        /// Aggregate output expressions.
        outputs: Vec<AggregateExpr>,
    },
}

/// One column emitted by a non-table value row source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ValueSourceColumn {
    /// Output column name.
    pub(crate) name: String,
    /// Value expression for seed rows. Binding-source columns should use
    /// `NormalizedValueRef::Param`; inline values may use literals or trusted
    /// policy-context claims.
    pub(crate) value: NormalizedValueRef,
    /// Groove column type.
    pub(crate) ty: ColumnType,
}

/// Non-table value source mode.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ValueSourceMode {
    /// Runtime binding source populated from query bindings.
    Binding,
    /// Inline single-row value source.
    Inline,
}

/// One projected row value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RowProjection {
    /// Output field.
    pub(crate) output: TypedOutputField,
    /// Value expression.
    pub(crate) value: NormalizedValueRef,
}

/// One union branch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct UnionInput {
    /// Input node id.
    pub(crate) node: RowSetNodeId,
    /// Stable branch label.
    pub(crate) label: String,
}

/// Predicate expression.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PredicateExpr {
    /// Always true.
    True,
    /// Always false.
    False,
    /// Comparison.
    Compare {
        /// Left value.
        left: NormalizedValueRef,
        /// Comparison operator.
        op: ComparisonOp,
        /// Right value.
        right: NormalizedValueRef,
    },
    /// `value IN options`.
    In {
        /// Value to test.
        value: NormalizedValueRef,
        /// Candidate values.
        options: Vec<NormalizedValueRef>,
    },
    /// Array membership.
    ArrayContains {
        /// Haystack.
        value: NormalizedValueRef,
        /// Needle.
        needle: NormalizedValueRef,
    },
    /// Text/full-text containment. Lowering can capability-error for
    /// unsupported index/materialization modes.
    TextContains {
        /// Text value.
        value: NormalizedValueRef,
        /// Search term.
        needle: NormalizedValueRef,
    },
    /// Null test.
    IsNull(NormalizedValueRef),
    /// Non-null test.
    IsNotNull(NormalizedValueRef),
    /// Boolean conjunction.
    And(Vec<PredicateExpr>),
    /// Boolean disjunction.
    Or(Vec<PredicateExpr>),
    /// Boolean negation.
    Not(Box<PredicateExpr>),
}

/// Comparison operator.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ComparisonOp {
    /// Equal.
    Eq,
    /// Not equal.
    Ne,
    /// Less than.
    Lt,
    /// Less than or equal.
    Lte,
    /// Greater than.
    Gt,
    /// Greater than or equal.
    Gte,
}

/// Normalized value reference.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum NormalizedValueRef {
    /// Field on a source row.
    SourceField {
        /// Source identity.
        source: SourceId,
        /// Field name.
        field: String,
    },
    /// User binding parameter.
    Param(String),
    /// Trusted claim.
    Claim(ClaimPath),
    /// Recursive frontier column.
    FrontierColumn {
        /// Frontier tuple identity.
        frontier: FrontierId,
        /// Field name.
        field: String,
    },
    /// Row identity reference.
    RowId(RowIdRef),
    /// Magic provenance field such as `$createdAt`.
    Provenance {
        /// Source identity.
        source: SourceId,
        /// Provenance field.
        field: ProvenanceField,
    },
    /// Literal value encoded canonically.
    Literal(Vec<u8>),
}

/// First-class provenance fields exposed by public query surfaces.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum ProvenanceField {
    /// `$createdAt`.
    CreatedAt,
    /// `$createdBy`.
    CreatedBy,
    /// `$updatedAt`.
    UpdatedAt,
    /// `$updatedBy`.
    UpdatedBy,
}

/// Normalized row-id reference.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum RowIdRef {
    /// Row id from a current source in this node.
    Source(SourceId),
    /// Row id carried by recursive frontier state.
    Frontier(FrontierId),
}

/// Stable carried tuple identity for recursive frontier state.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct FrontierId(pub(crate) String);

/// Path into trusted claim/session data.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ClaimPath(pub(crate) Vec<String>);

/// Normalized join mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum JoinMode {
    /// Required match; drop parent/source rows with no target.
    Inner,
    /// Keep parent/source rows and emit a hole/null edge.
    NullExtend,
}

/// Correlated path/relation coverage requirement.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum CorrelationRequirement {
    /// Parent may remain even when no children are covered.
    Optional,
    /// Parent requires at least one readable child.
    AtLeastOne,
    /// Parent requires cardinality-complete child coverage for the correlation.
    MatchCorrelationCardinality,
}

/// Aggregate output expression.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AggregateExpr {
    /// Output field.
    pub(crate) output: TypedOutputField,
    /// Aggregate function.
    pub(crate) function: AggregateFunction,
    /// Input value, when required by the aggregate.
    pub(crate) input: Option<NormalizedValueRef>,
}

/// Aggregate function.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum AggregateFunction {
    /// Count rows.
    Count,
    /// Sum values.
    Sum,
    /// Minimum value.
    Min,
    /// Maximum value.
    Max,
}

/// Ordered key in a normalized row-set program.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OrderKey {
    /// Value to order by.
    pub(crate) value: NormalizedValueRef,
    /// Ascending or descending.
    pub(crate) direction: SortDirection,
}

/// Sort direction.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum SortDirection {
    /// Ascending order.
    Asc,
    /// Descending order.
    Desc,
}

/// Query-visible deletion behavior.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum RowVisibility {
    /// Only currently visible rows.
    Visible,
    /// Include root deletion markers. Joins/includes still resolve visible
    /// target rows unless their own query semantics say otherwise.
    IncludeDeleted,
}
