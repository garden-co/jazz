#![allow(dead_code)]

//! Destination vocabulary for the unified Jazz query engine.
//!
//! The intended shape is deliberately small:
//!
//! 1. public query surfaces validate into [`QuerySurface`],
//! 2. callers choose one [`ReadView`] and one [`PolicySubject`],
//! 3. callers choose a semantic [`TerminalProgram`],
//! 4. one lowering pass resolves sources, policy, relation semantics, and
//!    terminal payloads into groove IVM graphs.
//!
//! This module is type-first scaffolding for that cleanup. It should not grow a
//! second evaluator or a second relation AST; `crate::query` owns public query
//! syntax, while this module names the compiler boundary that all one-shot
//! reads, subscriptions, sync-serving reads, dry-runs, branch reads, schema
//! projections, and transaction reads should share.

use std::collections::{BTreeMap, BTreeSet};

use groove::db::GraphBuilder;
use groove::records::{RecordDescriptor, Value};
use groove::schema::ColumnType;

use crate::ids::{AuthorId, BranchId, RowUuid, SchemaVersionId};
use crate::protocol::SubscriptionKey;
use crate::query::{Binding, BindingId, RelationQuery, ShapeId, ValidatedQuery};
use crate::schema::TableSchema;
use crate::time::{GlobalSeq, TxTime};
use crate::tx::{DurabilityTier, Snapshot, TxId};

/// One validated API request before semantic lowering.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct QueryProgramRequest {
    /// User-facing query vocabulary that entered the engine.
    pub(crate) surface: QuerySurface,
    /// Exact data view used for source resolution.
    pub(crate) view: ReadView,
    /// Identity and claims used by policy augmentation.
    pub(crate) subject: PolicySubject,
    /// Semantic terminal program requested from this query.
    pub(crate) terminal: TerminalProgram,
}

/// Public query vocabulary accepted by the unified engine.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum QuerySurface {
    /// Ordinary Jazz query shape plus binding values.
    Validated {
        /// Validated, schema-stamped query shape.
        shape: ValidatedQuery,
        /// Binding values for this use of the shape.
        binding: Binding,
    },
    /// Output-changing relation query used by alpha-style `hopTo`/`gather`.
    Relation {
        /// Relation query to normalize and lower through the same source engine.
        query: RelationQuery,
    },
}

/// Concrete read view selected before source resolution.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ReadView {
    /// Current rows at one durability tier and schema version.
    Current {
        /// Local, edge, or global source currency.
        tier: DurabilityTier,
        /// Schema version used to interpret logical tables and columns.
        schema: SchemaVersionId,
    },
    /// Historical global cut at one schema version.
    Historical {
        /// Inclusive global sequence cut.
        position: GlobalSeq,
        /// Schema version used to interpret logical tables and columns.
        schema: SchemaVersionId,
    },
    /// Dotted snapshot ref at one schema version.
    Snapshot {
        /// Snapshot frontier.
        snapshot: Snapshot,
        /// Schema version used to interpret logical tables and columns.
        schema: SchemaVersionId,
    },
    /// Exclusive transaction read over a snapshot base plus overlay writes.
    Transaction {
        /// Stable base snapshot for the transaction.
        base: Snapshot,
        /// Overlay and read-tracking behavior for this transaction.
        overlay: TransactionOverlay,
        /// Schema version used to interpret logical tables and columns.
        schema: SchemaVersionId,
    },
    /// Branch read. The branch record owns its frozen base snapshot.
    Branch {
        /// Branch identity.
        branch: BranchId,
        /// Schema version used to interpret logical tables and columns.
        schema: SchemaVersionId,
    },
    /// Read through a maintained usage-site result set.
    SettledResultSet {
        /// Usage-site subscription whose settled rows are the root source.
        subscription: SubscriptionKey,
        /// Schema version used to interpret logical tables and columns.
        schema: SchemaVersionId,
    },
}

impl ReadView {
    /// Return the schema version shared by this read view.
    pub(crate) fn schema(&self) -> SchemaVersionId {
        match self {
            Self::Current { schema, .. }
            | Self::Historical { schema, .. }
            | Self::Snapshot { schema, .. }
            | Self::Transaction { schema, .. }
            | Self::Branch { schema, .. }
            | Self::SettledResultSet { schema, .. } => *schema,
        }
    }

    /// Return the current durability tier when this is a current read.
    pub(crate) fn current_tier(&self) -> Option<DurabilityTier> {
        match self {
            Self::Current { tier, .. } => Some(*tier),
            Self::Historical { .. }
            | Self::Snapshot { .. }
            | Self::Transaction { .. }
            | Self::Branch { .. }
            | Self::SettledResultSet { .. } => None,
        }
    }
}

/// Transaction overlay behavior selected by exclusive transaction reads.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct TransactionOverlay {
    /// Transaction whose pending writes are visible, when already minted.
    pub(crate) tx_id: Option<TxId>,
    /// Whether predicate/point reads must be recorded for serializability.
    pub(crate) record_reads: bool,
}

/// Identity used by policy augmentation.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum PolicySubject {
    /// Internal/system reads bypass row-level policy.
    System,
    /// Authenticated identity plus trusted server/session claims.
    Identity {
        /// Identity whose permissions are being evaluated.
        permission_subject: AuthorId,
        /// Trusted claims available to policy queries.
        claims: BTreeMap<String, Value>,
        /// Author recorded on writes, when it differs from the permission subject.
        attribution: Option<AuthorId>,
    },
}

/// Semantic terminal program requested from one lowered query.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum TerminalProgram {
    /// Ordinary row materialization for one-shot `query()` calls.
    Rows,
    /// Relation payload materialization for alpha-style relation APIs.
    RelationSnapshot(RelationSnapshotTerminals),
    /// Maintained application/sync view with explicit terminal payloads.
    MaintainedView(MaintainedViewTerminals),
    /// Dry-run policy probe over the same source and policy machinery.
    PolicyProbe(PolicyProbe),
}

/// Terminal rows needed by relation snapshot queries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RelationSnapshotTerminals {
    /// Whether relation edges are part of the result payload.
    pub(crate) relation_edges: bool,
    /// Whether non-root payload rows are part of the result payload.
    pub(crate) payload_rows: bool,
}

/// Maintained terminal contract for subscriptions and query-driven sync.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MaintainedViewTerminals {
    /// Usage-site subscription, when this program is serving a concrete peer.
    pub(crate) usage_site: Option<SubscriptionKey>,
    /// First-delivery behavior for application callbacks.
    pub(crate) delivery: SubscriptionDelivery,
    /// Payload schemas emitted by terminal graphs.
    pub(crate) payloads: TerminalPayloadSchemas,
    /// Coverage facts required by the caller.
    pub(crate) coverage: CoverageRequest,
}

/// Subscription delivery semantics that do not alter source lowering.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SubscriptionDelivery {
    /// Publish synchronously for local UI work when local writes commit.
    LocalImmediate,
    /// Wait for a tier to settle before the first publication.
    Settled {
        /// Durability tier required before first delivery.
        tier: DurabilityTier,
    },
}

/// Operation tested by a dry-run policy probe.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PolicyProbe {
    /// Policy operation being probed.
    pub(crate) operation: DryRunOperation,
    /// Candidate row/change evaluated by the policy program.
    pub(crate) candidate: PolicyCandidate,
}

/// Operation tested by a dry-run policy probe.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DryRunOperation {
    /// Probe read visibility.
    Read,
    /// Probe insert admission.
    Insert,
    /// Probe update admission.
    Update,
    /// Probe delete admission.
    Delete,
}

/// Candidate row or mutation used by dry-run policy checks.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum PolicyCandidate {
    /// Existing row visibility check.
    Read {
        /// Logical table.
        table: String,
        /// Row identity.
        row: RowUuid,
    },
    /// Proposed insert.
    Insert {
        /// Logical table.
        table: String,
        /// Optional caller-chosen row identity.
        row: Option<RowUuid>,
        /// Proposed application cells.
        new_values: BTreeMap<String, Value>,
    },
    /// Proposed update, with old and new cells available to policy clauses.
    Update {
        /// Logical table.
        table: String,
        /// Row identity.
        row: RowUuid,
        /// Visible cells before the update.
        old_values: BTreeMap<String, Value>,
        /// Proposed cells after the update.
        new_values: BTreeMap<String, Value>,
    },
    /// Proposed delete, with old cells available to policy clauses.
    Delete {
        /// Logical table.
        table: String,
        /// Row identity.
        row: RowUuid,
        /// Visible cells before deletion.
        old_values: BTreeMap<String, Value>,
    },
}

/// Typed terminal payload schemas emitted by maintained graphs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct TerminalPayloadSchemas {
    /// Root result-set membership rows.
    pub(crate) result_membership: Option<ResultMembershipSchema>,
    /// Matched include/join path rows.
    pub(crate) matched_paths: Option<MatchedPathSchema>,
    /// Relation edge rows for array subqueries and relation APIs.
    pub(crate) relation_edges: Option<RelationEdgeSchema>,
    /// Content/deletion version witnesses needed for payload shipping.
    pub(crate) version_witnesses: Option<VersionWitnessSchemas>,
    /// Replacement witnesses for visible rows that leave or change.
    pub(crate) replacement_witnesses: Option<VersionWitnessSchemas>,
    /// Policy dependency witnesses that can grant or revoke visibility.
    pub(crate) policy_witnesses: Option<PolicyWitnessSchema>,
}

/// Root result membership terminal row schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ResultMembershipSchema {
    /// Logical table field in the terminal row.
    pub(crate) table_field: String,
    /// Row identity field in the terminal row.
    pub(crate) row_field: String,
    /// Retained binding/routing parameter fields.
    pub(crate) routing_param_fields: BTreeSet<String>,
}

/// Matched include/join path terminal row schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MatchedPathSchema {
    /// Root result row fields.
    pub(crate) root: ResultMembershipSchema,
    /// Witness row fields.
    pub(crate) witness: ResultMembershipSchema,
    /// Include or join path field.
    pub(crate) path_field: String,
    /// Whether a missing witness is represented as a hole.
    pub(crate) allows_holes: bool,
}

/// Relation edge terminal row schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RelationEdgeSchema {
    /// Source row fields.
    pub(crate) source: ResultMembershipSchema,
    /// Relation/array-subquery name field.
    pub(crate) relation_field: String,
    /// Target row fields.
    pub(crate) target: ResultMembershipSchema,
    /// Stable edge order field, when ordering matters.
    pub(crate) order_field: Option<String>,
}

/// Content/deletion terminal witness schemas.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct VersionWitnessSchemas {
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
    /// Schema version field.
    pub(crate) schema_field: String,
    /// Version layer field.
    pub(crate) layer_field: String,
}

/// Policy dependency witness terminal row schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PolicyWitnessSchema {
    /// Policy clause or branch field.
    pub(crate) policy_path_field: String,
    /// Witness row fields.
    pub(crate) witness: ResultMembershipSchema,
}

/// Coverage information requested from a maintained terminal graph.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct CoverageRequest {
    /// Coverage identity shared by equivalent usage-site subscriptions.
    pub(crate) key: Option<CoverageKey>,
    /// Source/table coverage facts required by the peer or runtime.
    pub(crate) source_rows: bool,
    /// View-scoped completeness facts for partial exclusive transaction payloads.
    pub(crate) view_complete_exclusive: bool,
}

/// Stable grouping key for equivalent maintained coverage work.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct CoverageKey {
    /// Registered shape id.
    pub(crate) shape_id: ShapeId,
    /// Registered binding id.
    pub(crate) binding_id: BindingId,
    /// Read view identity.
    pub(crate) view: ReadView,
}

/// Logical source request made by query, policy, or terminal lowering.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SourceRequest {
    /// Logical table requested by the compiler.
    pub(crate) table: String,
    /// Why this source is being read.
    pub(crate) role: SourceRole,
    /// Data view for this source.
    pub(crate) view: ReadView,
    /// Deletion visibility expected from this source.
    pub(crate) deletion_scope: DeletionScope,
}

/// Role of a table source in the lowered program.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SourceRole {
    /// Root rows returned to the user.
    Root,
    /// Rows needed to prove joins/includes/payloads.
    Witness,
    /// Rows used only for policy augmentation.
    Policy,
    /// Rows emitted by relation subprograms.
    Relation,
    /// Rows needed to ship replacement winners.
    Replacement,
    /// Large-value/content extent authorization source.
    LargeValue,
}

/// Deletion visibility for source resolution.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DeletionScope {
    /// Only currently visible rows.
    VisibleOnly,
    /// Include deletion markers at the root result source.
    IncludeDeletedRoot,
    /// Include deletion markers for witness/replacement source work.
    IncludeDeletedWitness,
}

/// Resolver that turns logical Jazz source requests into groove inputs.
pub(crate) trait SourceResolver {
    /// Resolve a source request into a concrete source plan and row shape.
    fn resolve_source(
        &mut self,
        request: &SourceRequest,
    ) -> Result<ResolvedSource, SourceResolutionError>;
}

/// Concrete source selected for one logical source request.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ResolvedSource {
    /// Logical table schema after schema/lens resolution.
    pub(crate) table_schema: TableSchema,
    /// Groove-level source composition.
    pub(crate) plan: SourcePlan,
    /// Canonical row shape emitted by the source plan.
    pub(crate) row_shape: SourceRowShape,
}

/// Groove source composition before operator lowering.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SourcePlan {
    /// Concrete groove graph source.
    Graph {
        /// Source graph.
        graph: GraphBuilder,
    },
    /// Union of schema partitions or compatible source fragments.
    Union {
        /// Source fragments.
        inputs: Vec<SourcePlan>,
    },
    /// Overlay source over a base source, used for branches and transactions.
    Overlay {
        /// Overlay kind.
        kind: SourceOverlayKind,
        /// Overlay rows.
        overlay: Box<SourcePlan>,
        /// Base rows.
        base: Box<SourcePlan>,
    },
    /// Schema/lens projection over one source.
    Projected {
        /// Source before projection.
        input: Box<SourcePlan>,
        /// Lens path used to project rows.
        lens: SchemaLensPath,
        /// Target schema version.
        target_schema: SchemaVersionId,
    },
    /// Settled result-set source for rehydrate/reset and query-driven sync.
    SettledResultSet {
        /// Usage-site subscription.
        subscription: SubscriptionKey,
    },
    /// Capability-gated placeholder for a source shape not yet lowered.
    Unsupported {
        /// Explicit source gap.
        gap: SourceGap,
    },
}

/// Overlay source category.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SourceOverlayKind {
    /// Branch overlay rows over the branch base.
    Branch(BranchId),
    /// Exclusive transaction overlay rows over its snapshot base.
    Transaction(Option<TxId>),
}

/// Schema/lens path followed by a projected source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SchemaLensPath {
    /// Source schema version.
    pub(crate) from: SchemaVersionId,
    /// Intermediate lens/schema versions, in application order.
    pub(crate) via: Vec<SchemaVersionId>,
    /// Target schema version.
    pub(crate) to: SchemaVersionId,
}

/// Canonical row shape emitted by source resolution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SourceRowShape {
    /// Logical table emitted by this source.
    pub(crate) table: String,
    /// Descriptor of the record emitted by this source.
    pub(crate) descriptor: RecordDescriptor,
    /// Field containing row identity.
    pub(crate) row_uuid_field: String,
    /// Field containing schema version, when present.
    pub(crate) schema_version_field: Option<String>,
    /// Field containing content transaction time, when present.
    pub(crate) tx_time_field: Option<String>,
    /// Field containing content transaction node, when present.
    pub(crate) tx_node_field: Option<String>,
    /// Whether this source carries deletion marker state.
    pub(crate) includes_deletion_marker: bool,
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
    /// Historical source cannot yet be represented incrementally.
    Historical,
    /// Snapshot source includes local overlays or dots not yet represented.
    SnapshotRef,
    /// Schema/lens fanout or projection cannot yet be represented.
    SchemaProjection,
    /// Branch overlay source cannot yet be represented.
    BranchOverlay,
    /// Transaction overlay source cannot yet be represented.
    TransactionOverlay,
    /// Settled result-set source cannot yet be represented.
    SettledResultSet,
}

/// Parameter domains attached to one lowered graph.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ParameterDomain {
    /// User-supplied binding parameters.
    pub(crate) user_params: BTreeMap<String, ColumnType>,
    /// Server-derived hidden parameters such as claims.
    pub(crate) hidden_params: BTreeMap<String, ColumnType>,
    /// Parameters retained in terminal rows for usage-site routing.
    pub(crate) routing_params: BTreeSet<String>,
}

/// Result of lowering one query program.
#[derive(Clone, Debug)]
pub(crate) struct QueryProgram {
    /// Original request.
    pub(crate) request: QueryProgramRequest,
    /// Groove graph and its boundary contracts.
    pub(crate) lowered: LoweredGraph,
    /// Capability status for the exact requested program.
    pub(crate) capability: CapabilityReport,
    /// Human-readable debugging and test artifact.
    pub(crate) explain: ExplainPlan,
}

/// Groove graph plus the semantic contracts needed to consume it.
#[derive(Clone, Debug)]
pub(crate) struct LoweredGraph {
    /// Executable groove graph.
    pub(crate) graph: GraphBuilder,
    /// Parameter domains expected by the graph.
    pub(crate) parameters: ParameterDomain,
    /// Terminal payload schemas emitted by the graph.
    pub(crate) terminals: TerminalPayloadSchemas,
}

/// Capability status for the requested program.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct CapabilityReport {
    /// Unsupported pieces. An empty list means the requested program is supported.
    pub(crate) gaps: Vec<UnsupportedReason>,
}

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
    /// Requested terminal payload is not yet emitted.
    Terminal(String),
    /// Policy composition is not yet lowered.
    Policy(String),
    /// Runtime contract is not yet connected to the lowered graph.
    Runtime(String),
}

/// Debug artifact for query-engine tests and design audits.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ExplainPlan {
    /// Public surface summary.
    pub(crate) surface: String,
    /// Source/frontier decisions.
    pub(crate) sources: Vec<String>,
    /// Policy rewrite decisions.
    pub(crate) policy: Vec<String>,
    /// Terminal payload decisions.
    pub(crate) terminals: Vec<String>,
    /// Capability decisions.
    pub(crate) capabilities: Vec<String>,
    /// Physical graph summaries.
    pub(crate) physical: Vec<String>,
}

/// Concrete row-version identity used by decoded terminal witnesses.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct VersionIdentity {
    /// Row identity.
    pub(crate) row: RowUuid,
    /// Transaction HLC.
    pub(crate) tx_time: TxTime,
    /// Transaction id.
    pub(crate) tx_id: TxId,
    /// Schema version of the stored version.
    pub(crate) schema: SchemaVersionId,
    /// Version layer.
    pub(crate) layer: VersionLayer,
}

/// Version layer carried by terminal witness rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum VersionLayer {
    /// Content-register version.
    Content,
    /// Deletion-register version.
    Deletion,
}
