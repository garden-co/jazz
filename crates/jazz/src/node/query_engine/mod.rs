#![allow(dead_code)]

//! Unified Jazz query compiler vocabulary.
//!
//! This module is intentionally type-first. It names the destination shape for
//! the ongoing cleanup: every public Jazz query surface is normalized into one
//! semantic IR, composed with source/frontier and policy context, augmented with
//! an explicit terminal contract, then lowered onto groove IVM graphs. Existing
//! query code can move here incrementally, but new row-producing paths should
//! target these types instead of creating another evaluator.

use std::collections::{BTreeMap, BTreeSet};

use groove::db::GraphBuilder;
use groove::ivm::PreparedShapeId;
use groove::records::{RecordDescriptor, Value};
use groove::schema::ColumnType;

use crate::ids::{AuthorId, BranchId, RowUuid, SchemaVersionId};
use crate::protocol::{ResultRowEntry, SubscriptionKey};
use crate::query::{
    AggregateQuery, Binding, BindingId, OrderBy, Predicate, RelationKeyRef, RelationQuery, ShapeId,
    ValidatedQuery,
};
use crate::time::GlobalSeq;
use crate::tx::{DurabilityTier, Snapshot, TxId};

/// One user/API request after public validation but before semantic lowering.
#[derive(Clone, Debug)]
pub(crate) struct QueryEngineRequest {
    /// Query surface that entered the engine.
    pub(crate) surface: QuerySurface,
    /// Read, schema, branch, and policy context for this execution.
    pub(crate) context: QueryContext,
    /// How the lowered program will be consumed.
    pub(crate) execution: ExecutionMode,
    /// Which terminal facts the caller needs from the same semantics.
    pub(crate) output: OutputContract,
}

/// Public query vocabulary accepted by the unified engine.
#[derive(Clone, Debug)]
pub(crate) enum QuerySurface {
    /// Ordinary Jazz query shape plus binding.
    Validated {
        /// Validated, schema-stamped query shape.
        shape: ValidatedQuery,
        /// Binding values for the shape.
        binding: Binding,
    },
    /// Output-changing relation query used by alpha-style `hopTo`/`gather`.
    Relation {
        /// Relation query to normalize into the same relational IR.
        query: RelationQuery,
    },
}

/// Context dimensions that must not create separate query engines.
#[derive(Clone, Debug)]
pub(crate) struct QueryContext {
    /// Read frontier/source freshness requested by the caller.
    pub(crate) frontier: ReadFrontier,
    /// Schema/lens view used to interpret logical tables and columns.
    pub(crate) schema: SchemaView,
    /// Branch or base-data view used by source resolution.
    pub(crate) branch: BranchView,
    /// Policy/claims context used for read narrowing or dry-run checks.
    pub(crate) policy: PolicyContext,
}

/// Read frontier selected before lowering query algebra.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ReadFrontier {
    /// Local currency, including this node's pending committed writes.
    Local,
    /// Edge-accepted state.
    Edge,
    /// Globally accepted state.
    Global,
    /// Historical global cut.
    Historical {
        /// Inclusive global sequence cut.
        position: GlobalSeq,
    },
    /// Dotted snapshot reference used by exclusive and future branch reads.
    Snapshot {
        /// Snapshot frontier.
        snapshot: Snapshot,
    },
    /// Exclusive transaction read: snapshot base plus the transaction overlay.
    TransactionOverlay {
        /// Stable base snapshot for the transaction.
        base: Snapshot,
    },
}

impl ReadFrontier {
    /// Return the current durability tier when this is a current read frontier.
    pub(crate) fn durability_tier(&self) -> Option<DurabilityTier> {
        match self {
            Self::Local => Some(DurabilityTier::Local),
            Self::Edge => Some(DurabilityTier::Edge),
            Self::Global => Some(DurabilityTier::Global),
            Self::Historical { .. } | Self::Snapshot { .. } | Self::TransactionOverlay { .. } => {
                None
            }
        }
    }
}

/// Schema view used by source resolution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SchemaView {
    /// Node's current catalogue schema.
    Current,
    /// Query validated against a specific schema version.
    Version {
        /// Schema version used by the query.
        version: SchemaVersionId,
    },
    /// Logical read projected through migration lenses into `to`.
    Projected {
        /// Stored/source schema version.
        from: SchemaVersionId,
        /// Requested/read schema version.
        to: SchemaVersionId,
    },
}

/// Branch/base data view used by source resolution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum BranchView {
    /// Main/base branch.
    Main,
    /// Overlay branch over a base snapshot.
    Branch {
        /// Branch identity.
        branch: BranchId,
        /// Optional base snapshot ref for this branch.
        base: Option<Snapshot>,
    },
}

/// Authenticated policy context for read narrowing and dry-run APIs.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum PolicyContext {
    /// System reads bypass RLS.
    System,
    /// Non-system authenticated identity with trusted session/admission claims.
    Authenticated {
        /// Identity used to evaluate read/write policy.
        identity: AuthorId,
        /// Server-derived claims, never caller-supplied query bindings.
        claims: BTreeMap<String, Value>,
        /// Trust boundary for this policy context.
        trust: PolicyTrust,
    },
}

/// Trust boundary attached to a policy context.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PolicyTrust {
    /// Ordinary client/session context.
    Session,
    /// Trusted backend context.
    TrustedBackend,
    /// Internal relay/system context.
    System,
}

/// How the compiled program will be executed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ExecutionMode {
    /// One-shot read over the chosen frontier.
    OneShot,
    /// Maintained application-facing subscription.
    Subscription {
        /// Subscription tier/source frontier.
        tier: DurabilityTier,
        /// First-delivery and subsequent event behavior.
        delivery: SubscriptionDelivery,
    },
    /// Server-side query-driven sync for a usage-site subscription.
    SyncServing {
        /// Usage-site subscription key.
        subscription: SubscriptionKey,
        /// Whether this execution is a reset/rehydrate attach.
        reset_result_set: bool,
    },
    /// Read inside an exclusive transaction, recording predicate reads.
    TransactionRead,
    /// Hypothetical policy evaluation without ingesting a mutation.
    DryRun {
        /// Policy operation being probed.
        operation: DryRunOperation,
    },
}

/// Delivery behavior for application-facing subscriptions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SubscriptionDelivery {
    /// Local UI delivery, including synchronous local write visibility.
    LocalImmediate,
    /// Wait until the requested upstream tier has settled before first event.
    Settled {
        /// Tier required for first publication.
        tier: DurabilityTier,
    },
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

/// Terminal facts requested from one semantic program.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OutputContract {
    /// Result shape expected by the caller.
    pub(crate) shape: OutputShape,
    /// Terminal events/facts required by the execution mode.
    pub(crate) terminal: TerminalContract,
}

/// User-visible result material requested from a query.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum OutputShape {
    /// Root/current rows only.
    Rows,
    /// Root rows plus relation payload rows and edges.
    RelationPayload,
    /// Rows plus matched include path material.
    MatchedPaths,
    /// No user rows; caller only needs policy/dry-run answer.
    PolicyDecision,
}

/// Side-output facts the maintained/sync layer needs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct TerminalContract {
    /// Required terminal event kinds.
    pub(crate) events: BTreeSet<TerminalEventKind>,
}

impl TerminalContract {
    /// User rows only, with no maintained/sync side outputs.
    pub(crate) fn rows_only() -> Self {
        Self::default()
    }

    /// Maintained subscription/sync terminal contract.
    pub(crate) fn maintained_subscription() -> Self {
        Self {
            events: [
                TerminalEventKind::ResultMembership,
                TerminalEventKind::MatchedPath,
                TerminalEventKind::RelationEdge,
                TerminalEventKind::VersionContent,
                TerminalEventKind::VersionDeletion,
                TerminalEventKind::ReplacementContent,
                TerminalEventKind::ReplacementDeletion,
                TerminalEventKind::PolicyWitness,
                TerminalEventKind::CoverageFact,
                TerminalEventKind::ExclusiveViewCompleteness,
            ]
            .into_iter()
            .collect(),
        }
    }
}

/// Typed event classes emitted by maintained terminal graphs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum TerminalEventKind {
    /// Result-set membership `(table, row_uuid, tx_id)`.
    ResultMembership,
    /// Matched include path or join witness.
    MatchedPath,
    /// Relation payload edge for array subqueries / relation roots.
    RelationEdge,
    /// Content version witness required for payload shipping.
    VersionContent,
    /// Deletion-register witness required for payload shipping.
    VersionDeletion,
    /// Replacement content winner required when a visible row leaves/changes.
    ReplacementContent,
    /// Replacement deletion winner required when a visible row leaves/changes.
    ReplacementDeletion,
    /// Policy dependency that can grant or revoke visibility.
    PolicyWitness,
    /// Source/table/key coverage fact.
    CoverageFact,
    /// View-scoped completeness for an exclusive transaction payload.
    ExclusiveViewCompleteness,
}

/// Normalized Jazz relational IR before physical lowering to groove.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum JazzRel {
    /// Scan a logical table through source resolution.
    Source(SourceDescriptor),
    /// Filter rows with a Jazz predicate.
    Filter {
        /// Input relation.
        input: Box<JazzRel>,
        /// Predicate to evaluate.
        predicate: Predicate,
    },
    /// Project or retarget row fields.
    Project {
        /// Input relation.
        input: Box<JazzRel>,
        /// Projected fields.
        fields: Vec<ProjectionField>,
    },
    /// Relational join.
    Join {
        /// Left input.
        left: Box<JazzRel>,
        /// Right input.
        right: Box<JazzRel>,
        /// Join keys.
        on: Vec<JoinCondition>,
        /// Join kind.
        kind: JoinKind,
    },
    /// Anti-join.
    AntiJoin {
        /// Left input.
        left: Box<JazzRel>,
        /// Right input.
        right: Box<JazzRel>,
        /// Join keys.
        on: Vec<JoinCondition>,
    },
    /// Union of compatible inputs.
    Union {
        /// Inputs to union.
        inputs: Vec<JazzRel>,
    },
    /// Distinct rows by key.
    Distinct {
        /// Input relation.
        input: Box<JazzRel>,
        /// Stable distinct key.
        key: Vec<KeyExpr>,
    },
    /// Recursive reachability/gather expression.
    Recursive {
        /// Initial seed relation.
        seed: Box<JazzRel>,
        /// Step relation using the frontier source.
        step: Box<JazzRel>,
        /// Frontier key.
        frontier_key: Vec<KeyExpr>,
        /// Maximum iteration count.
        max_depth: usize,
    },
    /// Ordered finite window.
    TopBy {
        /// Input relation.
        input: Box<JazzRel>,
        /// User order terms plus stable ties.
        order_by: Vec<OrderBy>,
        /// Offset retained in the window.
        offset: usize,
        /// Finite limit retained in the window.
        limit: usize,
    },
    /// Aggregate result.
    Aggregate {
        /// Input relation.
        input: Box<JazzRel>,
        /// Aggregate shape.
        aggregate: AggregateQuery,
    },
    /// Attach typed terminal facts to an input relation.
    Terminal {
        /// Input relation.
        input: Box<JazzRel>,
        /// Terminal contract to emit.
        contract: TerminalContract,
    },
}

/// Logical table source before it becomes a concrete groove graph.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SourceDescriptor {
    /// Logical table requested by the query.
    pub(crate) table: String,
    /// Read source selected for that table.
    pub(crate) source: ResolvedSource,
    /// Output row descriptor expected after source resolution.
    pub(crate) row_shape: RowShape,
}

/// Concrete source category selected for a logical table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ResolvedSource {
    /// Current visible rows at a durability tier.
    VisibleCurrent {
        /// Durability tier.
        tier: DurabilityTier,
    },
    /// Current root rows including deletion marker.
    IncludeDeletedCurrent {
        /// Durability tier.
        tier: DurabilityTier,
    },
    /// Historical visible rows at a global cut.
    HistoricalCurrent {
        /// Inclusive global sequence cut.
        position: GlobalSeq,
    },
    /// Branch overlay rows.
    BranchOverlay {
        /// Branch identity.
        branch: BranchId,
    },
    /// Exclusive transaction overlay rows.
    TransactionOverlay,
    /// Inline rows used as a staging representation for source gaps.
    InlineSnapshot,
}

/// Canonical row shape emitted by source resolution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RowShape {
    /// Descriptor of the record emitted by this source.
    pub(crate) descriptor: RecordDescriptor,
    /// Field containing row identity.
    pub(crate) row_uuid_field: String,
    /// Field containing content transaction time when present.
    pub(crate) content_tx_time_field: Option<String>,
    /// Field containing content transaction node alias when present.
    pub(crate) content_tx_node_field: Option<String>,
    /// Whether this source includes a deletion marker field.
    pub(crate) includes_deletion_marker: bool,
}

/// Projected output field.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ProjectionField {
    /// Source expression.
    pub(crate) expr: ValueExpr,
    /// Output field name.
    pub(crate) alias: String,
}

/// Value expression used by normalized relational nodes.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ValueExpr {
    /// Named column.
    Column(String),
    /// Stable row id.
    RowId,
    /// Literal value.
    Literal(Value),
    /// Query binding parameter.
    Param(String),
    /// Server-derived claim.
    Claim(String),
}

/// Join condition between two inputs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct JoinCondition {
    /// Left key expression.
    pub(crate) left: KeyExpr,
    /// Right key expression.
    pub(crate) right: KeyExpr,
}

/// Join kind represented by the unified IR.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JoinKind {
    /// Inner join.
    Inner,
    /// Left join, capability-gated until maintained semantics are implemented.
    Left,
}

/// Stable key expression for joins, distinct, and recursion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum KeyExpr {
    /// Named column key.
    Column(String),
    /// Row id key.
    RowId,
    /// Relation key reference from alpha-style relation IR.
    Relation(RelationKeyRef),
}

/// Parameter domain for a prepared program.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ParameterDomain {
    /// User-supplied binding parameters.
    pub(crate) user_params: BTreeMap<String, ColumnType>,
    /// Server-derived hidden parameters such as claims.
    pub(crate) hidden_params: BTreeMap<String, ColumnType>,
    /// Parameters retained in terminal rows for binding routing.
    pub(crate) routing_params: BTreeSet<String>,
}

/// Capability verdict for the current implementation of a program.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CapabilityPlan {
    /// Whether the program can be served as a maintained subscription.
    pub(crate) maintained_subscription: Capability,
    /// Whether the program can be executed as a one-shot read.
    pub(crate) one_shot: Capability,
    /// Whether the program can produce sync terminal facts.
    pub(crate) sync_terminal: Capability,
}

/// Capability status for one execution mode.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Capability {
    /// Supported by the unified lowering path.
    Supported,
    /// Not yet supported; caller must fail loudly instead of falling back.
    Unsupported {
        /// Stable diagnostic reason.
        reason: UnsupportedReason,
    },
}

/// Reason a shape is not yet supported by the unified maintained path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum UnsupportedReason {
    /// Source/frontier is not yet representable as a maintained groove source.
    Source(SourceGap),
    /// Operator is not yet represented in maintained groove lowering.
    Operator(String),
    /// Terminal facts requested by the caller are not yet emitted.
    Terminal(TerminalEventKind),
    /// Policy composition for this shape is not yet lowered.
    Policy(String),
}

/// Source-resolution gap.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SourceGap {
    /// Historical source cannot yet be maintained incrementally.
    Historical,
    /// Schema-projected/lensed source cannot yet be maintained incrementally.
    SchemaProjected,
    /// Branch overlay source cannot yet be maintained incrementally.
    BranchOverlay,
    /// Transaction overlay source cannot yet be maintained incrementally.
    TransactionOverlay,
}

/// Result of compiling one query request.
#[derive(Clone, Debug)]
pub(crate) struct CompiledQueryProgram {
    /// Original request.
    pub(crate) request: QueryEngineRequest,
    /// Normalized semantic IR after policy/source composition.
    pub(crate) logical: JazzRel,
    /// Parameter domain used by any prepared/routed groove program.
    pub(crate) parameters: ParameterDomain,
    /// Concrete groove graphs for the requested execution.
    pub(crate) physical: PhysicalProgram,
    /// Capability verdicts for supported execution modes.
    pub(crate) capabilities: CapabilityPlan,
    /// Human-readable explain plan fragments for debugging and tests.
    pub(crate) explain: ExplainPlan,
}

/// Groove-level program emitted by the Jazz compiler.
#[derive(Clone, Debug)]
pub(crate) enum PhysicalProgram {
    /// Single graph, used by one-shot reads and simple subscriptions.
    Graph {
        /// Executable groove graph.
        graph: GraphBuilder,
    },
    /// Prepared graph with a groove binding shape.
    Prepared {
        /// Prepared shape id.
        shape: PreparedShapeId,
        /// Names in binding order.
        param_names: Vec<String>,
        /// Types in binding order.
        param_types: Vec<ColumnType>,
    },
    /// Maintained terminal graph plus optional routing graph.
    MaintainedTerminal {
        /// Public terminal event graph.
        terminal_graph: GraphBuilder,
        /// Routing graph retaining binding parameters for multi-binding reuse.
        routing_graph: Option<GraphBuilder>,
        /// Terminal contract decoded from the terminal graph.
        contract: TerminalContract,
    },
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
    /// Capability decisions.
    pub(crate) capabilities: Vec<String>,
    /// Physical graph summaries.
    pub(crate) physical: Vec<String>,
}

/// Subscriber-side result cache keyed by the same shape/binding vocabulary.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct SettledResultCache {
    /// Settled result entries by usage-site subscription.
    pub(crate) by_subscription: BTreeMap<SubscriptionKey, BTreeSet<ResultRowEntry>>,
    /// Binding ids represented by each shared coverage group.
    pub(crate) coverage_groups: BTreeMap<(ShapeId, BindingId), BTreeSet<SubscriptionKey>>,
}

/// Typed maintained event after decoding a terminal row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum MaintainedEvent {
    /// Result membership event.
    Result(ResultRowEntry),
    /// Matched include/join path witness.
    MatchedPath {
        /// Source result row.
        source: ResultRowEntry,
        /// Witness row.
        witness: ResultRowEntry,
    },
    /// Relation payload edge.
    RelationEdge {
        /// Source table.
        source_table: String,
        /// Source row.
        source_row: RowUuid,
        /// Relation name.
        relation: String,
        /// Target table.
        target_table: String,
        /// Target row.
        target_row: RowUuid,
    },
    /// Version witness.
    Version {
        /// Version transaction id.
        tx_id: TxId,
        /// Version layer.
        layer: TerminalVersionLayer,
    },
    /// Replacement witness for a row.
    Replacement {
        /// Table containing the row.
        table: String,
        /// Row identity.
        row: RowUuid,
        /// Replacement layer.
        layer: TerminalVersionLayer,
    },
    /// Coverage or completeness fact.
    Coverage(CoverageFact),
}

/// Version layer carried by terminal witness events.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TerminalVersionLayer {
    /// Content version.
    Content,
    /// Deletion-register version.
    Deletion,
}

/// Query/source coverage fact.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CoverageFact {
    /// A table/key set is covered for a subscription.
    SourceRows {
        /// Logical table.
        table: String,
        /// Covered row ids when known exactly.
        rows: BTreeSet<RowUuid>,
    },
    /// A transaction is complete for this maintained view only.
    ViewCompleteExclusive {
        /// Exclusive transaction id.
        tx_id: TxId,
    },
    /// Complete transaction payload has been shipped to a peer.
    CompleteTransaction {
        /// Transaction id.
        tx_id: TxId,
    },
}
