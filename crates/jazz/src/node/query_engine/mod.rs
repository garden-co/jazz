#![allow(dead_code)]

//! Destination vocabulary for the unified Jazz query engine.
//!
//! The intended shape is deliberately small:
//!
//! 1. public query surfaces validate into [`QuerySurface`],
//! 2. callers choose one [`ReadView`] and one [`PolicySubject`],
//! 3. callers choose either a query surface or policy probe as input,
//! 4. callers choose a semantic [`TerminalProgram`] and explicit effects,
//! 5. one lowering pass resolves sources, policy, relation semantics, terminal
//!    payloads, and transaction read tracking into groove IVM graphs.
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

use super::OpenTxId;
use crate::ids::{AuthorId, BranchId, RowUuid, SchemaVersionId};
use crate::protocol::SubscriptionKey;
use crate::query::{Binding, BindingId, RelationQuery, ShapeId, ValidatedQuery};
use crate::schema::TableSchema;
use crate::time::GlobalSeq;
use crate::tx::{DurabilityTier, Snapshot, TxId};

/// One validated API request before semantic lowering.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct QueryProgramRequest {
    /// User-facing query or policy probe that entered the engine.
    pub(crate) input: QueryProgramInput,
    /// Exact data view used for source resolution.
    pub(crate) view: ReadView,
    /// Identity and claims used by policy augmentation.
    pub(crate) subject: PolicySubject,
    /// Semantic terminal program requested from this query.
    pub(crate) terminal: TerminalProgram,
    /// Side effects the caller expects from the same lowered program.
    pub(crate) effects: ProgramEffects,
}

/// Input accepted by the unified query engine.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum QueryProgramInput {
    /// User-facing query vocabulary.
    Surface(QuerySurface),
    /// Policy dry-run probe. Write probes are not fake query shapes.
    PolicyProbe(PolicyProbe),
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
        /// Validated, schema-stamped relation query shape.
        shape: ValidatedRelationQuery,
        /// Binding values for this use of the shape.
        binding: Binding,
    },
}

/// Validated relation query shape.
///
/// This is the destination shape for moving relation validation into
/// `crate::query`: relation queries must be as addressable and cacheable as
/// ordinary query shapes, otherwise relation live queries grow a parallel
/// identity model.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ValidatedRelationQuery {
    /// Canonical relation query.
    pub(crate) query: RelationQuery,
    /// Schema version this relation query was authored and validated against.
    pub(crate) schema_version: SchemaVersionId,
    /// Inferred parameter types by name.
    pub(crate) params: BTreeMap<String, ColumnType>,
    /// Canonical bytes used for shape identity.
    pub(crate) canonical: Vec<u8>,
    /// Content-addressed shape id, in a relation-discriminated namespace.
    pub(crate) shape_id: ShapeId,
}

/// Stable identity of a query surface plus binding.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct SurfaceIdentity {
    /// Surface vocabulary kind.
    pub(crate) kind: QuerySurfaceKind,
    /// Content-addressed shape id.
    pub(crate) shape_id: ShapeId,
    /// Content-addressed binding id.
    pub(crate) binding_id: BindingId,
}

/// Query surface category used in shared-program keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum QuerySurfaceKind {
    /// Ordinary root-table query.
    Query,
    /// Relation query with an explicit terminal row set.
    Relation,
}

/// Concrete read view selected before source resolution.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ReadView {
    /// Schema axes used by source and policy resolution.
    pub(crate) schemas: SchemaContext,
    /// Data source/frontier selected for the read.
    pub(crate) source: ReadSource,
}

impl ReadView {
    /// Return the schema version visible to application/query semantics.
    pub(crate) fn read_schema(&self) -> SchemaVersionId {
        self.schemas.read_schema
    }

    /// Return the current durability tier when this is a current read.
    pub(crate) fn current_tier(&self) -> Option<DurabilityTier> {
        match self.source {
            ReadSource::Current { tier } => Some(tier),
            ReadSource::Historical { .. }
            | ReadSource::Snapshot { .. }
            | ReadSource::Transaction { .. }
            | ReadSource::Branch { .. }
            | ReadSource::MergedBranches { .. }
            | ReadSource::SettledResultSet { .. } => None,
        }
    }
}

/// Schema axes for a read.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct SchemaContext {
    /// Schema version exposed to the query and application rows.
    pub(crate) read_schema: SchemaVersionId,
    /// Schema version used to evaluate read/write policies.
    pub(crate) policy_schema: SchemaVersionId,
    /// Schema version used to interpret proposed write cells, when present.
    pub(crate) write_schema: Option<SchemaVersionId>,
    /// Stored schema partitions considered by source resolution.
    pub(crate) storage: StorageSchemaSelection,
}

/// Stored schema partitions to read before projecting into `read_schema`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum StorageSchemaSelection {
    /// Resolver chooses every compatible partition for the current catalogue.
    CompatiblePartitions,
    /// Read one stored schema partition.
    Single(SchemaVersionId),
    /// Read an explicit partition set, usually for tests or snapshot refs.
    Explicit(BTreeSet<SchemaVersionId>),
}

/// Data source/frontier selected for a read.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ReadSource {
    /// Current rows at one durability tier.
    Current {
        /// Local, edge, or global source currency.
        tier: DurabilityTier,
    },
    /// Historical global cut.
    Historical {
        /// Inclusive global sequence cut.
        position: GlobalSeq,
    },
    /// Dotted snapshot ref.
    Snapshot {
        /// Snapshot frontier.
        snapshot: Snapshot,
    },
    /// Exclusive transaction read over a snapshot base plus overlay writes.
    Transaction {
        /// Stable base snapshot for the transaction.
        base: Snapshot,
        /// Overlay identity and lifetime.
        overlay: TransactionOverlay,
    },
    /// Branch read. The branch record owns its frozen base snapshot.
    Branch {
        /// Branch identity.
        branch: BranchId,
    },
    /// LWW merge of multiple branch overlays, used by alpha branch-list reads
    /// if that facade surface remains part of integration.
    MergedBranches {
        /// Branches participating in the merged read.
        branches: BTreeSet<BranchId>,
    },
    /// Read through a maintained usage-site result set.
    SettledResultSet {
        /// Usage-site subscription whose settled rows are the root source.
        subscription: SubscriptionKey,
    },
}

/// Transaction overlay identity selected by exclusive transaction reads.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum TransactionOverlay {
    /// Open transaction whose pending writes are visible and mutable.
    Open(OpenTxId),
    /// Committed transaction overlay used by validation or replay.
    Committed(TxId),
    /// Inline overlay supplied by a future test/repair caller.
    Inline {
        /// Stable diagnostic label for the inline overlay.
        label: String,
    },
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

/// Stable policy identity used to decide whether compiled programs can share work.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum PolicySharingKey {
    /// Internal/system policy bypass.
    System,
    /// Authenticated policy context. Claims are addressed by canonical bytes to
    /// avoid making `Value` itself part of a hash key.
    Identity {
        /// Identity whose permissions are being evaluated.
        permission_subject: AuthorId,
        /// Identity recorded on writes, when it differs from the permission subject.
        attribution: Option<AuthorId>,
        /// Canonical fingerprint of trusted claims.
        claims_fingerprint: Vec<u8>,
    },
}

/// Semantic terminal program requested from one lowered query.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum TerminalProgram {
    /// Row materialization for one-shot `query()` calls and relation snapshots.
    Rows(RowSetTerminalRequirements),
    /// Maintained application/sync view with explicit terminal requirements.
    MaintainedView(RowSetTerminalRequirements),
    /// Policy dry-run decision.
    PolicyDecision,
}

/// Row-set terminal requirements shared by one-shot and maintained programs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RowSetTerminalRequirements {
    /// Deletion visibility for the root result source.
    pub(crate) root_deletion: RootDeletionMode,
    /// Semantic terminal facts requested by the caller.
    pub(crate) terminals: TerminalRequirements,
}

/// Deletion visibility for the root result source.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum RootDeletionMode {
    /// Only currently visible rows.
    Visible,
    /// Include root deletion markers, while joins/includes still use visible rows.
    IncludeDeleted,
}

/// Semantic terminal facts requested from a maintained graph.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct TerminalRequirements {
    /// Required terminal fact categories.
    pub(crate) facts: BTreeSet<TerminalRequirementKind>,
}

/// Terminal fact category requested from a maintained graph.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum TerminalRequirementKind {
    /// Root result-set membership rows.
    ResultMembership,
    /// Matched include/join path rows.
    MatchedPaths,
    /// Relation edge rows for array subqueries and relation APIs.
    RelationEdges,
    /// Content/deletion version witnesses needed for payload shipping.
    VersionWitnesses,
    /// Replacement witnesses for visible rows that leave or change.
    ReplacementWitnesses,
    /// Policy dependency witnesses that can grant or revoke visibility.
    PolicyWitnesses,
    /// Source/table coverage facts.
    SourceCoverage,
    /// View-scoped completeness facts for partial exclusive transaction payloads.
    ViewCompleteExclusive,
    /// Aggregate group result rows.
    AggregateRows,
    /// Ordered/ranked finite-window result rows.
    WindowRows,
    /// Large-value column authorization/materialization witnesses.
    LargeValueWitnesses,
}

/// Dry-run policy probe.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum PolicyProbe {
    /// Existing row visibility check.
    CanRead {
        /// Logical table.
        table: String,
        /// Row identity.
        row: RowUuid,
    },
    /// Proposed insert.
    CanInsert {
        /// Logical table.
        table: String,
        /// Row identity semantics for the proposed insert.
        row: InsertRowId,
        /// Proposed application cells.
        new_values: BTreeMap<String, Value>,
    },
    /// Proposed update. The old row is resolved from `ReadView`.
    CanUpdate {
        /// Logical table.
        table: String,
        /// Row identity.
        row: RowUuid,
        /// Proposed update cells.
        proposal: WriteProposal,
    },
    /// Proposed delete. The old row is resolved from `ReadView`.
    CanDelete {
        /// Logical table.
        table: String,
        /// Row identity.
        row: RowUuid,
    },
}

/// Row identity semantics for insert probes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum InsertRowId {
    /// Caller supplied the row id before policy evaluation.
    Provided(RowUuid),
    /// Engine must allocate/bind the row id before policy evaluation.
    GenerateBeforeProbe,
    /// Probe is only valid when policy does not inspect row identity.
    IdIndependentOnly,
}

/// Proposed write shape for update/upsert-style policy checks.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum WriteProposal {
    /// Patch semantics: omitted fields keep their current value.
    Patch(BTreeMap<String, Value>),
    /// After-image semantics: supplied values are the complete proposed row.
    AfterImage(BTreeMap<String, Value>),
    /// Deletion/restoration intent with no cell patch.
    StateChange(WriteStateChange),
}

/// Proposed row-state change.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum WriteStateChange {
    /// Delete the row.
    Delete,
    /// Restore a previously deleted row.
    Restore,
}

/// Side effects requested from the same lowered query program.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ProgramEffects {
    /// Predicate-read tracking for exclusive transaction reads.
    pub(crate) predicate_reads: PredicateReadMode,
}

/// Predicate-read tracking requested from a query.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) enum PredicateReadMode {
    /// Do not emit predicate reads.
    #[default]
    None,
    /// Emit predicate reads for the caller to record.
    ReturnToCaller,
    /// Record predicate reads against the open transaction named by `ReadSource`.
    RecordForTransaction,
}

/// Stable key for safe sharing of compiled maintained work.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ProgramSharingKey {
    /// Query/probe identity.
    pub(crate) input: ProgramInputKey,
    /// Resolved read/source identity. This is post-catalogue resolution: no
    /// `CompatiblePartitions` or other deferred choices may remain here.
    pub(crate) read: ResolvedReadKey,
    /// Policy identity and claims.
    pub(crate) policy: PolicySharingKey,
    /// Terminal fact requirements.
    pub(crate) terminal: TerminalSharingKey,
    /// Side-effect requirements.
    pub(crate) effects: ProgramEffectsKey,
}

/// Resolved read identity used by shared maintained work.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ResolvedReadKey {
    /// Schema identity after source/lens resolution.
    pub(crate) schemas: ResolvedSchemaKey,
    /// Concrete source identity after branch/catalogue/result-set resolution.
    pub(crate) source: ResolvedSourceKey,
}

/// Resolved schema identity used by shared maintained work.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ResolvedSchemaKey {
    /// Schema version exposed to the query and application rows.
    pub(crate) read_schema: SchemaVersionId,
    /// Schema version used to evaluate policies.
    pub(crate) policy_schema: SchemaVersionId,
    /// Schema version used to interpret proposed write cells, when present.
    pub(crate) write_schema: Option<SchemaVersionId>,
    /// Concrete storage partitions read by the source resolver.
    pub(crate) storage_partitions: BTreeSet<SchemaVersionId>,
    /// Canonical fingerprint of applied lens/projection path.
    pub(crate) lens_path_fingerprint: Vec<u8>,
    /// Catalogue/source-resolution epoch or fingerprint.
    pub(crate) catalogue_fingerprint: Vec<u8>,
}

/// Resolved source identity used by shared maintained work.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ResolvedSourceKey {
    /// Current rows at one durability tier.
    Current {
        /// Local, edge, or global source currency.
        tier: DurabilityTier,
    },
    /// Historical global cut over a resolved source catalogue.
    Historical {
        /// Inclusive global sequence cut.
        position: GlobalSeq,
    },
    /// Dotted snapshot ref.
    Snapshot {
        /// Snapshot frontier.
        snapshot: Snapshot,
    },
    /// Exclusive transaction read over a snapshot base plus overlay writes.
    Transaction {
        /// Stable base snapshot for the transaction.
        base: Snapshot,
        /// Overlay identity and lifetime.
        overlay: TransactionOverlay,
    },
    /// Branch read at a resolved branch metadata generation.
    Branch {
        /// Branch identity.
        branch: BranchId,
        /// Canonical branch metadata/source generation.
        generation: Vec<u8>,
    },
    /// Merged branch-set read at a resolved merge generation.
    MergedBranches {
        /// Branches participating in the merged read.
        branches: BTreeSet<BranchId>,
        /// Canonical merge/source generation.
        generation: Vec<u8>,
    },
    /// Read through a maintained usage-site result set.
    SettledResultSet {
        /// Usage-site subscription whose settled rows are the root source.
        subscription: SubscriptionKey,
        /// Result-set generation/fingerprint.
        generation: Vec<u8>,
    },
}

/// Stable input identity used by shared maintained work.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ProgramInputKey {
    /// Query shape and binding identity.
    Surface(SurfaceIdentity),
    /// Policy probe identity.
    PolicyProbe(PolicyProbeKey),
}

/// Stable policy-probe identity.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct PolicyProbeKey {
    /// Probe kind.
    pub(crate) kind: PolicyProbeKind,
    /// Logical table.
    pub(crate) table: String,
    /// Row identity semantics for the probe.
    pub(crate) row: PolicyProbeRowKey,
    /// Canonical fingerprint of proposed values, when present.
    pub(crate) proposed_values_fingerprint: Option<Vec<u8>>,
}

/// Stable row identity component for policy-probe keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum PolicyProbeRowKey {
    /// Probe targets this concrete row.
    Row(RowUuid),
    /// Engine generated or must generate a row id before policy evaluation.
    Generated,
    /// Probe is only valid for id-independent policy.
    IdIndependent,
    /// Probe has no row identity component.
    None,
}

/// Policy-probe category.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum PolicyProbeKind {
    /// Existing row visibility check.
    CanRead,
    /// Proposed insert.
    CanInsert,
    /// Proposed update.
    CanUpdate,
    /// Proposed delete.
    CanDelete,
}

/// Stable terminal identity used by shared maintained work.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct TerminalSharingKey {
    /// Terminal program kind.
    pub(crate) kind: TerminalProgramKind,
    /// Root deletion mode, when the terminal has root rows.
    pub(crate) root_deletion: Option<RootDeletionMode>,
    /// Required maintained terminal fact categories.
    pub(crate) facts: BTreeSet<TerminalRequirementKind>,
}

/// Terminal program category for sharing keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum TerminalProgramKind {
    /// One-shot rows.
    Rows,
    /// Maintained view.
    MaintainedView,
    /// Policy dry-run decision.
    PolicyDecision,
}

/// Stable effect identity used by shared maintained work.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ProgramEffectsKey {
    /// No side effects.
    None,
    /// Predicate reads are returned to the caller.
    PredicateReadsReturned,
    /// Predicate reads are recorded against the transaction read source.
    PredicateReadsRecorded,
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
    /// Aggregate group rows.
    pub(crate) aggregate_rows: Option<AggregateResultSchema>,
    /// Ordered/ranked window rows.
    pub(crate) window_rows: Option<WindowResultSchema>,
    /// Large-value authorization/materialization witnesses.
    pub(crate) large_value_witnesses: Option<LargeValueWitnessSchema>,
    /// Source/table coverage rows.
    pub(crate) source_coverage: Option<SourceCoverageSchema>,
    /// View-scoped completeness rows for partial exclusive transactions.
    pub(crate) view_complete_exclusive: Option<ViewCompleteExclusiveSchema>,
}

/// Root result membership terminal row schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ResultMembershipSchema {
    /// Logical table field in the terminal row.
    pub(crate) table_field: String,
    /// Row identity field in the terminal row.
    pub(crate) row_field: String,
    /// Version fields that identify the result row in the maintained set.
    pub(crate) version: ResultMembershipVersionSchema,
    /// Retained binding/routing parameter fields.
    pub(crate) routing_param_fields: BTreeSet<String>,
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
    /// Canonical traversal-node id field. For relation queries this identifies
    /// the `RelationExpr` node; for array subqueries it identifies the relation name.
    pub(crate) relation_field: String,
    /// Target row fields.
    pub(crate) target: ResultMembershipSchema,
    /// Recursive depth field, when emitted by gather/reachability.
    pub(crate) depth_field: Option<String>,
    /// Multipath/path-id field, when a relation can produce multiple paths to
    /// the same target.
    pub(crate) path_field: Option<String>,
    /// Union/branch alternative field, when emitted by union or policy branches.
    pub(crate) branch_field: Option<String>,
    /// Terminal role field, for distinguishing intermediate, frontier, and
    /// terminal-output edges.
    pub(crate) role_field: Option<String>,
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
    /// Protected/root row whose visibility is affected.
    pub(crate) protected: ResultMembershipSchema,
    /// Policy clause or branch field.
    pub(crate) policy_path_field: String,
    /// Witness row fields.
    pub(crate) witness: ResultMembershipSchema,
    /// Dependency edge kind field, for join/reachability/branch gates.
    pub(crate) edge_kind_field: String,
}

/// Aggregate group terminal row schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AggregateResultSchema {
    /// Stable group-key fields.
    pub(crate) group_key_fields: BTreeSet<String>,
    /// Aggregate value fields.
    pub(crate) value_fields: BTreeSet<String>,
    /// Retained binding/routing parameter fields.
    pub(crate) routing_param_fields: BTreeSet<String>,
}

/// Ordered/ranked window terminal row schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WindowResultSchema {
    /// Result membership row represented in the window.
    pub(crate) result: ResultMembershipSchema,
    /// Order-by value fields retained for recomputation.
    pub(crate) order_fields: BTreeSet<String>,
    /// Internal retained window-position witness, when needed by the graph.
    pub(crate) position_witness_field: Option<String>,
}

/// Large-value witness terminal row schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LargeValueWitnessSchema {
    /// Row containing the large-value column.
    pub(crate) owner: ResultMembershipSchema,
    /// Column name field.
    pub(crate) column_field: String,
    /// Content extent or materialization key field.
    pub(crate) extent_field: String,
}

/// Source coverage terminal row schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SourceCoverageSchema {
    /// Logical table field.
    pub(crate) table_field: String,
    /// Covered row field, when coverage is row-specific.
    pub(crate) row_field: Option<String>,
    /// Coverage mode or key-range field.
    pub(crate) coverage_field: String,
    /// Retained binding/routing parameter fields.
    pub(crate) routing_param_fields: BTreeSet<String>,
}

/// View-scoped exclusive completeness terminal row schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ViewCompleteExclusiveSchema {
    /// Exclusive transaction time field.
    pub(crate) tx_time_field: String,
    /// Exclusive transaction node field.
    pub(crate) tx_node_field: String,
    /// Usage/view scope field, when emitted by sync-serving graphs.
    pub(crate) view_scope_field: Option<String>,
    /// Retained binding/routing parameter fields.
    pub(crate) routing_param_fields: BTreeSet<String>,
}

/// Logical source request made by query, policy, or terminal lowering.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SourceRequest {
    /// Logical table requested by the compiler.
    pub(crate) table: String,
    /// Data view for this source.
    pub(crate) view: ReadView,
    /// Deletion visibility expected from this source.
    pub(crate) deletion_scope: DeletionScope,
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
    /// Storage source for a historical global cut cannot yet be built.
    HistoricalStorageCut,
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
pub(crate) type QueryCompileResult = Result<QueryProgram, CapabilityReport>;

/// Runnable lowered query program.
#[derive(Clone, Debug)]
pub(crate) struct QueryProgram {
    /// Original request.
    pub(crate) request: QueryProgramRequest,
    /// Groove graph and its boundary contracts.
    pub(crate) lowered: LoweredGraph,
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
    /// Side-effect schemas emitted by the graph/runtime.
    pub(crate) effects: ProgramEffectSchemas,
}

/// Side-effect schemas emitted by a lowered program.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ProgramEffectSchemas {
    /// Predicate-read effect emitted by transaction reads.
    pub(crate) predicate_reads: Option<PredicateReadEffectSchema>,
}

/// Predicate-read side-effect schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PredicateReadEffectSchema {
    /// Shape id recorded for the predicate read.
    pub(crate) shape_id: ShapeId,
    /// Binding id recorded for the predicate read.
    pub(crate) binding_id: BindingId,
    /// Binding value fields retained for validation without prior registration.
    pub(crate) binding_value_fields: BTreeSet<String>,
}

/// Capability status for an unsupported requested program.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct CapabilityReport {
    /// Unsupported pieces. An empty list means the requested program is supported.
    pub(crate) gaps: Vec<UnsupportedReason>,
    /// Human-readable debugging and test artifact for the failed lowering.
    pub(crate) explain: ExplainPlan,
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
    Terminal(TerminalRequirementKind),
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
    /// Side-effect decisions.
    pub(crate) effects: Vec<String>,
    /// Capability decisions.
    pub(crate) capabilities: Vec<String>,
    /// Physical graph summaries.
    pub(crate) physical: Vec<String>,
}
