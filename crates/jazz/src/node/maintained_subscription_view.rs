use std::collections::{BTreeMap, BTreeSet};
use std::mem;
use std::rc::Rc;
use std::sync::Arc;

use groove::ivm::{
    MultisinkDeltas, RecordDeltas, TerminalEdit, TerminalOperation, TerminalPathSegment,
};
use groove::records::{
    BorrowedRecord, EnumValue, OwnedRecord, RecordDescriptor, RecordProjector, Value, ValueType,
};

use super::codec::{
    VersionLayer, VersionRow, VersionRowParts, authored_column_ids_from_value,
    deletion_event_from_value, history_values_from_parts, nullable_value,
    register_values_from_parts, runtime_result_identity_bytes, tx_ids_from_value,
    version_tx_id_from_aliases,
};
use super::query_engine::{
    AggregateResultSchema, AppRowCarrier, AppRowSchema, OutputTerminalSchema, ProgramFactKey,
    ProgramFactSchema, ProgramFactTerminal, QueryProgram, RelationEdgeSchema,
    ResultMembershipSchema, ResultMembershipVersionSchema, TypedOutputField, VersionWitnessSchema,
    VersionedRowRefSchema,
};
use crate::db::{TerminalRootCarrier, TerminalRootLayout, TerminalRootPublicField};
use crate::ids::{NodeAlias, NodeUuid, RowAuthor, RowUuid, SchemaVersionAlias};
use crate::node::{CurrentRowPublicationField, CurrentRowResultVisibility};
#[cfg(test)]
use crate::protocol::CoveredInputEntry;
use crate::protocol::{
    BranchKey, ProgramFactEntry, ProgramSourceId, RealRowMemberEntry, RelationEdgeEntry,
    ResultMemberEntry, ResultMemberPayloadEntry, ResultRowLayer, RowVersionRefEntry, SupportingRow,
    SyntheticReplacementToken,
};
use crate::schema::{RuntimeSchema, TableSchema};
use crate::time::{GlobalTime, TxTime};
use crate::tools::{ObjectId, OutputOccurrenceId};
use crate::tx::TxId;

type TableSchemas = BTreeMap<String, TableSchema>;
type VersionDecodePlanCache = BTreeMap<(String, VersionLayer), VersionDecodePlan>;

#[derive(Clone, Debug)]
struct VersionDecodePlan {
    descriptor: RecordDescriptor,
    branch_idx: Option<usize>,
    row_idx: usize,
    tx_time_idx: usize,
    tx_node_idx: usize,
    schema_version_idx: usize,
    parents_idx: usize,
    created_by_idx: usize,
    created_at_idx: usize,
    updated_by_idx: usize,
    updated_at_idx: usize,
    user_indices: BTreeMap<String, usize>,
    authored_columns_idx: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct MaintainedSubscriptionView {
    /// Keep reader exclusion inputs alive until the final serving view closes.
    pub(crate) edge_availability_owner:
        Option<std::sync::Arc<super::query_eval::EdgeAvailabilityOwner>>,
    /// The immutable resolved read-view identity of this maintained program.
    /// Terminal row members must retain it so distinct branch views never
    /// collapse when their source row and transaction coincide.
    read_view: crate::protocol::ReadViewKey,
    /// The graph uses read-schema names; immutable payload coordinates use
    /// the name of that same physical table in the authored schema.
    witness_table_names: BTreeMap<(String, SchemaVersionAlias), String>,
    result_weights: RetainedResultMap<i64>,
    /// Result memberships already exposed to the subscription consumer. A
    /// result-current terminal can advance before the companion content
    /// witness terminal, so raw membership alone is not publishable.
    published_result_members: RetainedResultMembers,
    /// After a complete storage-backed baseline, only changed result weights
    /// can affect publishability. Keep candidates across partial/failed drains;
    /// `None` requires a full reconcile (also used by witness-gated views).
    unreconciled_result_members: Option<RetainedResultMembers>,
    #[cfg(test)]
    result_member_reconcile_visits: usize,
    result_payloads: RetainedResultMap<ResultMemberPayloadEntry>,
    /// Payloads paired with memberships already exposed to a consumer. Keep
    /// this separate from `result_payloads`: the latter records the raw
    /// result-terminal state while a membership waits for its content witness.
    published_result_payloads: RetainedResultMap<ResultMemberPayloadEntry>,
    /// Incrementally maintained collector output, keyed by the opaque Groove
    /// terminal key and then encoded tree. A public root UUID is not a
    /// sufficient identity: one flat relation can contain several occurrences
    /// of that root with distinct joined descendants.
    structured_app_rows: BTreeMap<Vec<u8>, BTreeMap<Vec<u8>, i64>>,
    /// A root switches from its encoded seed to decoded state on its first
    /// descendant edit. Never retain a second encoded copy while it evolves.
    structured_terminal_records: BTreeMap<Vec<u8>, crate::db::terminal_record::TerminalRecordState>,
    /// Runtime terminal edits address roots by their opaque Groove key. Keep
    /// the compiler-emitted association solely to target a root's descendants;
    /// it must never be used to collapse retained occurrence records.
    structured_root_keys: BTreeMap<Vec<u8>, RowUuid>,
    /// Exact collector order for root terminal keys. Root UUIDs are not a
    /// sequence key: one flat relation can validly contain more than one
    /// occurrence of the same root.
    structured_root_key_order: Vec<Vec<u8>>,
    #[cfg(test)]
    root_order_insert_comparisons: usize,
    structured_app_row_descriptor: Option<RecordDescriptor>,
    /// Whether this maintained subscription retains the recursive app-row
    /// collector. Flat unordered subscriptions release it after their reset;
    /// subsequent terminal deltas must not rebuild the duplicate state.
    retains_structured_app_rows: bool,
    /// This binding retains only result membership. Its exact content bodies
    /// are loaded from immutable node storage on entry instead of being held
    /// as source-wide version/replacement terminal witnesses.
    storage_backed_result_materialization: bool,
    /// Frozen branch bases are static graph inputs with an exact immutable
    /// version identity. They do not emit a live Stream-B witness when a head
    /// deletion or rejection exposes the inherited member.
    inline_content_branch_keys: BTreeSet<Vec<u8>>,
    /// Evaluator-local proof/output facts. Physical input membership is owned
    /// solely by `supporting`; these facts are never a transport frontier.
    program_fact_weights: BTreeMap<ProgramFactEntry, i64>,
    pub(super) supporting: super::supporting_frontier::SupportingFrontier,
    pub(super) physical_tables: BTreeMap<groove::Intern<String>, crate::ids::GlobalPhysicalTableId>,
    /// Logical tables whose writes can affect this program's local result or
    /// its embedded policy decisions.
    pub(super) targeted_refresh_tables: BTreeSet<String>,
    /// Conservative fallback when policy compilation cannot expose all inputs.
    pub(super) targeted_refresh_uncertain: bool,
    /// Native deletion bodies retained for the frontier's selected-deletion
    /// contributions; this map is payload ownership, not another published set.
    selected_deletion_witnesses: BTreeMap<SupportingRow, VersionRow>,
    versions: WeightedVersionIndex,
    replacements: ReplacementIndex,
}

impl Default for MaintainedSubscriptionView {
    fn default() -> Self {
        Self {
            edge_availability_owner: None,
            read_view: Default::default(),
            witness_table_names: BTreeMap::new(),
            result_weights: RetainedResultMap::default(),
            published_result_members: RetainedResultMembers::default(),
            unreconciled_result_members: None,
            #[cfg(test)]
            result_member_reconcile_visits: 0,
            result_payloads: RetainedResultMap::default(),
            published_result_payloads: RetainedResultMap::default(),
            structured_app_rows: BTreeMap::new(),
            structured_terminal_records: BTreeMap::new(),
            structured_root_keys: BTreeMap::new(),
            structured_root_key_order: Vec::new(),
            #[cfg(test)]
            root_order_insert_comparisons: 0,
            structured_app_row_descriptor: None,
            retains_structured_app_rows: true,
            storage_backed_result_materialization: false,
            inline_content_branch_keys: BTreeSet::new(),
            program_fact_weights: BTreeMap::new(),
            supporting: Default::default(),
            physical_tables: BTreeMap::new(),
            targeted_refresh_tables: BTreeSet::new(),
            targeted_refresh_uncertain: false,
            selected_deletion_witnesses: BTreeMap::new(),
            versions: WeightedVersionIndex::default(),
            replacements: ReplacementIndex::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct MaintainedSubscriptionViewFootprint {
    pub(crate) result_rows: usize,
    pub(crate) result_weights: usize,
    pub(crate) result_payloads: usize,
    pub(crate) structured_app_rows: usize,
    pub(crate) version_identities: usize,
    pub(crate) version_tx_entries: usize,
    pub(crate) replacement_entries: usize,
    pub(crate) result_weights_bytes: usize,
    pub(crate) result_payloads_bytes: usize,
    pub(crate) structured_app_rows_bytes: usize,
    pub(crate) versions_bytes: usize,
    pub(crate) supporting_frontier_bytes: usize,
    pub(crate) replacements_bytes: usize,
    pub(crate) total_heap_bytes: usize,
}

/// Read-only map access keeps every mutation paired with the exact existing
/// referenced-byte model. No allocator sampling or delayed metric refresh.
#[derive(Clone, Debug, PartialEq, Eq)]
struct RetainedResultMap<V> {
    entries: BTreeMap<ResultMemberEntry, V>,
    entry_bytes: usize,
    positive_count: usize,
}

impl<V> Default for RetainedResultMap<V> {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
            entry_bytes: 0,
            positive_count: 0,
        }
    }
}

impl<V> std::ops::Deref for RetainedResultMap<V> {
    type Target = BTreeMap<ResultMemberEntry, V>;
    fn deref(&self) -> &Self::Target {
        &self.entries
    }
}

trait RetainedResultValue {
    fn retained_bytes(&self) -> usize;
    fn positive_count(&self) -> usize {
        0
    }
}

impl RetainedResultValue for i64 {
    fn retained_bytes(&self) -> usize {
        mem::size_of::<Self>()
    }
    fn positive_count(&self) -> usize {
        usize::from(*self > 0)
    }
}

impl RetainedResultValue for ResultMemberPayloadEntry {
    fn retained_bytes(&self) -> usize {
        result_member_payload_entry_bytes(self)
    }
}

impl<V: RetainedResultValue> RetainedResultMap<V> {
    fn insert(&mut self, member: ResultMemberEntry, value: V) -> Option<V> {
        let key_bytes = result_member_entry_bytes(&member);
        let value_bytes = value.retained_bytes();
        let positive = value.positive_count();
        let old = self.entries.insert(member, value);
        if let Some(old) = &old {
            self.entry_bytes -= old.retained_bytes();
            self.positive_count -= old.positive_count();
        } else {
            // Equal keys have equal modeled sizes: the model uses lengths,
            // not vector capacities or allocation addresses.
            self.entry_bytes += key_bytes;
        }
        self.entry_bytes += value_bytes;
        self.positive_count += positive;
        old
    }

    fn remove(&mut self, member: &ResultMemberEntry) -> Option<V> {
        let (key, value) = self.entries.remove_entry(member)?;
        self.entry_bytes -= result_member_entry_bytes(&key) + value.retained_bytes();
        self.positive_count -= value.positive_count();
        Some(value)
    }

    fn footprint_bytes(&self) -> usize {
        btree_map_bytes(self.entries.len()) + self.entry_bytes
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct RetainedResultMembers {
    entries: BTreeSet<ResultMemberEntry>,
    entry_bytes: usize,
}

impl std::ops::Deref for RetainedResultMembers {
    type Target = BTreeSet<ResultMemberEntry>;
    fn deref(&self) -> &Self::Target {
        &self.entries
    }
}

impl From<BTreeSet<ResultMemberEntry>> for RetainedResultMembers {
    fn from(entries: BTreeSet<ResultMemberEntry>) -> Self {
        let entry_bytes = entries.iter().map(result_member_entry_bytes).sum();
        Self {
            entries,
            entry_bytes,
        }
    }
}

impl IntoIterator for RetainedResultMembers {
    type Item = ResultMemberEntry;
    type IntoIter = std::collections::btree_set::IntoIter<ResultMemberEntry>;
    fn into_iter(self) -> Self::IntoIter {
        self.entries.into_iter()
    }
}

impl RetainedResultMembers {
    fn insert(&mut self, member: ResultMemberEntry) -> bool {
        let bytes = result_member_entry_bytes(&member);
        let inserted = self.entries.insert(member);
        if inserted {
            self.entry_bytes += bytes;
        }
        inserted
    }

    fn remove(&mut self, member: &ResultMemberEntry) -> bool {
        let removed = self.entries.remove(member);
        if removed {
            self.entry_bytes -= result_member_entry_bytes(member);
        }
        removed
    }
}

#[derive(Clone, Debug, Default)]
struct WeightedVersionIndex {
    // The sort key contains the complete version identity. Transaction and
    // row UUID are derived from that same immutable record, so a second global
    // identity map and singleton identity sets add no distinguishing power.
    by_tx: BTreeMap<TxId, BTreeMap<VersionSortKey, WeightedVersion>>,
    entry_count: usize,
    entry_bytes: usize,
}

#[derive(Clone, Debug)]
struct WeightedVersion {
    payload: Arc<VersionPayload>,
    weight: i64,
}

/// Immutable witness data shared only by retained role consumers, not by all
/// transient OwnedRecords. A paired terminal prepares this once for both indexes.
#[derive(Clone, Debug)]
struct VersionPayload {
    row: VersionRow,
    tx_id: TxId,
    sort_key: VersionSortKey,
}

impl std::ops::Deref for WeightedVersion {
    type Target = VersionPayload;

    fn deref(&self) -> &Self::Target {
        &self.payload
    }
}

impl VersionPayload {
    fn prepare(
        row: VersionRow,
        identity: &VersionIdentity,
        node_aliases: &BTreeMap<NodeUuid, NodeAlias>,
    ) -> Result<Arc<Self>, super::Error> {
        let tx_id = version_tx_id_from_aliases(&row, node_aliases).ok_or(
            super::Error::InvalidStoredValue("history tx node alias must exist"),
        )?;
        let sort_key = VersionSortKey::for_row(&row, identity);
        Ok(Arc::new(Self {
            row,
            tx_id,
            sort_key,
        }))
    }
}

#[derive(Clone, Debug, Default)]
struct ReplacementIndex {
    content_by_key: BTreeMap<ReplacementKey, BTreeMap<VersionIdentity, WeightedVersion>>,
    deletion_by_key: BTreeMap<ReplacementKey, BTreeMap<VersionIdentity, WeightedVersion>>,
    entry_count: usize,
    entry_bytes: usize,
    key_bytes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct VersionIdentity {
    table: groove::Intern<String>,
    layer: VersionLayer,
    raw_record: Arc<[u8]>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct VersionSortKey {
    table: groove::Intern<String>,
    row_uuid: RowUuid,
    layer: VersionLayer,
    raw_record: Arc<[u8]>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ReplacementKey {
    table: groove::Intern<String>,
    row_uuid: RowUuid,
    layer: VersionLayer,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ResultTransitions {
    /// Membership was reconciled from a newly advanced remote authority view,
    /// rather than emitted by this local Groove subscription.
    pub(crate) authoritative_membership_changed: bool,
    /// Exact members admitted by the newly advanced authority view. These
    /// may replace a stale internal member with the same public occurrence;
    /// unrelated local adds coalesced into the same batch must not.
    pub(crate) authoritative_member_adds: BTreeSet<ResultMemberEntry>,
    pub(crate) adds: Vec<ResultMemberEntry>,
    pub(crate) removes: Vec<ResultMemberEntry>,
    pub(crate) result_payload_adds: Vec<(ResultMemberEntry, ResultMemberPayloadEntry)>,
    pub(crate) result_payload_removes: Vec<ResultMemberEntry>,
    pub(crate) program_fact_adds: Vec<ProgramFactEntry>,
    pub(crate) program_fact_removes: Vec<ProgramFactEntry>,
    /// Ordered source-closure presence transitions observed while evaluating
    /// one terminal. `apply_multisink_deltas` uses this private stream to
    /// retain the real terminal order while coalescing one complete drain.
    pub(crate) supporting_changed: bool,
    /// Groove terminal patches are local binding output. They never enter a
    /// peer `ViewUpdate`, whose contract is the covered input closure only.
    pub(crate) terminal_operations: Vec<TerminalOperation>,
    pub(crate) allow_storage_witness_fallback: bool,
    pub(crate) observed_result_delta_batches: usize,
    /// A deletion-register witness changed while its anti-joined result
    /// terminal may be silent. When the public result terminals are silent,
    /// the caller must replace this tick with an authoritative membership
    /// reconciliation. A complete public result delta remains authoritative
    /// and must not be discarded merely because its witness changed too.
    pub(crate) requires_authoritative_membership_reconcile: bool,
}

#[derive(Clone, Debug)]
pub(crate) enum DecodedMaintainedEvent {
    ResultCurrent {
        member: ResultMemberEntry,
        payload: ResultMemberPayloadEntry,
    },
    AggregateResult {
        member: ResultMemberEntry,
        payload: ResultMemberPayloadEntry,
        synthetic: super::query_engine::SyntheticResultMembershipSchema,
        value_fields: Vec<String>,
    },
    VersionContent {
        source: ProgramSourceId,
        row: VersionRow,
    },
    VersionDeletion {
        source: ProgramSourceId,
        row: VersionRow,
    },
    ReplacementContent {
        source: ProgramSourceId,
        row: VersionRow,
    },
    ReplacementDeletion {
        source: ProgramSourceId,
        row: VersionRow,
    },
    /// Identical payload graph with two independent role consumers.
    SharedVersion {
        source: ProgramSourceId,
        row: VersionRow,
    },
    ProgramSourceCoverage(crate::protocol::ProgramSourceCoverageEntry),
    RelationEdge(RelationEdgeEntry),
    StructuredAppRow {
        root: RowUuid,
        record: OwnedRecord,
    },
}

#[derive(Clone, Debug, Default)]
pub(crate) struct MaintainedTerminalSchemas {
    sinks: BTreeMap<String, MaintainedTerminalKind>,
}

#[derive(Clone, Debug)]
enum MaintainedTerminalKind {
    ResultCurrent(ResultMembershipSchema),
    AggregateResult(AggregateResultSchema),
    VersionContent(VersionWitnessSchema),
    VersionDeletion(VersionWitnessSchema),
    ReplacementContent(VersionWitnessSchema),
    ReplacementDeletion(VersionWitnessSchema),
    SharedContent(VersionWitnessSchema),
    SharedDeletion(VersionWitnessSchema),
    ProgramSourceCoverage(super::query_engine::ProgramSourceCoverageSchema),
    RelationEdge(RelationEdgeSchema),
    /// A compiler-lowered public root collector. Its initial state and later
    /// positional edits are both owned by Groove's terminal reducer.
    RootCollectorAppRows {
        schema: AppRowSchema,
        layout: TerminalRootLayout,
    },
    /// Ordinary relational app-row tuples. These have no terminal position
    /// stream and remain on the membership/materialization bridge.
    DirectAppRows(AppRowSchema),
    /// Compiler-owned aggregate app rows. The same local terminal drives its
    /// reset state and subsequent replacements; derived aggregate results are
    /// never accepted from an authority snapshot.
    AggregateAppRows(AppRowSchema),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum EventIdentity {
    Result(Rc<ResultMemberEntry>),
    Version(ProgramSourceId, VersionIdentity),
    Replacement(ProgramSourceId, ReplacementKey, VersionIdentity),
    SharedVersion(ProgramSourceId, VersionIdentity),
    ProgramFact(Rc<ProgramFactEntry>),
    StructuredAppRow(RowUuid, Vec<u8>),
}

#[derive(Clone, Debug)]
struct AggregateNetEvent {
    member: Rc<ResultMemberEntry>,
    payload: ResultMemberPayloadEntry,
    synthetic: super::query_engine::SyntheticResultMembershipSchema,
    value_fields: Vec<String>,
}

// These temporary, thread-local entries share large identities with their
// tree keys. Ordinary version witnesses do not reserve the large inline space
// of result/proof alternatives; consuming the key releases its shared owner.
#[derive(Clone, Debug)]
enum NetEvent {
    Result(Box<(Rc<ResultMemberEntry>, ResultMemberPayloadEntry)>),
    AggregateResult(Box<AggregateNetEvent>),
    Version(ProgramSourceId, VersionIdentity, VersionRow),
    Replacement(ProgramSourceId, ReplacementKey, VersionIdentity, VersionRow),
    SharedVersion(ProgramSourceId, VersionIdentity, VersionRow),
    ProgramFact(Rc<ProgramFactEntry>),
    StructuredAppRow(RowUuid, OwnedRecord),
}

/// Root collector keys begin with the root source occurrence. For the common
/// single-root shape that occurrence is one ordered UUID scalar. Retained
/// snapshots predate an in-memory key map, so this lets a later remove/move
/// address that already-materialized root without reopening the query.
fn terminal_root_uuid_from_key(key: &[u8]) -> Option<RowUuid> {
    (key.first() == Some(&10))
        .then(|| uuid::Uuid::from_slice(key.get(1..17)?).ok())
        .flatten()
        .map(RowUuid)
}

impl MaintainedSubscriptionView {
    pub(crate) fn set_read_view(&mut self, read_view: crate::protocol::ReadViewKey) {
        self.read_view = read_view;
    }

    pub(crate) fn set_witness_table_names(
        &mut self,
        names: BTreeMap<(String, SchemaVersionAlias), String>,
    ) {
        self.witness_table_names = names;
    }

    fn supporting_row_for_version(
        &self,
        source: ProgramSourceId,
        row: &VersionRow,
        node_aliases: &BTreeMap<NodeUuid, NodeAlias>,
    ) -> Result<SupportingRow, super::Error> {
        let physical_table =
            *self
                .physical_tables
                .get(&source.table)
                .ok_or(super::Error::InvalidStoredValue(
                    "support terminal has no physical table identity",
                ))?;
        let tx = version_tx_id_from_aliases(row, node_aliases).ok_or(
            super::Error::InvalidStoredValue("support input tx node alias must exist"),
        )?;
        let branch = row.branch_key().canonical_bytes();
        let version_table = self
            .witness_table_names
            .get(&(row.table().to_owned(), row.schema_version_alias()))
            .map(String::as_str)
            .unwrap_or(row.table())
            .to_owned()
            .into();
        Ok(SupportingRow {
            physical_table,
            version_table,
            row: row.row_uuid(),
            version: RowVersionRefEntry {
                tx,
                schema_version: None,
                layer: if row.layer() == VersionLayer::Content {
                    ResultRowLayer::Content
                } else {
                    ResultRowLayer::Deletion
                },
                batch: Some(tx),
                branch_or_prefix: (!branch.is_empty()).then_some(branch),
                row_digest: None,
            },
        })
    }
    pub(crate) fn uses_storage_backed_result_materialization(&self) -> bool {
        self.storage_backed_result_materialization
    }

    pub(crate) fn enable_storage_backed_result_materialization(&mut self) {
        self.storage_backed_result_materialization = true;
        self.unreconciled_result_members = None;
    }

    pub(crate) fn enable_inline_content_branch_key(&mut self, branch_key: &BranchKey) {
        self.inline_content_branch_keys
            .insert(branch_key.canonical_bytes());
    }

    pub(crate) fn terminal_schemas_for_program(
        program: &QueryProgram,
    ) -> MaintainedTerminalSchemas {
        MaintainedTerminalSchemas::for_program(program)
    }

    pub(crate) fn apply_typed_deltas(
        &mut self,
        sink: &str,
        deltas: &RecordDeltas,
        schemas: &MaintainedTerminalSchemas,
        tables: &TableSchemas,
        node_aliases: &BTreeMap<NodeUuid, NodeAlias>,
    ) -> Result<ResultTransitions, super::Error> {
        let kind = schemas.get(sink)?;
        let observed_result_delta_batch = !deltas.is_empty() && kind.is_result_terminal();
        // Deletion witnesses are part of the membership proof: a current-row
        // anti-join can become empty solely because its deletion-register
        // input changed. Groove reports that change through the witness
        // terminal even when the result-current terminal is silent.
        let requires_authoritative_membership_reconcile =
            !deltas.is_empty() && kind.requires_authoritative_membership_reconcile();
        let mut decode_plan_cache = VersionDecodePlanCache::new();
        let mut payload_plans = std::collections::HashMap::new();
        let read_view = self.read_view;
        let decoded = deltas.iter().map(|(record, weight)| {
            decode_typed_terminal_record(
                record,
                kind,
                tables,
                node_aliases,
                &mut decode_plan_cache,
                &mut payload_plans,
                read_view,
            )
            .map(|event| (event, weight))
        });
        let mut transitions = self.apply_decoded_delta_results(decoded, node_aliases)?;
        if observed_result_delta_batch {
            transitions.observed_result_delta_batches += 1;
        }
        transitions.requires_authoritative_membership_reconcile =
            requires_authoritative_membership_reconcile;
        Ok(transitions)
    }

    #[cfg_attr(
        feature = "cold-settle-attribution",
        tracing::instrument(skip_all, name = "cold.phase.decode_query_outputs")
    )]
    pub(crate) fn apply_multisink_deltas(
        &mut self,
        deltas: MultisinkDeltas,
        schemas: &MaintainedTerminalSchemas,
        tables: &TableSchemas,
        node_aliases: &BTreeMap<NodeUuid, NodeAlias>,
    ) -> Result<ResultTransitions, super::Error> {
        let mut transitions = ResultTransitions::default();
        // A single IVM drain may touch the same source fact through more than
        // one terminal. Record only the first pre-state and final post-state
        // for each touched peer fact, then publish the net closure change.
        // This is deliberately proportional to changed facts: cloning the
        // whole active closure here would turn every incremental tick into a
        // snapshot-sized operation.
        for (sink, terminal) in deltas.terminal_sinks {
            if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some()
                && !terminal.operations.is_empty()
            {
                eprintln!(
                    "JAZZ_COVERED_INPUT_TRACE stage=terminal_operations sink={sink} kind={:?} operations={}",
                    schemas.get(&sink)?,
                    terminal.operations.len(),
                );
            }
            if let MaintainedTerminalKind::RootCollectorAppRows { layout, .. } =
                schemas.get(&sink)?
            {
                let operations = terminal
                    .operations
                    .into_iter()
                    .map(|operation| rebind_terminal_operation_to_layout(operation, layout))
                    .collect::<Result<Vec<_>, _>>()?;
                // A root removal can share its batch with descendants which
                // are being retracted beneath it. Fold every root first, then
                // skip only those now-unreachable descendants. This mirrors
                // the facade reducer and keeps malformed descendants for an
                // otherwise retained root fail-closed.
                let (root_operations, nested_operations): (Vec<_>, Vec<_>) = operations
                    .into_iter()
                    .partition(|operation| operation.path.is_empty());
                let inserted_roots = root_operations
                    .iter()
                    .filter_map(|operation| match operation.edit {
                        TerminalEdit::Insert { .. } => Some((operation.root_key.clone(), true)),
                        TerminalEdit::Remove { .. } => Some((operation.root_key.clone(), false)),
                        _ => None,
                    })
                    .collect::<BTreeMap<_, _>>()
                    .into_iter()
                    .filter_map(|(key, present)| present.then_some(key))
                    .collect::<BTreeSet<_>>();
                let removed_roots = root_operations
                    .iter()
                    .filter_map(|operation| {
                        matches!(operation.edit, TerminalEdit::Remove { .. })
                            .then_some(operation.root_key.clone())
                    })
                    .collect::<BTreeSet<_>>();
                // A scalar replacement can arrive as -old/+new in one drain.
                // Its independently maintained descendants were not removed.
                let mut replaced_records = removed_roots
                    .intersection(&inserted_roots)
                    .filter_map(|key| {
                        self.structured_terminal_records
                            .remove(key)
                            .map(|record| (key.clone(), record))
                    })
                    .collect::<BTreeMap<_, _>>();
                for operation in root_operations.into_iter().chain(nested_operations) {
                    if !operation.path.is_empty()
                        && removed_roots.contains(operation.root_key.as_slice())
                        && !inserted_roots.contains(operation.root_key.as_slice())
                    {
                        continue;
                    }
                    self.apply_structured_terminal_operation(&operation)?;
                    if operation.path.is_empty()
                        && let TerminalEdit::Insert { value, .. } = &operation.edit
                        && let Some(mut record) = replaced_records.remove(&operation.root_key)
                    {
                        record
                            .update_record(OwnedRecord::new(
                                value.clone(),
                                operation.root_descriptor,
                            ))
                            .map_err(|_| {
                                super::Error::InvalidStoredValue(
                                    "invalid replacement collector terminal record",
                                )
                            })?;
                        self.structured_app_rows.remove(&operation.root_key);
                        self.structured_terminal_records
                            .insert(operation.root_key.clone(), record);
                    }
                    transitions.terminal_operations.push(operation);
                }
            }
        }
        for (sink, deltas) in deltas.sinks {
            if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() && !deltas.is_empty() {
                eprintln!(
                    "JAZZ_COVERED_INPUT_TRACE stage=terminal_sink sink={sink} kind={:?} records={}",
                    schemas.get(&sink)?,
                    deltas.deltas.len(),
                );
            }
            // The root collector's positional terminal is the sole owner of
            // retained app-row state. Its ordinary sink delta is the same
            // fact stream without an occurrence key; applying it here would
            // either duplicate an occurrence or collapse siblings sharing a
            // public root UUID.
            if matches!(
                schemas.get(&sink)?,
                MaintainedTerminalKind::RootCollectorAppRows { .. }
            ) {
                continue;
            }
            let delta_transitions =
                self.apply_typed_deltas(&sink, &deltas, schemas, tables, node_aliases)?;
            transitions.supporting_changed |= delta_transitions.supporting_changed;
            transitions.adds.extend(delta_transitions.adds);
            transitions.removes.extend(delta_transitions.removes);
            transitions
                .program_fact_adds
                .extend(delta_transitions.program_fact_adds);
            transitions
                .program_fact_removes
                .extend(delta_transitions.program_fact_removes);
            transitions
                .result_payload_adds
                .extend(delta_transitions.result_payload_adds);
            transitions
                .result_payload_removes
                .extend(delta_transitions.result_payload_removes);
            transitions.observed_result_delta_batches +=
                delta_transitions.observed_result_delta_batches;
            transitions.requires_authoritative_membership_reconcile |=
                delta_transitions.requires_authoritative_membership_reconcile;
        }
        self.finalize_multisink_transitions(&mut transitions, node_aliases);
        if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some()
            && (!transitions.adds.is_empty()
                || !transitions.program_fact_adds.is_empty()
                || !transitions.program_fact_removes.is_empty())
        {
            eprintln!(
                "JAZZ_COVERED_INPUT_TRACE stage=maintained_transition adds={} removes={} fact_adds={} fact_removes={} terminal_ops={}",
                transitions.adds.len(),
                transitions.removes.len(),
                transitions.program_fact_adds.len(),
                transitions.program_fact_removes.len(),
                transitions.terminal_operations.len(),
            );
        }
        Ok(transitions)
    }

    fn finalize_multisink_transitions(
        &mut self,
        transitions: &mut ResultTransitions,
        node_aliases: &BTreeMap<NodeUuid, NodeAlias>,
    ) {
        // A multisink delta need not contain every terminal that participates
        // in one maintained result. In particular, a current-membership row
        // can arrive before its content witness while a cold source finishes
        // on a later runtime turn. Keep that raw membership internally, but
        // do not publish Stream A until its Stream B bundle witness is
        // present. A later content-only delta revisits all pending members and
        // promotes the now-complete one without requiring another membership
        // edge.
        // Synthetic/path payloads are self-contained. Row-digest payloads,
        // however, are the Stream-A half of a real-row membership and must
        // cross the same witness boundary as that membership.
        transitions
            .result_payload_adds
            .retain(|(member, _)| member.as_row().is_none());
        transitions
            .result_payload_removes
            .retain(|member| member.as_row().is_none());
        let (adds, removes, payload_adds, payload_removes) =
            self.reconcile_publishable_result_members(node_aliases);
        transitions.adds = adds;
        transitions.removes = removes;
        transitions.result_payload_adds.extend(payload_adds);
        transitions.result_payload_removes.extend(payload_removes);
    }

    #[cfg(test)]
    fn apply_decoded_deltas(
        &mut self,
        rows: impl IntoIterator<Item = (DecodedMaintainedEvent, i64)>,
        node_aliases: &BTreeMap<NodeUuid, NodeAlias>,
    ) -> Result<ResultTransitions, super::Error> {
        self.apply_decoded_delta_results(rows.into_iter().map(Ok), node_aliases)
    }

    fn apply_decoded_delta_results(
        &mut self,
        rows: impl IntoIterator<Item = Result<(DecodedMaintainedEvent, i64), super::Error>>,
        node_aliases: &BTreeMap<NodeUuid, NodeAlias>,
    ) -> Result<ResultTransitions, super::Error> {
        // Decode into the net-change accumulator directly. No retained state
        // changes until the complete input has decoded successfully.
        #[cfg(feature = "cold-settle-attribution")]
        let net_span = tracing::trace_span!("cold.phase.terminal_net").entered();
        let mut net = BTreeMap::<EventIdentity, (NetEvent, i64)>::new();
        for row in rows {
            let (event, weight) = row?;
            let net_event = match event {
                DecodedMaintainedEvent::ResultCurrent { member, payload } => {
                    NetEvent::Result(Box::new((Rc::new(member), payload)))
                }
                DecodedMaintainedEvent::AggregateResult {
                    member,
                    payload,
                    synthetic,
                    value_fields,
                } => NetEvent::AggregateResult(Box::new(AggregateNetEvent {
                    member: Rc::new(member),
                    payload,
                    synthetic,
                    value_fields,
                })),
                DecodedMaintainedEvent::VersionContent { source, row }
                | DecodedMaintainedEvent::VersionDeletion { source, row } => {
                    let identity = VersionIdentity::for_row(&row);
                    NetEvent::Version(source, identity, row)
                }
                DecodedMaintainedEvent::ReplacementContent { source, row } => {
                    let identity = VersionIdentity::for_row(&row);
                    let key = ReplacementKey::for_row(&row, VersionLayer::Content);
                    NetEvent::Replacement(source, key, identity, row)
                }
                DecodedMaintainedEvent::ReplacementDeletion { source, row } => {
                    let identity = VersionIdentity::for_row(&row);
                    let key = ReplacementKey::for_row(&row, VersionLayer::Deletion);
                    NetEvent::Replacement(source, key, identity, row)
                }
                DecodedMaintainedEvent::SharedVersion { source, row } => {
                    let identity = VersionIdentity::for_row(&row);
                    NetEvent::SharedVersion(source, identity, row)
                }
                DecodedMaintainedEvent::ProgramSourceCoverage(coverage) => NetEvent::ProgramFact(
                    Rc::new(ProgramFactEntry::ProgramSourceCoverage(coverage)),
                ),
                DecodedMaintainedEvent::RelationEdge(edge) => {
                    NetEvent::ProgramFact(Rc::new(ProgramFactEntry::RelationEdge(edge)))
                }
                DecodedMaintainedEvent::StructuredAppRow { root, record } => {
                    NetEvent::StructuredAppRow(root, record)
                }
            };
            let identity = net_event.identity();
            net.entry(identity)
                .and_modify(|(_, net_weight)| *net_weight += weight)
                .or_insert((net_event, weight));
        }

        #[cfg(feature = "cold-settle-attribution")]
        drop(net_span);
        #[cfg(feature = "cold-settle-attribution")]
        let _apply_span = tracing::trace_span!("cold.phase.terminal_apply").entered();
        let mut transitions = ResultTransitions::default();
        for (identity, (event, weight)) in net {
            drop(identity);
            if weight == 0 {
                continue;
            }
            if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
                eprintln!(
                    "JAZZ_COVERED_INPUT_TRACE stage=apply_decoded_event event={event:?} weight={weight}"
                );
            }
            match event {
                NetEvent::Result(result) => {
                    let (member, payload) = *result;
                    let entry = Rc::unwrap_or_clone(member);
                    self.apply_result_delta(entry, payload, weight, &mut transitions);
                }
                NetEvent::AggregateResult(result) => {
                    let AggregateNetEvent {
                        member,
                        payload,
                        synthetic,
                        value_fields,
                    } = *result;
                    let member = Rc::unwrap_or_clone(member);
                    self.apply_aggregate_result_delta(
                        member,
                        payload,
                        &synthetic,
                        &value_fields,
                        weight,
                        &mut transitions,
                    )?;
                }
                NetEvent::Version(source, identity, row) => {
                    let covered_input =
                        self.supporting_row_for_version(source, &row, node_aliases)?;
                    let payload = VersionPayload::prepare(row, &identity, node_aliases)?;
                    self.versions.apply_delta(payload, weight);
                    self.supporting.apply(0, covered_input, weight);
                    transitions.supporting_changed = true;
                }
                NetEvent::Replacement(source, key, identity, row) => {
                    let covered_input =
                        self.supporting_row_for_version(source, &row, node_aliases)?;
                    let payload = VersionPayload::prepare(row, &identity, node_aliases)?;
                    self.replacements
                        .apply_delta(key, identity, payload, weight);
                    self.supporting.apply(1, covered_input, weight);
                    transitions.supporting_changed = true;
                }
                NetEvent::SharedVersion(source, identity, row) => {
                    let covered_input =
                        self.supporting_row_for_version(source, &row, node_aliases)?;
                    let key = ReplacementKey::for_row(&row, identity.layer);
                    let payload = VersionPayload::prepare(row, &identity, node_aliases)?;
                    self.versions.apply_delta(Arc::clone(&payload), weight);
                    self.replacements
                        .apply_delta(key, identity, payload, weight);
                    self.supporting.apply(0, covered_input.clone(), weight);
                    self.supporting.apply(1, covered_input, weight);
                    transitions.supporting_changed = true;
                }
                NetEvent::ProgramFact(fact) => {
                    let fact = Rc::unwrap_or_clone(fact);
                    if matches!(fact, ProgramFactEntry::ProgramSourceCoverage(_)) {
                        continue;
                    }
                    if let Some(is_present) = self.apply_program_fact_delta(fact.clone(), weight) {
                        if is_present {
                            transitions.program_fact_adds.push(fact);
                        } else {
                            transitions.program_fact_removes.push(fact);
                        }
                    }
                }
                NetEvent::StructuredAppRow(root, record) => {
                    if self.retains_structured_app_rows {
                        // Direct app rows have no Groove positional terminal,
                        // so their public root remains their only identity.
                        // Root-collector rows never reach this branch; they
                        // retain the exact opaque terminal key above.
                        let terminal_key = root.0.as_bytes().to_vec();
                        self.structured_root_keys.insert(terminal_key.clone(), root);
                        self.apply_structured_app_row_delta(terminal_key.clone(), record, weight);
                        if !self.structured_root_key_order.contains(&terminal_key) {
                            self.structured_root_key_order.push(terminal_key);
                        }
                    }
                }
            }
        }
        Ok(transitions)
    }

    pub(crate) fn versions_by_tx(&self, tx_id: TxId) -> Vec<VersionRow> {
        let mut versions = self.versions.versions_by_tx(tx_id);
        for (fact, version) in &self.selected_deletion_witnesses {
            if (fact.version.tx == tx_id) && !versions.contains(version) {
                versions.push(version.clone());
            }
        }
        versions
    }

    pub(crate) fn acknowledged_supporting_rows(&self) -> impl Iterator<Item = &SupportingRow> {
        self.supporting.acknowledged_rows()
    }

    pub(crate) fn supporting_rows(&self) -> impl Iterator<Item = &SupportingRow> {
        self.supporting.rows()
    }

    pub(crate) fn unpublished_supporting_delta(
        &self,
    ) -> Option<(Vec<SupportingRow>, Vec<SupportingRow>)> {
        self.supporting.delta()
    }

    pub(crate) fn forget_peer_source_closure_baseline(&mut self) {
        self.supporting.forget_predecessor();
    }
    pub(crate) fn acknowledge_peer_source_closure(&mut self) {
        self.supporting.acknowledge();
    }

    /// Selected-scope tombstones are another contributor to the same physical
    /// frontier. Their native bodies are retained for relaying, not membership.
    pub(crate) fn replace_selected_deletion_witnesses(
        &mut self,
        witnesses: BTreeMap<SupportingRow, VersionRow>,
    ) -> bool {
        let mut changed = false;
        for row in self
            .selected_deletion_witnesses
            .keys()
            .filter(|row| !witnesses.contains_key(*row))
        {
            self.supporting.apply(3, row.clone(), -1);
            changed = true;
        }
        for row in witnesses
            .keys()
            .filter(|row| !self.selected_deletion_witnesses.contains_key(*row))
        {
            self.supporting.apply(3, row.clone(), 1);
            changed = true;
        }
        self.selected_deletion_witnesses = witnesses;
        changed
    }

    /// Local proof/output facts are independent of the physical sync frontier.
    fn apply_program_fact_delta(&mut self, fact: ProgramFactEntry, weight: i64) -> Option<bool> {
        let retained = self.program_fact_weights.entry(fact.clone()).or_default();
        let before = *retained > 0;
        *retained += weight;
        let after = *retained > 0;
        if *retained == 0 {
            self.program_fact_weights.remove(&fact);
        }
        (before != after).then_some(after)
    }

    pub(crate) fn replacement_for(
        &self,
        table: &str,
        row_uuid: RowUuid,
    ) -> (Option<VersionRow>, Option<VersionRow>) {
        self.replacements.replacement_for(table, row_uuid)
    }

    pub(crate) fn footprint(&self) -> MaintainedSubscriptionViewFootprint {
        let result_weights_bytes = self.result_weights.footprint_bytes()
            + self
                .unreconciled_result_members
                .as_ref()
                .map_or(0, |members| {
                    btree_set_bytes(members.len()) + members.entry_bytes
                })
            + btree_map_bytes(self.published_result_members.len())
            + self.published_result_members.entry_bytes;
        let result_payloads_bytes = self.result_payloads.footprint_bytes()
            + self.published_result_payloads.footprint_bytes();
        #[cfg(test)]
        self.assert_incremental_footprint_matches_full_scan();
        let supporting_frontier_bytes = self.supporting.retained_bytes();
        let versions_bytes = self.versions.footprint_bytes()
            + btree_map_bytes(self.selected_deletion_witnesses.len())
            + self
                .selected_deletion_witnesses
                .values()
                .map(version_row_bytes)
                .sum::<usize>();
        let replacements_bytes = self.replacements.footprint_bytes();
        let witness_table_names_bytes = btree_map_bytes(self.witness_table_names.len())
            + self
                .witness_table_names
                .iter()
                .map(|((logical, _), authored)| {
                    logical.len()
                        + authored.len()
                        + 2 * mem::size_of::<String>()
                        + mem::size_of::<SchemaVersionAlias>()
                })
                .sum::<usize>();
        let structured_app_rows_bytes = self
            .structured_app_rows
            .values()
            .map(|records| {
                records
                    .keys()
                    .map(|record| record.len() + mem::size_of::<i64>())
                    .sum::<usize>()
                    + btree_map_bytes(records.len())
            })
            .sum::<usize>()
            + btree_map_bytes(self.structured_app_rows.len())
            + btree_map_bytes(self.structured_terminal_records.len())
            + self
                .structured_terminal_records
                .values()
                .map(|record| record.retained_bytes())
                .sum::<usize>();
        MaintainedSubscriptionViewFootprint {
            result_rows: self.result_weights.positive_count,
            result_weights: self.result_weights.len(),
            result_payloads: self.result_payloads.len(),
            structured_app_rows: self
                .structured_app_rows
                .values()
                .map(|records| records.values().filter(|weight| **weight > 0).count())
                .sum::<usize>()
                + self.structured_terminal_records.len(),
            version_identities: self.versions.entry_count + self.selected_deletion_witnesses.len(),
            version_tx_entries: self.versions.entry_count,
            replacement_entries: self.replacements.entry_count(),
            result_weights_bytes,
            result_payloads_bytes,
            structured_app_rows_bytes,
            versions_bytes,
            supporting_frontier_bytes,
            replacements_bytes,
            total_heap_bytes: result_weights_bytes
                + result_payloads_bytes
                + structured_app_rows_bytes
                + versions_bytes
                + supporting_frontier_bytes
                + replacements_bytes
                + witness_table_names_bytes,
        }
    }

    // Accounting is not observable from row/query APIs. Keep the old full-scan
    // model as a test-only oracle at every exercised footprint refresh.
    #[cfg(test)]
    fn assert_incremental_footprint_matches_full_scan(&self) {
        fn check_map<V: RetainedResultValue>(map: &RetainedResultMap<V>) {
            assert_eq!(
                map.entry_bytes,
                map.iter()
                    .map(|(key, value)| { result_member_entry_bytes(key) + value.retained_bytes() })
                    .sum::<usize>()
            );
            assert_eq!(
                map.positive_count,
                map.values()
                    .map(RetainedResultValue::positive_count)
                    .sum::<usize>()
            );
        }
        check_map(&self.result_weights);
        check_map(&self.result_payloads);
        check_map(&self.published_result_payloads);
        for members in std::iter::once(&self.published_result_members)
            .chain(self.unreconciled_result_members.as_ref())
        {
            assert_eq!(
                members.entry_bytes,
                members.iter().map(result_member_entry_bytes).sum::<usize>()
            );
        }
        self.replacements.assert_footprint_matches_full_scan();
    }

    /// Current positive result memberships, including unchanged members during
    /// a non-reset rehydrate. Tuple-source admissions are a closure over this
    /// set, not merely over the membership delta.
    pub(crate) fn active_result_members(&self) -> Vec<ResultMemberEntry> {
        self.result_weights
            .iter()
            .filter(|(_, weight)| **weight > 0)
            .map(|(member, _)| member.clone())
            .collect()
    }

    /// Current memberships that have crossed the result/content witness
    /// boundary and are therefore safe to expose to a subscription consumer.
    /// Cold runtime recovery uses this complete set to reconcile a retained
    /// downstream membership without reopening the just-hydrated view.
    pub(crate) fn published_result_members(&self) -> &BTreeSet<ResultMemberEntry> {
        &self.published_result_members
    }

    /// Returns the collector's current recursive row for one opaque terminal
    /// key.
    fn structured_app_row_for_terminal_key(
        &self,
        terminal_key: &[u8],
    ) -> Result<Option<OwnedRecord>, super::Error> {
        if let Some(record) = self.structured_terminal_records.get(terminal_key) {
            return record.record().map(Some).map_err(|_| {
                super::Error::InvalidStoredValue("cannot encode retained terminal tree")
            });
        }
        let Some(descriptor) = self.structured_app_row_descriptor else {
            return Ok(None);
        };
        let Some(records) = self.structured_app_rows.get(terminal_key) else {
            return Ok(None);
        };
        Ok(records
            .iter()
            .filter(|(_, weight)| **weight > 0)
            .map(|(raw, _)| OwnedRecord::new(raw.clone(), descriptor))
            .next())
    }

    /// Returns the collector row for a public root only when it has one
    /// occurrence. Callers that materialize a flat relation must use the
    /// opaque-key accessor below: a root UUID cannot select among siblings.
    #[cfg(test)]
    pub(crate) fn structured_app_row(&self, root: RowUuid) -> Option<OwnedRecord> {
        let mut rows = self
            .structured_root_key_order
            .iter()
            .filter(|key| self.structured_root_keys.get(*key) == Some(&root))
            .filter_map(|key| {
                self.structured_app_row_for_terminal_key(key)
                    .expect("valid test terminal")
            });
        let row = rows.next()?;
        rows.next().is_none().then_some(row)
    }

    #[cfg(test)]
    pub(crate) fn structured_app_rows(&self) -> Vec<(RowUuid, OwnedRecord)> {
        self.structured_root_key_order
            .iter()
            .filter_map(|key| {
                self.structured_root_keys.get(key).and_then(|root| {
                    self.structured_app_row_for_terminal_key(key)
                        .expect("valid test terminal")
                        .map(|record| (*root, record))
                })
            })
            .collect()
    }

    /// Return one retained collector row for every opaque terminal key.
    ///
    /// A flat relation may legitimately produce multiple occurrences of the
    /// same root row.  The public row record does not encode its joined
    /// occurrence, so its `RowUuid` is insufficient as a snapshot key; the
    /// collector's opaque key is the only exact association.
    pub(crate) fn structured_app_rows_by_terminal_key(
        &self,
    ) -> Result<Vec<(Vec<u8>, OwnedRecord)>, super::Error> {
        let mut rows = Vec::new();
        for key in &self.structured_root_key_order {
            if self.structured_root_keys.contains_key(key)
                && let Some(record) = self.structured_app_row_for_terminal_key(key)?
            {
                rows.push((key.clone(), record));
            }
        }
        Ok(rows)
    }

    pub(crate) fn decoded_terminal_records(
        &self,
    ) -> &BTreeMap<Vec<u8>, crate::db::terminal_record::TerminalRecordState> {
        &self.structured_terminal_records
    }

    /// Release the app-row collector after a flat subscription's reset has
    /// been materialized. Only structured array output reads this state after
    /// opening; flat subscriptions publish subsequent rows from membership
    /// and version witnesses.
    pub(crate) fn discard_structured_app_rows(&mut self) {
        self.structured_app_rows.clear();
        self.structured_terminal_records.clear();
        self.structured_root_keys.clear();
        self.structured_root_key_order.clear();
        self.structured_app_row_descriptor = None;
        self.retains_structured_app_rows = false;
    }

    fn apply_structured_app_row_delta(
        &mut self,
        terminal_key: Vec<u8>,
        record: OwnedRecord,
        weight: i64,
    ) {
        self.structured_app_row_descriptor = Some(*record.descriptor());
        let records = self
            .structured_app_rows
            .entry(terminal_key.clone())
            .or_default();
        let new_weight = records.get(record.raw()).copied().unwrap_or(0) + weight;
        if new_weight == 0 {
            records.remove(record.raw());
        } else {
            records.insert(record.into_raw(), new_weight);
        }
        if records.is_empty() {
            self.structured_app_rows.remove(&terminal_key);
        }
    }

    /// Fold root terminal edits into the same retained collector tree used by
    /// an initial/reset snapshot. This is deliberately receiver-local: it
    /// never re-runs the query or reads authority output.
    fn apply_structured_terminal_operation(
        &mut self,
        operation: &TerminalOperation,
    ) -> Result<(), super::Error> {
        // A descendant edit has the root collector's key, but its value is a
        // nested element rather than an app-row record. Resolve its retained
        // root before looking at the edit: attempting to decode that element
        // as a root is both invalid and would create a second snapshot path.
        if !operation.path.is_empty() {
            let _root = self
                .structured_root_keys
                .get(&operation.root_key)
                .copied()
                .or_else(|| terminal_root_uuid_from_key(&operation.root_key))
                .ok_or(super::Error::InvalidStoredValue(
                    "terminal descendant operation addresses an unknown root key",
                ))?;
            let descriptor =
                self.structured_app_row_descriptor
                    .ok_or(super::Error::InvalidStoredValue(
                        "terminal descendant operation arrived before its root collector record",
                    ))?;
            if descriptor != operation.root_descriptor {
                return Err(super::Error::InvalidStoredValue(
                    "terminal descendant descriptor disagrees with retained collector layout",
                ));
            }
            if !self
                .structured_terminal_records
                .contains_key(&operation.root_key)
            {
                let records = self
                    .structured_app_rows
                    .get_mut(&operation.root_key)
                    .ok_or(super::Error::InvalidStoredValue(
                        "terminal descendant operation addressed an absent retained root",
                    ))?;
                let mut candidates = records
                    .iter()
                    .filter(|(_, weight)| **weight > 0)
                    .map(|(raw, weight)| (raw.clone(), *weight));
                let (raw, weight) = candidates.next().ok_or(super::Error::InvalidStoredValue(
                    "terminal descendant operation addressed a non-positive retained root",
                ))?;
                if candidates.next().is_some() {
                    return Err(super::Error::InvalidStoredValue(
                        "terminal descendant operation addressed an ambiguous retained root",
                    ));
                }
                if weight != 1 {
                    return Err(super::Error::InvalidStoredValue(
                        "collector terminal root has non-unit multiplicity",
                    ));
                }
                let state = crate::db::terminal_record::TerminalRecordState::new(OwnedRecord::new(
                    raw, descriptor,
                ))
                .map_err(|_| {
                    super::Error::InvalidStoredValue("invalid retained collector terminal record")
                })?;
                self.structured_app_rows.remove(&operation.root_key);
                self.structured_terminal_records
                    .insert(operation.root_key.clone(), state);
            }
            self.structured_terminal_records
                .get_mut(&operation.root_key)
                .expect("initialized above")
                .apply(operation)
                .map_err(|_| {
                    super::Error::InvalidStoredValue(
                        "invalid collector descendant terminal operation",
                    )
                })?;
            return Ok(());
        }
        let root_was_present = self.structured_root_keys.contains_key(&operation.root_key);
        let _root = match &operation.edit {
            TerminalEdit::Insert { value, .. } | TerminalEdit::Update { value, .. } => {
                let record = OwnedRecord::new(value.clone(), operation.root_descriptor);
                let index = operation.root_descriptor.field_index("row_uuid").ok_or(
                    super::Error::InvalidStoredValue(
                        "root collector terminal operation has no row_uuid",
                    ),
                )?;
                let root = RowUuid(record.borrowed().get_uuid(index)?);
                self.structured_root_keys
                    .insert(operation.root_key.clone(), root);
                root
            }
            TerminalEdit::Remove { .. } | TerminalEdit::Move { .. } => self
                .structured_root_keys
                .get(&operation.root_key)
                .copied()
                .or_else(|| terminal_root_uuid_from_key(&operation.root_key))
                .ok_or(super::Error::InvalidStoredValue(
                    "root collector terminal edit addresses an unknown root key",
                ))?,
        };
        match &operation.edit {
            TerminalEdit::Insert { index, value, .. } => {
                self.structured_terminal_records.remove(&operation.root_key);
                let record = OwnedRecord::new(value.clone(), operation.root_descriptor);
                self.structured_app_rows.remove(&operation.root_key);
                self.apply_structured_app_row_delta(operation.root_key.clone(), record, 1);
                // An insert may replace an existing occurrence. Fresh roots
                // cannot be in the order yet: scanning the growing vector for
                // each one would make an initial result quadratic.
                if root_was_present {
                    self.structured_root_key_order.retain(|key| {
                        #[cfg(test)]
                        {
                            self.root_order_insert_comparisons += 1;
                        }
                        key != &operation.root_key
                    });
                }
                self.structured_root_key_order.insert(
                    (*index).min(self.structured_root_key_order.len()),
                    operation.root_key.clone(),
                );
            }
            TerminalEdit::Update { value, .. } => {
                let record = OwnedRecord::new(value.clone(), operation.root_descriptor);
                if let Some(state) = self
                    .structured_terminal_records
                    .get_mut(&operation.root_key)
                {
                    state.update_record(record).map_err(|_| {
                        super::Error::InvalidStoredValue("invalid collector terminal scalar update")
                    })?;
                    return Ok(());
                }
                self.structured_app_rows.remove(&operation.root_key);
                self.apply_structured_app_row_delta(operation.root_key.clone(), record, 1);
            }
            TerminalEdit::Remove { .. } => {
                self.structured_terminal_records.remove(&operation.root_key);
                self.structured_root_keys.remove(&operation.root_key);
                self.structured_root_key_order
                    .retain(|key| key != &operation.root_key);
                self.structured_app_rows.remove(&operation.root_key);
            }
            TerminalEdit::Move { index, .. } => {
                let key_previous = self
                    .structured_root_key_order
                    .iter()
                    .position(|key| key == &operation.root_key)
                    .ok_or(super::Error::InvalidStoredValue(
                        "root collector terminal move addresses an absent root key",
                    ))?;
                self.structured_root_key_order.remove(key_previous);
                self.structured_root_key_order.insert(
                    (*index).min(self.structured_root_key_order.len()),
                    operation.root_key.clone(),
                );
            }
        }
        Ok(())
    }

    fn reconcile_publishable_result_members(
        &mut self,
        node_aliases: &BTreeMap<NodeUuid, NodeAlias>,
    ) -> (
        Vec<ResultMemberEntry>,
        Vec<ResultMemberEntry>,
        Vec<(ResultMemberEntry, ResultMemberPayloadEntry)>,
        Vec<ResultMemberEntry>,
    ) {
        let (adds, removes) = if let Some(candidates) = self.unreconciled_result_members.take() {
            debug_assert!(self.storage_backed_result_materialization);
            #[cfg(test)]
            {
                self.result_member_reconcile_visits += candidates.len();
            }
            let mut adds = Vec::new();
            let mut removes = Vec::new();
            for member in candidates {
                let present = self
                    .result_weights
                    .get(&member)
                    .is_some_and(|weight| *weight > 0);
                match (self.published_result_members.contains(&member), present) {
                    (false, true) => {
                        self.published_result_members.insert(member.clone());
                        adds.push(member);
                    }
                    (true, false) => {
                        self.published_result_members.remove(&member);
                        removes.push(member);
                    }
                    _ => {}
                }
            }
            (adds, removes)
        } else {
            #[cfg(test)]
            {
                self.result_member_reconcile_visits += self.result_weights.len();
            }
            let publishable = self
                .result_weights
                .iter()
                .filter(|(member, weight)| {
                    **weight > 0
                        && (self.storage_backed_result_materialization
                            || self.result_member_has_inline_content_source(member)
                            || self.result_member_has_bundle_witness(member, node_aliases))
                })
                .map(|(member, _)| member.clone())
                .collect::<BTreeSet<_>>();
            let adds = publishable
                .difference(&self.published_result_members)
                .cloned()
                .collect::<Vec<_>>();
            let removes = self
                .published_result_members
                .difference(&publishable)
                .cloned()
                .collect::<Vec<_>>();
            self.published_result_members = publishable.into();
            (adds, removes)
        };
        self.unreconciled_result_members = self
            .storage_backed_result_materialization
            .then(RetainedResultMembers::default);
        let payload_removes = removes
            .iter()
            .filter(|member| self.published_result_payloads.contains_key(*member))
            .cloned()
            .collect::<Vec<_>>();
        let payload_adds = adds
            .iter()
            .filter_map(|member| {
                self.result_payloads
                    .get(member)
                    .cloned()
                    .map(|payload| (member.clone(), payload))
            })
            .collect::<Vec<_>>();
        for member in &payload_removes {
            self.published_result_payloads.remove(member);
        }
        for (member, payload) in &payload_adds {
            self.published_result_payloads
                .insert(member.clone(), payload.clone());
        }
        (adds, removes, payload_adds, payload_removes)
    }

    fn result_member_has_bundle_witness(
        &self,
        member: &ResultMemberEntry,
        node_aliases: &BTreeMap<NodeUuid, NodeAlias>,
    ) -> bool {
        let Some((table, row_uuid, tx_id)) = member.as_row() else {
            // Synthetic aggregate output is self-contained in its payload
            // fact, so it has no Stream B history-row witness.
            return true;
        };
        // Existence does not require owning or deduplicating the transaction's
        // rows. Keep selected deletion witnesses in the same candidate union.
        self.versions
            .rows_by_tx(tx_id)
            .chain(
                self.selected_deletion_witnesses
                    .iter()
                    .filter_map(|(fact, version)| (fact.version.tx == tx_id).then_some(version)),
            )
            .any(|version| {
                version.table() == table.as_str()
                    && version.row_uuid() == row_uuid
                    && version.deletion().is_none()
            })
            || self
                .replacement_for(table.as_str(), row_uuid)
                .0
                .is_some_and(|version| {
                    version_tx_id_from_aliases(&version, node_aliases) == Some(tx_id)
                })
    }

    fn result_member_has_inline_content_source(&self, member: &ResultMemberEntry) -> bool {
        member
            .as_real_row()
            .and_then(|row| row.branch_or_prefix.as_ref())
            .is_some_and(|branch_key| self.inline_content_branch_keys.contains(branch_key))
    }

    fn apply_result_delta(
        &mut self,
        entry: ResultMemberEntry,
        payload: ResultMemberPayloadEntry,
        weight: i64,
        transitions: &mut ResultTransitions,
    ) {
        self.mark_result_member_changed(&entry);
        let old = self.result_weights.get(&entry).copied().unwrap_or(0);
        let new = old + weight;
        if old <= 0 && new > 0 {
            transitions.adds.push(entry.clone());
            if entry
                .as_real_row()
                .is_some_and(|row| row.row_digest.is_some())
            {
                transitions
                    .result_payload_adds
                    .push((entry.clone(), payload.clone()));
                self.result_payloads.insert(entry.clone(), payload);
            }
        }
        if old > 0 && new <= 0 {
            transitions.removes.push(entry.clone());
            transitions.result_payload_removes.push(entry.clone());
            self.result_payloads.remove(&entry);
        }
        if new == 0 {
            self.result_weights.remove(&entry);
        } else {
            self.result_weights.insert(entry, new);
        }
    }

    fn apply_aggregate_result_delta(
        &mut self,
        member: ResultMemberEntry,
        payload: ResultMemberPayloadEntry,
        _synthetic: &super::query_engine::SyntheticResultMembershipSchema,
        _value_fields: &[String],
        weight: i64,
        transitions: &mut ResultTransitions,
    ) -> Result<(), super::Error> {
        self.mark_result_member_changed(&member);
        let (old_member, old_payload) = self.aggregate_payload_for_stable_member(&member);
        if weight < 0 {
            // Groove's aggregate operator emits complete before/after group
            // rows. A retraction therefore removes only the payload it names;
            // if its replacement is already current, it is stale.
            if old_member.as_ref() == Some(&member) {
                transitions.removes.push(member.clone());
                self.result_weights.remove(&member);
                if let Some(existing) = self.result_payloads.remove(&member) {
                    transitions.result_payload_removes.push(member.clone());
                    transitions
                        .program_fact_removes
                        .push(ProgramFactEntry::ResultPayload(existing));
                }
            }
            return Ok(());
        }

        if let Some(old_member) = old_member
            && old_member != member
        {
            self.mark_result_member_changed(&old_member);
            transitions.removes.push(old_member.clone());
            self.result_weights.remove(&old_member);
            if let Some(existing) = self.result_payloads.remove(&old_member).or(old_payload) {
                transitions.result_payload_removes.push(old_member.clone());
                transitions
                    .program_fact_removes
                    .push(ProgramFactEntry::ResultPayload(existing));
            }
        }
        if self.result_weights.get(&member).copied().unwrap_or(0) <= 0 {
            transitions.adds.push(member.clone());
        }
        transitions
            .program_fact_adds
            .push(ProgramFactEntry::ResultPayload(payload.clone()));
        transitions
            .result_payload_adds
            .push((member.clone(), payload.clone()));
        self.result_payloads.insert(member.clone(), payload);
        self.result_weights.insert(member, 1);
        Ok(())
    }

    fn mark_result_member_changed(&mut self, member: &ResultMemberEntry) {
        if let Some(changed) = &mut self.unreconciled_result_members {
            changed.insert(member.clone());
        }
    }

    fn aggregate_payload_for_stable_member(
        &self,
        member: &ResultMemberEntry,
    ) -> (Option<ResultMemberEntry>, Option<ResultMemberPayloadEntry>) {
        let ResultMemberEntry::Synthetic { table, row, .. } = member else {
            return (None, None);
        };
        self.result_payloads
            .iter()
            .find_map(|(candidate, payload)| match candidate {
                ResultMemberEntry::Synthetic {
                    table: candidate_table,
                    row: candidate_row,
                    ..
                } if candidate_table == table && candidate_row == row => {
                    Some((candidate.clone(), payload.clone()))
                }
                _ => None,
            })
            .map(|(member, payload)| (Some(member), Some(payload)))
            .unwrap_or((None, None))
    }
}

/// Preserve the exact source occurrence that made a maintained program
/// advance. This intentionally names input rows rather than collector output:
/// a retained result member can change because a nested child, a sort key, or
/// a deletion-register witness advanced while the output membership did not.
#[cfg(test)]
fn covered_input_for_version(
    source: ProgramSourceId,
    row: &VersionRow,
    node_aliases: &BTreeMap<NodeUuid, NodeAlias>,
) -> Result<CoveredInputEntry, super::Error> {
    let tx = version_tx_id_from_aliases(row, node_aliases).ok_or(
        super::Error::InvalidStoredValue("covered input tx node alias must exist"),
    )?;
    let branch_or_prefix = row.branch_key().canonical_bytes();
    Ok(CoveredInputEntry {
        source,
        version_table: row.table().to_owned().into(),
        source_row: row.row_uuid(),
        version: RowVersionRefEntry {
            tx,
            schema_version: None,
            layer: match row.layer() {
                VersionLayer::Content => ResultRowLayer::Content,
                VersionLayer::Deletion => ResultRowLayer::Deletion,
            },
            batch: Some(tx),
            branch_or_prefix: (!branch_or_prefix.is_empty()).then_some(branch_or_prefix),
            row_digest: None,
        },
    })
}

/// Rebind a runtime terminal operation to its early-bound prepared layout.
///
/// The runtime may tighten a root field from `Nullable(T)` to `T` after an
/// inner proof.  That is not an alternate public layout: the prepared layout
/// is the subscription's immutable decoding contract.  Re-encode only a
/// root-level payload into that contract, preserving the source value as a
/// present nullable cell. Nested edits address named collections and stable
/// keys. An unrelated root field may tighten without changing those edits,
/// but the addressed collection's complete subtree layout must agree exactly.
#[cfg_attr(
    feature = "cold-settle-attribution",
    tracing::instrument(skip_all, name = "cold.phase.rebind_terminal_output")
)]
fn rebind_terminal_operation_to_layout(
    mut operation: TerminalOperation,
    layout: &TerminalRootLayout,
) -> Result<TerminalOperation, super::Error> {
    if operation.root_descriptor == layout.root_descriptor {
        return Ok(operation);
    }
    if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
        eprintln!(
            "JAZZ_COVERED_INPUT_TRACE terminal_descriptor_mismatch operation={:?} layout={:?}",
            operation.root_descriptor, layout.root_descriptor,
        );
    }
    if !terminal_descriptor_can_rebind_to_layout(
        &operation.root_descriptor,
        &layout.root_descriptor,
    ) || !terminal_nested_collection_layout_agrees(&operation, &layout.root_descriptor)
    {
        return Err(super::Error::InvalidStoredValue(
            "structured terminal operation descriptor disagrees with prepared root layout",
        ));
    }

    match &mut operation.edit {
        TerminalEdit::Insert { value, .. } | TerminalEdit::Update { value, .. }
            if operation.path.is_empty() =>
        {
            *value = reencode_terminal_root_record(
                operation.root_descriptor,
                &layout.root_descriptor,
                value,
            )?;
        }
        _ => {}
    }
    operation.root_descriptor = layout.root_descriptor;
    Ok(operation)
}

fn terminal_nested_collection_layout_agrees(
    operation: &TerminalOperation,
    target: &RecordDescriptor,
) -> bool {
    let Some(first) = operation.path.first() else {
        return true;
    };
    let TerminalPathSegment::Collection(name) = first else {
        return false;
    };
    let (Some(source_index), Some(target_index)) = (
        operation.root_descriptor.field_index(name),
        target.field_index(name),
    ) else {
        return false;
    };
    let source_type = &operation.root_descriptor.fields()[source_index].value_type;
    matches!(source_type, ValueType::Array(element) if matches!(element.as_ref(), ValueType::Record(_)))
        && source_type == &target.fields()[target_index].value_type
}

fn terminal_descriptor_can_rebind_to_layout(
    source: &RecordDescriptor,
    target: &RecordDescriptor,
) -> bool {
    source.fields().len() == target.fields().len()
        && source
            .fields()
            .iter()
            .zip(target.fields())
            .all(|(source, target)| {
                source.name == target.name
                    && terminal_field_can_rebind_to_layout(&source.value_type, &target.value_type)
            })
}

fn terminal_field_can_rebind_to_layout(source: &ValueType, target: &ValueType) -> bool {
    source == target
        || matches!(target, ValueType::Nullable(inner) if source == inner.as_ref())
        || RecordProjector::new_registry_rebound(
            RecordDescriptor::new([("value", source.clone())]),
            RecordDescriptor::new([("value", target.clone())]),
            [(0, 0)],
        )
        .is_ok()
}

fn reencode_terminal_root_record(
    source: RecordDescriptor,
    target: &RecordDescriptor,
    raw: &[u8],
) -> Result<Vec<u8>, super::Error> {
    let values = source
        .bind(raw)
        .to_values()
        .map_err(|_| super::Error::InvalidStoredValue("invalid structured terminal record"))?;
    let values = source
        .fields()
        .iter()
        .zip(target.fields())
        .zip(values)
        .map(|((source, target), value)| {
            rebind_terminal_value(value, &source.value_type, &target.value_type)
        })
        .collect::<Result<Vec<_>, _>>()?;
    target.create(&values).map_err(|_| {
        super::Error::InvalidStoredValue("structured terminal root re-encoding failed")
    })
}

fn rebind_terminal_value(
    value: Value,
    source: &ValueType,
    target: &ValueType,
) -> Result<Value, super::Error> {
    if source == target {
        return Ok(value);
    }
    if let ValueType::Nullable(inner) = target
        && source == inner.as_ref()
    {
        return Ok(Value::Nullable(Some(Box::new(value))));
    }
    if !terminal_field_can_rebind_to_layout(source, target) {
        return Err(super::Error::InvalidStoredValue(
            "structured terminal root value disagrees with prepared layout",
        ));
    }
    match (value, source, target) {
        (Value::Tuple(values), ValueType::Tuple(source), ValueType::Tuple(target)) => {
            Ok(Value::Tuple(
                values
                    .into_iter()
                    .zip(source)
                    .zip(target)
                    .map(|((value, source), target)| rebind_terminal_value(value, source, target))
                    .collect::<Result<_, _>>()?,
            ))
        }
        (Value::Array(values), ValueType::Array(source), ValueType::Array(target)) => {
            Ok(Value::Array(
                values
                    .into_iter()
                    .map(|value| rebind_terminal_value(value, source, target))
                    .collect::<Result<_, _>>()?,
            ))
        }
        (Value::Nullable(value), ValueType::Nullable(source), ValueType::Nullable(target)) => {
            Ok(Value::Nullable(
                value
                    .map(|value| rebind_terminal_value(*value, source, target).map(Box::new))
                    .transpose()?,
            ))
        }
        (Value::Record(record), ValueType::Record(source), ValueType::Record(target)) => {
            let values = record.to_values().map_err(|_| {
                super::Error::InvalidStoredValue("invalid structured terminal record")
            })?;
            let values = source
                .fields()
                .iter()
                .zip(target.fields())
                .zip(values)
                .map(|((source, target), value)| {
                    rebind_terminal_value(value, &source.value_type, &target.value_type)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let raw = target.create(&values).map_err(|_| {
                super::Error::InvalidStoredValue("structured terminal nested re-encoding failed")
            })?;
            Ok(Value::Record(OwnedRecord::new(raw, **target)))
        }
        (Value::Enum(value), ValueType::Enum(source), ValueType::Enum(target)) => {
            let tag = value.tag();
            let source_case = source.case(tag).map_err(|_| {
                super::Error::InvalidStoredValue("invalid structured terminal enum tag")
            })?;
            let target_case = target.case(tag).map_err(|_| {
                super::Error::InvalidStoredValue("prepared terminal enum tag is absent")
            })?;
            let record = rebind_terminal_value(
                Value::Record(value.into_record()),
                &ValueType::Record(Box::new(source_case.payload)),
                &ValueType::Record(Box::new(target_case.payload)),
            )?;
            let Value::Record(record) = record else {
                unreachable!("record rebinding preserves record value")
            };
            Ok(Value::Enum(EnumValue::new(tag, record)))
        }
        (value, _, _) => Ok(value),
    }
}

impl MaintainedTerminalSchemas {
    pub(in crate::node) fn current_payload_schema(
        &self,
    ) -> Result<&ResultMembershipSchema, super::Error> {
        self.sinks
            .values()
            .find_map(|kind| match kind {
                MaintainedTerminalKind::ResultCurrent(schema) => Some(schema),
                _ => None,
            })
            .ok_or(super::Error::InvalidStoredValue(
                "maintained result has no compiled payload schema",
            ))
    }
    pub(in crate::node) fn direct_app_row_schema(&self) -> Result<&AppRowSchema, super::Error> {
        self.sinks
            .values()
            .find_map(|kind| match kind {
                MaintainedTerminalKind::DirectAppRows(output) => Some(output),
                _ => None,
            })
            .ok_or(super::Error::InvalidStoredValue(
                "maintained direct result has no compiler-owned app-row schema",
            ))
    }

    pub(in crate::node) fn aggregate_app_row_schema(&self) -> Result<&AppRowSchema, super::Error> {
        self.sinks
            .values()
            .find_map(|kind| match kind {
                MaintainedTerminalKind::AggregateAppRows(output) => Some(output),
                _ => None,
            })
            .ok_or(super::Error::InvalidStoredValue(
                "maintained aggregate has no compiler-owned app-row schema",
            ))
    }

    #[cfg(feature = "testing")]
    pub(crate) fn footprint(&self) -> MaintainedTerminalSchemasFootprint {
        let terminal_schemas_bytes = btree_map_bytes(self.sinks.len())
            + self
                .sinks
                .iter()
                .map(|(sink, kind)| sink.len() + mem::size_of_val(kind))
                .sum::<usize>();
        MaintainedTerminalSchemasFootprint {
            terminal_schemas: self.sinks.len(),
            terminal_schemas_bytes,
        }
    }

    fn for_program(program: &QueryProgram) -> Self {
        let mut sinks = BTreeMap::new();
        for terminal in &program.lowered.terminals {
            if let OutputTerminalSchema::AppRows(rows) = &terminal.output {
                let kind = match &rows.terminal {
                    crate::node::query_engine::AppRowTerminal::RootCollector => {
                        if rows.descriptor.field_index("row_uuid").is_none() {
                            panic!("public root collector app-row terminal has no row_uuid");
                        }
                        MaintainedTerminalKind::RootCollectorAppRows {
                            schema: rows.clone(),
                            layout: terminal_root_layout(rows),
                        }
                    }
                    crate::node::query_engine::AppRowTerminal::Direct
                        if rows.descriptor.field_index("row_uuid").is_some() =>
                    {
                        MaintainedTerminalKind::DirectAppRows(rows.clone())
                    }
                    crate::node::query_engine::AppRowTerminal::Aggregate(_) => {
                        MaintainedTerminalKind::AggregateAppRows(rows.clone())
                    }
                    crate::node::query_engine::AppRowTerminal::Direct => {
                        panic!("direct app-row terminal has no row_uuid")
                    }
                };
                sinks.insert(terminal.sink.clone(), kind);
                continue;
            };
            let OutputTerminalSchema::Fact(fact) = &terminal.output else {
                unreachable!("app-row terminals were handled above")
            };
            let mut kind = match (&fact.key, fact.terminal, &fact.schema) {
                (
                    ProgramFactKey::ResultMembership,
                    ProgramFactTerminal::Primary,
                    ProgramFactSchema::ResultMembership(schema),
                ) => Some(MaintainedTerminalKind::ResultCurrent(schema.clone())),
                (
                    ProgramFactKey::ResultMembership,
                    ProgramFactTerminal::Primary,
                    ProgramFactSchema::AggregateResult(schema),
                ) => Some(MaintainedTerminalKind::AggregateResult(schema.clone())),
                (
                    ProgramFactKey::ProgramSourceCoverage(_),
                    ProgramFactTerminal::Primary,
                    ProgramFactSchema::ProgramSourceCoverage(schema),
                ) => Some(MaintainedTerminalKind::ProgramSourceCoverage(
                    schema.clone(),
                )),
                (
                    ProgramFactKey::RelationEdges,
                    ProgramFactTerminal::Primary,
                    ProgramFactSchema::RelationEdges(schema),
                ) => Some(MaintainedTerminalKind::RelationEdge(schema.clone())),
                (
                    ProgramFactKey::VersionWitnesses,
                    ProgramFactTerminal::VersionWitnessDeletion,
                    ProgramFactSchema::VersionWitnesses(schema),
                ) => schema
                    .deletion
                    .clone()
                    .map(MaintainedTerminalKind::VersionDeletion),
                (
                    ProgramFactKey::VersionWitnesses,
                    ProgramFactTerminal::VersionWitnessContent,
                    ProgramFactSchema::VersionWitnesses(schema),
                ) => schema
                    .content
                    .clone()
                    .map(MaintainedTerminalKind::VersionContent),
                (
                    ProgramFactKey::ReplacementWitnesses,
                    ProgramFactTerminal::ReplacementWitnessDeletion,
                    ProgramFactSchema::ReplacementWitnesses(schema),
                ) => schema
                    .deletion
                    .clone()
                    .map(MaintainedTerminalKind::ReplacementDeletion),
                (
                    ProgramFactKey::ReplacementWitnesses,
                    ProgramFactTerminal::ReplacementWitnessContent,
                    ProgramFactSchema::ReplacementWitnesses(schema),
                ) => schema
                    .content
                    .clone()
                    .map(MaintainedTerminalKind::ReplacementContent),
                _ => None,
            };
            if program
                .lowered
                .shared_witness_sinks
                .values()
                .any(|sink| sink == &terminal.sink)
            {
                kind = match kind {
                    Some(MaintainedTerminalKind::VersionContent(schema)) => {
                        Some(MaintainedTerminalKind::SharedContent(schema))
                    }
                    Some(MaintainedTerminalKind::VersionDeletion(schema)) => {
                        Some(MaintainedTerminalKind::SharedDeletion(schema))
                    }
                    other => other,
                };
            }
            if let Some(kind) = kind {
                sinks.insert(terminal.sink.clone(), kind);
            }
        }
        Self { sinks }
    }

    fn get(&self, sink: &str) -> Result<&MaintainedTerminalKind, super::Error> {
        self.sinks.get(sink).ok_or(super::Error::InvalidStoredValue(
            "maintained view delta arrived for an unknown query-engine terminal",
        ))
    }

    pub(crate) fn terminal_root_layout(&self) -> Option<&TerminalRootLayout> {
        self.sinks.values().find_map(|kind| match kind {
            MaintainedTerminalKind::RootCollectorAppRows { layout, .. } => Some(layout),
            _ => None,
        })
    }

    pub(crate) fn has_root_collector(&self) -> bool {
        self.sinks
            .values()
            .any(|kind| matches!(kind, MaintainedTerminalKind::RootCollectorAppRows { .. }))
    }
}

fn terminal_root_layout(rows: &AppRowSchema) -> TerminalRootLayout {
    // The terminal exposes canonical nested schemas, while publication below
    // restores only public logical identities. Runtime allocation slots never
    // enter the terminal descriptor or its layout hash.
    let canonical_descriptor = groove::records::decode_persisted_record_descriptor(
        &groove::records::encode_persisted_record_descriptor(&rows.descriptor)
            .expect("compiler row schema is encodable"),
    )
    .expect("compiler row schema is canonical");
    let mut descriptor_fields = canonical_descriptor.fields().to_vec();
    let root_key_slot = rows
        .descriptor
        .fields()
        .iter()
        .position(|field| field.name.as_deref() == Some("row_uuid"))
        .expect("structured app-row terminal has a row_uuid slot");
    // Bind every public descriptor slot, including collector-owned trailing
    // arrays/records. A collector root may contain both physical `user_*`
    // source cells and logical nested fields, so the presence of one family
    // must not hide the other.
    let public_fields = rows
        .descriptor
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(slot, field)| {
            let name = field.name.as_deref()?;
            let publication = rows.publication_fields.get(name)?.clone();
            // Provenance is retained by the collector as a routing field, but
            // a selected magic column is public output. Keeping its terminal
            // slot preserves both the synthesized value and recursive fields
            // already owned by this descriptor.
            let selected_provenance = matches!(
                &publication,
                CurrentRowPublicationField::ResultField {
                    visibility: CurrentRowResultVisibility::PublicProvenance,
                    ..
                }
            );
            if slot == root_key_slot || (rows.hidden_fields.contains(name) && !selected_provenance)
            {
                return None;
            }
            let public_name = publication.public_name()?.to_owned();
            let carrier = rows
                .field_carriers
                .get(name)
                .copied()
                .unwrap_or(rows.carrier);
            Some(TerminalRootPublicField {
                publication,
                name: public_name,
                descriptor_field_name: name.to_owned(),
                slot,
                carrier: match carrier {
                    AppRowCarrier::CurrentRow => TerminalRootCarrier::CurrentRow,
                    AppRowCarrier::Logical => TerminalRootCarrier::Logical,
                },
            })
        })
        .collect::<Vec<_>>();
    for field in &public_fields {
        if let Some(descriptor_field) = descriptor_fields.get_mut(field.slot) {
            descriptor_field.identity =
                Some(groove::records::FieldIdentity::Name(field.name.clone()));
        }
    }
    let root_descriptor = RecordDescriptor::new_with_fields(descriptor_fields);
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"jazz terminal root publication v1");
    let role_descriptor =
        RecordDescriptor::new_with_fields(root_descriptor.fields().iter().enumerate().map(
            |(slot, field)| {
                let role_name = if slot == root_key_slot {
                    "metadata/row_uuid".to_owned()
                } else if let Some(public) = public_fields.iter().find(|public| public.slot == slot)
                {
                    use super::{
                        CurrentRowPublicationField as Publication,
                        CurrentRowResultVisibility as Visibility,
                    };
                    let role = match &public.publication {
                        Publication::StoredColumn { .. }
                        | Publication::UnresolvedSourceCell { .. } => "source",
                        Publication::ResultField {
                            visibility: Visibility::ApplicationCell,
                            ..
                        } => "result",
                        Publication::ResultField {
                            visibility: Visibility::PublicProvenance,
                            ..
                        } => "provenance",
                        Publication::ResultField {
                            visibility: Visibility::HiddenMetadata,
                            ..
                        } => "hidden",
                    };
                    format!("{role}/{slot}/{}", public.name)
                } else {
                    // Hidden routing fields affect byte layout, not public field identity.
                    format!("hidden/{slot}")
                };
                groove::records::DescriptorField::new(role_name, field.value_type.clone())
            },
        ));
    hasher.update(
        &groove::records::encode_persisted_record_descriptor(&role_descriptor)
            .expect("terminal role schemas contain valid Groove record descriptors"),
    );
    hasher.update(&(root_key_slot as u64).to_le_bytes());
    hasher.update(&[match rows.carrier {
        AppRowCarrier::CurrentRow => 0,
        AppRowCarrier::Logical => 1,
    }]);
    hasher.update(&[u8::from(rows.root_union_arm)]);
    for field in &public_fields {
        hasher.update(field.name.as_bytes());
        hasher.update(&[0]);

        hasher.update(&(field.slot as u64).to_le_bytes());
        hasher.update(&[match field.carrier {
            TerminalRootCarrier::CurrentRow => 0,
            TerminalRootCarrier::Logical => 1,
        }]);
    }
    TerminalRootLayout {
        id: format!("terminal:{}", hasher.finalize().to_hex()),
        root_descriptor,
        root_key_slot,
        root_key_field_name: rows.descriptor.fields()[root_key_slot]
            .name
            .clone()
            .expect("structured app-row row_uuid field is named"),
        public_fields,
        carrier: match rows.carrier {
            AppRowCarrier::CurrentRow => TerminalRootCarrier::CurrentRow,
            AppRowCarrier::Logical => TerminalRootCarrier::Logical,
        },
        root_union_arm: rows.root_union_arm,
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[cfg(feature = "testing")]
pub(crate) struct MaintainedTerminalSchemasFootprint {
    pub(crate) terminal_schemas: usize,
    pub(crate) terminal_schemas_bytes: usize,
}

impl MaintainedTerminalKind {
    fn is_result_terminal(&self) -> bool {
        matches!(
            self,
            MaintainedTerminalKind::ResultCurrent(_) | MaintainedTerminalKind::AggregateResult(_)
        )
    }

    fn requires_authoritative_membership_reconcile(&self) -> bool {
        matches!(
            self,
            MaintainedTerminalKind::VersionDeletion(_)
                | MaintainedTerminalKind::ReplacementDeletion(_)
                | MaintainedTerminalKind::SharedDeletion(_)
        )
    }
}

fn decode_typed_terminal_record(
    record: BorrowedRecord<'_>,
    kind: &MaintainedTerminalKind,
    tables: &TableSchemas,
    node_aliases: &BTreeMap<NodeUuid, NodeAlias>,
    decode_plan_cache: &mut VersionDecodePlanCache,
    payload_plans: &mut std::collections::HashMap<
        RecordDescriptor,
        super::descriptor_roles::CurrentPayloadEncodePlan,
    >,
    read_view: crate::protocol::ReadViewKey,
) -> Result<DecodedMaintainedEvent, super::Error> {
    match kind {
        MaintainedTerminalKind::SharedContent(schema)
        | MaintainedTerminalKind::SharedDeletion(schema) => {
            let expected = if matches!(kind, MaintainedTerminalKind::SharedContent(_)) {
                "version_content"
            } else {
                "version_deletion"
            };
            validate_witness_event_kind(record, expected)?;
            decode_typed_version_witness(record, schema, tables, decode_plan_cache).map(|row| {
                DecodedMaintainedEvent::SharedVersion {
                    source: schema.source.clone(),
                    row,
                }
            })
        }
        MaintainedTerminalKind::AggregateAppRows(output) => {
            let crate::node::query_engine::AppRowTerminal::Aggregate(schema) = &output.terminal
            else {
                return Err(super::Error::InvalidStoredValue(
                    "aggregate terminal lost its lowered schema",
                ));
            };
            decode_aggregate_app_row(record, schema)
        }
        MaintainedTerminalKind::ResultCurrent(schema) => {
            let table_name = match record.get_idx(field_idx(record, &schema.table_field)?)? {
                Value::String(value) => value,
                _ => {
                    return Err(super::Error::InvalidStoredValue(
                        "maintained result membership table field must be string",
                    ));
                }
            };
            let table = tables
                .get(&table_name)
                .ok_or(super::Error::InvalidStoredValue(
                    "maintained result membership table_name must exist",
                ))?;
            let row_uuid = RowUuid(record.get_uuid(field_idx(record, &schema.row_field)?)?);
            let mut occurrence_ids = Vec::with_capacity(schema.occurrence_id_fields.len());
            for field in &schema.occurrence_id_fields {
                occurrence_ids.push(ObjectId::from_uuid(
                    record.get_uuid(field_idx(record, field)?)?,
                ));
            }
            let Some((root, joined)) = occurrence_ids.split_first() else {
                return Err(super::Error::InvalidStoredValue(
                    "maintained result membership occurrence must include its root row",
                ));
            };
            let union_arms = schema
                .occurrence_union_arm_fields
                .iter()
                .map(|(position, field)| {
                    let label = match record.get_idx(field_idx(record, field)?)? {
                        Value::String(label) if !label.is_empty() => label.clone(),
                        _ => {
                            return Err(super::Error::InvalidStoredValue(
                                "maintained result union arm must be a non-empty string",
                            ));
                        }
                    };
                    Ok((*position, label))
                })
                .collect::<Result<Vec<_>, super::Error>>()?;
            let occurrence_id =
                OutputOccurrenceId::with_union_arms(*root, joined.iter().copied(), union_arms)
                    .ok_or(super::Error::InvalidStoredValue(
                        "maintained result union occurrence carrier is malformed",
                    ))?;
            let (tx_time_field, tx_node_field) = match &schema.version {
                super::query_engine::ResultMembershipVersionSchema::Content(content) => {
                    (&content.tx_time_field, &content.tx_node_field)
                }
                super::query_engine::ResultMembershipVersionSchema::ContentOrDeletion {
                    ..
                } => {
                    return Err(super::Error::InvalidStoredValue(
                        "maintained result membership does not support include-deleted schemas yet",
                    ));
                }
            };
            let tx_time = TxTime(record_u64(record, tx_time_field)?);
            let tx_node_alias = NodeAlias(record_u64(record, tx_node_field)?);
            let tx_node = node_aliases
                .iter()
                .find_map(|(node, alias)| (*alias == tx_node_alias).then_some(*node))
                .ok_or(super::Error::InvalidStoredValue(
                    "result tx node alias must exist",
                ))?;
            let settle_position = schema
                .settle_position_field
                .as_ref()
                .map(|field| nullable_u64(record, field).map(|seq| seq.map(GlobalTime)))
                .transpose()?
                .flatten();
            let flat_join_digest = (!schema.payload_fields.is_empty())
                .then(|| {
                    schema
                        .payload_fields
                        .iter()
                        .map(|field| {
                            record
                                .get_idx(field_idx(record, &field.name)?)
                                .map_err(super::Error::from)
                        })
                        .collect::<Result<Vec<_>, _>>()
                        .and_then(|values| flat_join_row_digest(&schema.payload_fields, &values))
                })
                .transpose()?;
            let branch_or_prefix = schema
                .branch_or_prefix_field
                .as_deref()
                .map(|field| match record.get_idx(field_idx(record, field)?)? {
                    Value::Uuid(value) => Ok(value.as_bytes().to_vec()),
                    Value::Bytes(value) => Ok(value),
                    Value::Nullable(Some(value)) => match *value {
                        Value::Uuid(value) => Ok(value.as_bytes().to_vec()),
                        Value::Bytes(value) => Ok(value),
                        _ => Err(super::Error::InvalidStoredValue(
                            "result branch discriminator must be UUID or bytes",
                        )),
                    },
                    Value::Nullable(None) => Ok(Vec::new()),
                    _ => Err(super::Error::InvalidStoredValue(
                        "result branch discriminator must be UUID or bytes",
                    )),
                })
                .transpose()?
                // The empty/shared branch has a non-empty postcard encoding.
                // Keep its historical `None` identity so ordinary result
                // members and runtime receipts do not churn merely because
                // branch coordinates are now carried for non-shared rows.
                .filter(|bytes| {
                    !bytes.is_empty() && *bytes != BranchKey::default().canonical_bytes()
                });
            let mut member = RealRowMemberEntry::current_content((
                table.name.clone().into(),
                row_uuid,
                TxId::new(tx_time, tx_node),
            ))
            .with_occurrence_id(occurrence_id)
            .with_settle_position(settle_position);
            member.read_view = read_view;
            member.branch_or_prefix = branch_or_prefix;
            let member: ResultMemberEntry = match flat_join_digest {
                Some(digest) => member.with_row_digest(digest),
                None => member,
            }
            .into();
            let plan = match payload_plans.entry(record.descriptor()) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(super::descriptor_roles::CurrentPayloadEncodePlan::new(
                        record.descriptor(),
                        schema,
                    )?)
                }
            };
            let (descriptor, row_bytes) = plan.encode(record)?;
            let payload = ResultMemberPayloadEntry {
                member: member.clone(),
                descriptor,
                record: row_bytes,
            };
            Ok(DecodedMaintainedEvent::ResultCurrent { member, payload })
        }
        MaintainedTerminalKind::AggregateResult(schema) => {
            let table = match record.get_idx(field_idx(record, &schema.synthetic.table_field)?)? {
                Value::String(value) => value,
                _ => {
                    return Err(super::Error::InvalidStoredValue(
                        "aggregate result table field must be string",
                    ));
                }
            };
            let row_idx = field_idx(record, &schema.synthetic.row_field)?;
            let row_value = record.get_idx(row_idx)?;
            let row_type = record
                .descriptor()
                .fields()
                .get(row_idx)
                .ok_or(super::Error::InvalidStoredValue(
                    "aggregate result row field is missing from descriptor",
                ))?
                .value_type
                .clone();
            let row = runtime_result_identity_bytes(&row_value, &row_type)?;
            let replacement_idx = field_idx(record, &schema.synthetic.replacement_field)?;
            let replacement_value = record.get_idx(replacement_idx)?;
            let replacement_type = record
                .descriptor()
                .fields()
                .get(replacement_idx)
                .ok_or(super::Error::InvalidStoredValue(
                    "aggregate replacement field is missing from descriptor",
                ))?
                .value_type
                .clone();
            let replacement = runtime_result_identity_bytes(&replacement_value, &replacement_type)?;
            let member = ResultMemberEntry::Synthetic {
                table,
                row,
                replacement: SyntheticReplacementToken::from_encoded_record(replacement),
            };
            let (descriptor, row_bytes) =
                super::descriptor_roles::encode_aggregate_payload_record(record, schema)?;
            let payload = ResultMemberPayloadEntry {
                member: member.clone(),
                descriptor,
                record: row_bytes,
            };
            Ok(DecodedMaintainedEvent::AggregateResult {
                member,
                payload,
                synthetic: schema.synthetic.clone(),
                value_fields: schema
                    .value_fields
                    .iter()
                    .map(|field| {
                        field
                            .name
                            .clone()
                            .expect("lowered aggregate output is named")
                    })
                    .collect(),
            })
        }
        MaintainedTerminalKind::ProgramSourceCoverage(schema) => {
            let complete = match record.get_idx(field_idx(record, "complete")?)? {
                Value::Bool(complete) => complete,
                _ => {
                    return Err(super::Error::InvalidStoredValue(
                        "program-source coverage complete field must be bool",
                    ));
                }
            };
            if complete != schema.complete {
                return Err(super::Error::InvalidStoredValue(
                    "program-source coverage terminal disagrees with compiled schema",
                ));
            }
            Ok(DecodedMaintainedEvent::ProgramSourceCoverage(
                crate::protocol::ProgramSourceCoverageEntry {
                    source: schema.source.clone(),
                    complete,
                },
            ))
        }
        MaintainedTerminalKind::VersionContent(schema) => {
            validate_witness_event_kind(record, "version_content")?;
            decode_typed_version_witness(record, schema, tables, decode_plan_cache).map(|row| {
                DecodedMaintainedEvent::VersionContent {
                    source: schema.source.clone(),
                    row,
                }
            })
        }
        MaintainedTerminalKind::VersionDeletion(schema) => {
            validate_witness_event_kind(record, "version_deletion")?;
            decode_typed_version_witness(record, schema, tables, decode_plan_cache).map(|row| {
                DecodedMaintainedEvent::VersionDeletion {
                    source: schema.source.clone(),
                    row,
                }
            })
        }
        MaintainedTerminalKind::ReplacementContent(schema) => {
            validate_witness_event_kind(record, "replacement_content")?;
            decode_typed_version_witness(record, schema, tables, decode_plan_cache).map(|row| {
                DecodedMaintainedEvent::ReplacementContent {
                    source: schema.source.clone(),
                    row,
                }
            })
        }
        MaintainedTerminalKind::ReplacementDeletion(schema) => {
            validate_witness_event_kind(record, "replacement_deletion")?;
            decode_typed_version_witness(record, schema, tables, decode_plan_cache).map(|row| {
                DecodedMaintainedEvent::ReplacementDeletion {
                    source: schema.source.clone(),
                    row,
                }
            })
        }
        MaintainedTerminalKind::RelationEdge(schema) => {
            decode_typed_relation_edge(record, schema, tables, node_aliases)
                .map(DecodedMaintainedEvent::RelationEdge)
        }
        MaintainedTerminalKind::RootCollectorAppRows { schema, .. }
        | MaintainedTerminalKind::DirectAppRows(schema) => {
            let root = RowUuid(record.get_uuid(field_idx(record, "row_uuid")?)?);
            Ok(DecodedMaintainedEvent::StructuredAppRow {
                root,
                record: OwnedRecord::new(record.raw().to_vec(), schema.descriptor),
            })
        }
    }
}

/// Decode the aggregate graph's sole application terminal into the synthetic
/// member/payload pair used by the maintained reducer.  The member identity is
/// derived from the group key (or the one ungrouped empty group) and the
/// replacement token from the aggregate value; neither is an authority-sent
/// result row.
fn decode_aggregate_app_row(
    record: BorrowedRecord<'_>,
    schema: &AggregateResultSchema,
) -> Result<DecodedMaintainedEvent, super::Error> {
    if schema.group_key_fields.len() > 1 {
        return Err(super::Error::InvalidStoredValue(
            "aggregate app-row terminal has unsupported multi-column group identity",
        ));
    }
    let descriptor = record.descriptor();
    let (row_value, row_type) = match schema.group_key_fields.first() {
        Some(group) => {
            let index = descriptor
                .field_index_by_identity(group.identity.as_ref().ok_or(
                    super::Error::InvalidStoredValue("aggregate group has no lowered identity"),
                )?)
                .ok_or(super::Error::InvalidStoredValue(
                    "aggregate app-row terminal is missing group identity",
                ))?;
            let field = descriptor
                .fields()
                .get(index)
                .ok_or(super::Error::InvalidStoredValue(
                    "aggregate app-row group descriptor is missing",
                ))?;
            (record.get_idx(index)?, field.value_type.clone())
        }
        None => (Value::String("global".to_owned()), ValueType::String),
    };
    let row = runtime_result_identity_bytes(&row_value, &row_type)?;
    let (replacement_value, replacement_type) = match schema.value_fields.first() {
        Some(output) => {
            let index = descriptor
                .field_index_by_identity(output.identity.as_ref().ok_or(
                    super::Error::InvalidStoredValue("aggregate output has no lowered identity"),
                )?)
                .ok_or(super::Error::InvalidStoredValue(
                    "aggregate app-row terminal is missing aggregate output",
                ))?;
            let field = descriptor
                .fields()
                .get(index)
                .ok_or(super::Error::InvalidStoredValue(
                    "aggregate app-row output descriptor is missing",
                ))?;
            (record.get_idx(index)?, field.value_type.clone())
        }
        None => (Value::String("empty".to_owned()), ValueType::String),
    };
    let replacement = runtime_result_identity_bytes(&replacement_value, &replacement_type)?;
    let member = ResultMemberEntry::Synthetic {
        table: "aggregate_result".to_owned(),
        row,
        replacement: SyntheticReplacementToken::from_encoded_record(replacement),
    };
    let (payload_descriptor, row_bytes) =
        super::descriptor_roles::encode_aggregate_payload_record(record, schema)?;
    let payload = ResultMemberPayloadEntry {
        member: member.clone(),
        descriptor: payload_descriptor,
        record: row_bytes,
    };
    Ok(DecodedMaintainedEvent::AggregateResult {
        member,
        payload,
        synthetic: schema.synthetic.clone(),
        value_fields: schema
            .value_fields
            .iter()
            .map(|field| {
                field
                    .name
                    .clone()
                    .expect("lowered aggregate output is named")
            })
            .collect(),
    })
}

/// Domain separation for the runtime flat-join result revision.
///
/// `ResultMemberEntry::row_digest` identifies runtime tuple replacements. Its
/// canonical identity does not inherit Rust/postcard layout. The
/// preimage is a V1 envelope containing a canonical Groove descriptor with
/// engine-owned ordinal field names and one canonical record under that exact
/// descriptor. The descriptor carries every declared field type (including
/// nested enum registry identity); ordinal names make user aliases irrelevant.
const FLAT_JOIN_ROW_DIGEST_DOMAIN: &str = "jazz.flat-join-row-digest.v1";
const FLAT_JOIN_ROW_DIGEST_MAGIC: &[u8; 4] = b"JFRD";
const FLAT_JOIN_ROW_DIGEST_VERSION: u8 = 1;

fn flat_join_row_digest(
    fields: &[TypedOutputField],
    values: &[Value],
) -> Result<Vec<u8>, super::Error> {
    let bytes = flat_join_row_digest_preimage(fields, values)?;
    Ok(blake3::derive_key(FLAT_JOIN_ROW_DIGEST_DOMAIN, &bytes).to_vec())
}

fn flat_join_row_digest_preimage(
    fields: &[TypedOutputField],
    values: &[Value],
) -> Result<Vec<u8>, super::Error> {
    if fields.len() != values.len() {
        return Err(super::Error::InvalidStoredValue(
            "flat joined result revision field/value arity disagrees",
        ));
    }
    let descriptor = RecordDescriptor::new(
        fields
            .iter()
            .enumerate()
            .map(|(index, field)| (format!("flat_join_payload_{index}"), field.ty.clone())),
    );
    let descriptor_bytes = groove::records::encode_persisted_record_descriptor(&descriptor)?;
    // The public payload descriptor is the runtime revision contract. An inner join may
    // nevertheless tighten a proven-present `Nullable(T)` runtime field to
    // `T` before the terminal sees it. Restore that wrapper here so the same
    // logical tuple gets one digest regardless of that execution detail.
    let values = values
        .iter()
        .cloned()
        .zip(fields)
        .map(|(value, field)| canonicalize_flat_join_payload_value(value, &field.ty))
        .collect::<Vec<_>>();
    let record_bytes = descriptor.create(&values)?;
    let field_count = u32::try_from(fields.len()).map_err(|_| {
        super::Error::InvalidStoredValue("flat joined result revision has too many fields")
    })?;
    let descriptor_len = u32::try_from(descriptor_bytes.len()).map_err(|_| {
        super::Error::InvalidStoredValue("flat joined result revision descriptor is too large")
    })?;
    let record_len = u32::try_from(record_bytes.len()).map_err(|_| {
        super::Error::InvalidStoredValue("flat joined result revision record is too large")
    })?;
    let mut bytes = Vec::with_capacity(4 + 1 + 12 + descriptor_bytes.len() + record_bytes.len());
    bytes.extend_from_slice(FLAT_JOIN_ROW_DIGEST_MAGIC);
    bytes.push(FLAT_JOIN_ROW_DIGEST_VERSION);
    bytes.extend_from_slice(&field_count.to_be_bytes());
    bytes.extend_from_slice(&descriptor_len.to_be_bytes());
    bytes.extend_from_slice(&descriptor_bytes);
    bytes.extend_from_slice(&record_len.to_be_bytes());
    bytes.extend_from_slice(&record_bytes);
    Ok(bytes)
}

fn canonicalize_flat_join_payload_value(value: Value, target: &ValueType) -> Value {
    match (value, target) {
        (Value::Nullable(None), ValueType::Nullable(_)) => Value::Nullable(None),
        (Value::Nullable(Some(value)), ValueType::Nullable(inner)) => {
            let value = canonicalize_flat_join_payload_value(*value, inner);
            // A relation carrier can add one nullable layer around a null
            // authored value. A `Nullable(T)` public field has only one such
            // layer, so collapse that extra present-null carrier.
            if matches!(value, Value::Nullable(None))
                && !matches!(inner.as_ref(), ValueType::Nullable(_))
            {
                Value::Nullable(None)
            } else {
                Value::Nullable(Some(Box::new(value)))
            }
        }
        // Runtime lowering may unwrap a nullable join key after proving it is
        // present. Persisted payload identity remains expressed in the public
        // nullable descriptor, not in that temporary tightened layout.
        (value, ValueType::Nullable(inner)) => Value::Nullable(Some(Box::new(
            canonicalize_flat_join_payload_value(value, inner),
        ))),
        // Conversely, a runtime source can retain an optional carrier around
        // a field whose public flat-join projection is proven non-null. Only a
        // present wrapper is equivalent; `None` deliberately remains invalid
        // for a non-null declared field and is rejected by `descriptor.create`.
        (Value::Nullable(Some(value)), target) => {
            canonicalize_flat_join_payload_value(*value, target)
        }
        (Value::Array(values), ValueType::Array(inner)) => Value::Array(
            values
                .into_iter()
                .map(|value| canonicalize_flat_join_payload_value(value, inner))
                .collect(),
        ),
        (Value::Tuple(values), ValueType::Tuple(types)) if values.len() == types.len() => {
            Value::Tuple(
                values
                    .into_iter()
                    .zip(types)
                    .map(|(value, target)| canonicalize_flat_join_payload_value(value, target))
                    .collect(),
            )
        }
        (value, _) => value,
    }
}

fn decode_typed_relation_edge(
    record: BorrowedRecord<'_>,
    schema: &RelationEdgeSchema,
    tables: &TableSchemas,
    node_aliases: &BTreeMap<NodeUuid, NodeAlias>,
) -> Result<RelationEdgeEntry, super::Error> {
    let source_table = table_name_from_versioned_ref(record, &schema.source, tables)?;
    let target_table = table_name_from_versioned_ref(record, &schema.target, tables)?;
    let path = match record.get_idx(field_idx(record, &schema.path_field)?)? {
        Value::String(value) => value,
        _ => {
            return Err(super::Error::InvalidStoredValue(
                "relation edge path field must be string",
            ));
        }
    };
    Ok(RelationEdgeEntry {
        path,
        source_table: source_table.clone().into(),
        source_row: RowUuid(record.get_uuid(field_idx(record, &schema.source.row.row_field)?)?),
        target_table: target_table.clone().into(),
        target_row: RowUuid(record.get_uuid(field_idx(record, &schema.target.row.row_field)?)?),
        kind: Some(crate::protocol::RelationEdgeKind::Relation),
        source_version: decode_relation_edge_version(record, &schema.source, node_aliases)?,
        target_version: decode_relation_edge_version(record, &schema.target, node_aliases)?,
        depth: None,
        edge_id: None,
        branch: None,
        role: Some(crate::protocol::RelationEdgeRole::Terminal),
        order: None,
        hole_state: None,
    })
}

fn table_name_from_versioned_ref(
    record: BorrowedRecord<'_>,
    schema: &VersionedRowRefSchema,
    tables: &TableSchemas,
) -> Result<String, super::Error> {
    let table_name = match record.get_idx(field_idx(record, &schema.row.table_field)?)? {
        Value::String(value) => value,
        _ => {
            return Err(super::Error::InvalidStoredValue(
                "relation edge table field must be string",
            ));
        }
    };
    tables
        .get(&table_name)
        .ok_or(super::Error::InvalidStoredValue(
            "relation edge table_name must exist",
        ))?;
    Ok(table_name)
}

fn decode_relation_edge_version(
    record: BorrowedRecord<'_>,
    schema: &VersionedRowRefSchema,
    node_aliases: &BTreeMap<NodeUuid, NodeAlias>,
) -> Result<Option<RowVersionRefEntry>, super::Error> {
    let Some(ResultMembershipVersionSchema::Content(version)) = &schema.version else {
        return Ok(None);
    };
    let tx_time = TxTime(record_u64(record, &version.tx_time_field)?);
    let tx_node_alias = NodeAlias(record_u64(record, &version.tx_node_field)?);
    let tx_node = node_aliases
        .iter()
        .find_map(|(node, alias)| (*alias == tx_node_alias).then_some(*node))
        .ok_or(super::Error::InvalidStoredValue(
            "relation edge tx node alias must exist",
        ))?;
    let branch_or_prefix = schema
        .branch_or_prefix_field
        .as_deref()
        .map(|field| match record.get_idx(field_idx(record, field)?)? {
            Value::Uuid(value) => Ok(value.as_bytes().to_vec()),
            Value::Bytes(value) => Ok(value),
            Value::Nullable(Some(value)) => match *value {
                Value::Uuid(value) => Ok(value.as_bytes().to_vec()),
                Value::Bytes(value) => Ok(value),
                _ => Err(super::Error::InvalidStoredValue(
                    "relation edge branch discriminator must be UUID or bytes",
                )),
            },
            Value::Nullable(None) => Ok(Vec::new()),
            _ => Err(super::Error::InvalidStoredValue(
                "relation edge branch discriminator must be UUID or bytes",
            )),
        })
        .transpose()?
        .filter(|bytes| !bytes.is_empty());
    Ok(Some(RowVersionRefEntry {
        tx: TxId::new(tx_time, tx_node),
        schema_version: None,
        layer: ResultRowLayer::Content,
        batch: None,
        branch_or_prefix,
        row_digest: None,
    }))
}

fn validate_witness_event_kind(
    record: BorrowedRecord<'_>,
    expected: &str,
) -> Result<(), super::Error> {
    match record.get_idx(field_idx(record, "event_kind")?)? {
        Value::String(value) if value == expected => Ok(()),
        Value::String(_) => Err(super::Error::InvalidStoredValue(
            "maintained witness event kind did not match query-engine terminal schema",
        )),
        _ => Err(super::Error::InvalidStoredValue(
            "maintained witness event kind must be string",
        )),
    }
}

#[cfg_attr(
    feature = "cold-settle-attribution",
    tracing::instrument(skip_all, name = "cold.phase.decode_version_witness")
)]
fn decode_typed_version_witness(
    record: BorrowedRecord<'_>,
    schema: &VersionWitnessSchema,
    tables: &TableSchemas,
    decode_plan_cache: &mut VersionDecodePlanCache,
) -> Result<VersionRow, super::Error> {
    let table_name = match record.get_idx(field_idx(record, &schema.identity.table_field)?)? {
        Value::String(value) => value,
        _ => {
            return Err(super::Error::InvalidStoredValue(
                "maintained witness table field must be string",
            ));
        }
    };
    let table = tables
        .get(&table_name)
        .ok_or(super::Error::InvalidStoredValue(
            "maintained witness table_name must exist",
        ))?;
    let deletion = tagged_deletion(record.get_idx(field_idx(record, &schema.deletion_field)?)?)?;
    let layer = if deletion.is_some() {
        VersionLayer::Deletion
    } else {
        VersionLayer::Content
    };
    let cache_key = (table.name.clone(), layer);
    if !decode_plan_cache.contains_key(&cache_key) {
        let plan = build_version_decode_plan(record.descriptor(), schema, table, layer)?;
        decode_plan_cache.insert(cache_key.clone(), plan);
    }
    let plan = decode_plan_cache
        .get(&cache_key)
        .expect("version decode plan was just inserted");
    let tx_time = TxTime(record_u64_idx(record, plan.tx_time_idx)?);
    let branch_key = match plan.branch_idx {
        Some(idx) => match record.get_idx(idx)? {
            Value::Bytes(bytes) => RuntimeSchema::decode_persisted_branch_key(table, &bytes)
                .map_err(|_| {
                    super::Error::InvalidStoredValue("maintained witness branch key is invalid")
                })?,
            Value::Nullable(None) => BranchKey::default(),
            Value::Nullable(Some(value)) => match *value {
                Value::Bytes(bytes) => RuntimeSchema::decode_persisted_branch_key(table, &bytes)
                    .map_err(|_| {
                        super::Error::InvalidStoredValue("maintained witness branch key is invalid")
                    })?,
                _ => return Err(super::Error::InvalidStoredValue("branch key must be bytes")),
            },
            _ => return Err(super::Error::InvalidStoredValue("branch key must be bytes")),
        },
        None => BranchKey::default(),
    };
    let authored_columns = if layer == VersionLayer::Content {
        nullable_value(record.get_idx(plan.authored_columns_idx)?)?
            .map(authored_column_ids_from_value)
            .transpose()?
    } else {
        None
    };
    let parts = VersionRowParts {
        table: table.name.clone(),
        branch_key,
        row_uuid: RowUuid(record.get_uuid(plan.row_idx)?),
        tx_node_alias: NodeAlias(record_u64_idx(record, plan.tx_node_idx)?),
        schema_version_alias: crate::ids::SchemaVersionAlias(record_u64_idx(
            record,
            plan.schema_version_idx,
        )?),
        tx_time,
        parents: tx_ids_from_value(record.get_idx(plan.parents_idx)?)?,
        created_by: RowAuthor::from_record(record.get_record(plan.created_by_idx)?)
            .map_err(|_| groove::records::Error::NonCanonicalRecord)?
            .as_author_subject(),
        // Current-row provenance is public Unix milliseconds. Witness state
        // needs the corresponding history form only to identify/materialize
        // the authored version, whose provenance HLC always has counter zero.
        created_at: TxTime::from_physical_ms(record_u64_idx(record, plan.created_at_idx)?)
            .map_err(|_| {
                super::Error::InvalidStoredValue(
                    "maintained witness created_at_ms exceeds packed HLC range",
                )
            })?,
        updated_by: RowAuthor::from_record(record.get_record(plan.updated_by_idx)?)
            .map_err(|_| groove::records::Error::NonCanonicalRecord)?
            .as_author_subject(),
        updated_at: TxTime::from_physical_ms(record_u64_idx(record, plan.updated_at_idx)?)
            .map_err(|_| {
                super::Error::InvalidStoredValue(
                    "maintained witness updated_at_ms exceeds packed HLC range",
                )
            })?,
        cells: BTreeMap::new(),
        authored_columns,
        deletion,
    };
    let values = if layer == VersionLayer::Content {
        history_values_from_parts(table, &parts)?
    } else {
        register_values_from_parts(&parts)?
    };
    // Query witnesses already contain encoded nullable user cells. Copy those
    // fields into the history layout instead of allocating a cells map, cloning
    // its values, and encoding them again. Metadata still follows the existing
    // normalization path (in particular author admission and packed timestamps).
    let raw = plan.descriptor.create_with_encoded_fields::<super::Error>(
        record.raw().len(),
        |index, output| {
            if layer == VersionLayer::Content && index >= 10 && index < 10 + table.columns.len() {
                let source_index = plan.user_indices[&table.columns[index - 10].name];
                if record.descriptor().fields()[source_index].value_type
                    == plan.descriptor.fields()[index].value_type
                {
                    let span = record.descriptor().field_span(record.raw(), source_index)?;
                    output.extend_from_slice(&record.raw()[span]);
                    return Ok(());
                }
                let value = record.get_idx(source_index)?;
                nullable_value(value.clone())?;
                plan.descriptor.encode_field_into(index, &value, output)?;
            } else {
                plan.descriptor
                    .encode_field_into(index, &values[index], output)?;
            }
            Ok(())
        },
    )?;
    #[cfg(test)]
    let mut parts = parts;
    #[cfg(test)]
    {
        // Internal byte-equivalence oracle: public query equality would not
        // detect a change to the immutable history record's exact encoding.
        let reference_parts = &mut parts;
        if layer == VersionLayer::Content {
            for column in &table.columns {
                if let Some(value) =
                    nullable_value(record.get_idx(plan.user_indices[&column.name])?)?
                {
                    reference_parts.cells.insert(column.name.clone(), value);
                }
            }
        }
        let reference_values = if layer == VersionLayer::Content {
            history_values_from_parts(table, reference_parts)?
        } else {
            register_values_from_parts(reference_parts)?
        };
        assert_eq!(raw, plan.descriptor.create(&reference_values)?);
    }
    let version = VersionRow {
        table: groove::Intern::new(parts.table),
        branch_key: parts.branch_key,
        record: OwnedRecord::new(raw, plan.descriptor),
    };
    version.validate_canonical()?;
    Ok(version)
}

fn build_version_decode_plan(
    terminal_descriptor: RecordDescriptor,
    schema: &VersionWitnessSchema,
    table: &TableSchema,
    layer: VersionLayer,
) -> Result<VersionDecodePlan, super::Error> {
    let descriptor = if layer == VersionLayer::Deletion {
        table.register_storage_table().record_schema()
    } else {
        table.history_storage_table().record_schema()
    };
    let branch_idx = schema
        .identity
        .branch_or_prefix_field
        .as_ref()
        .map(|field| field_idx_in_descriptor(terminal_descriptor, field))
        .transpose()?;
    let user_indices = if layer == VersionLayer::Content {
        schema
            .user_fields
            .iter()
            .map(|(column, field)| {
                Ok((
                    column.clone(),
                    field_idx_in_descriptor(terminal_descriptor, field)?,
                ))
            })
            .collect::<Result<_, super::Error>>()?
    } else {
        BTreeMap::new()
    };
    Ok(VersionDecodePlan {
        descriptor,
        branch_idx,
        row_idx: field_idx_in_descriptor(terminal_descriptor, &schema.identity.row_field)?,
        tx_time_idx: field_idx_in_descriptor(terminal_descriptor, &schema.identity.tx_time_field)?,
        tx_node_idx: field_idx_in_descriptor(terminal_descriptor, &schema.identity.tx_node_field)?,
        schema_version_idx: field_idx_in_descriptor(
            terminal_descriptor,
            &schema.identity.schema_field,
        )?,
        parents_idx: field_idx_in_descriptor(terminal_descriptor, &schema.parents_field)?,
        created_by_idx: field_idx_in_descriptor(terminal_descriptor, &schema.created_by_field)?,
        created_at_idx: field_idx_in_descriptor(terminal_descriptor, &schema.created_at_field)?,
        updated_by_idx: field_idx_in_descriptor(terminal_descriptor, &schema.updated_by_field)?,
        updated_at_idx: field_idx_in_descriptor(terminal_descriptor, &schema.updated_at_field)?,
        user_indices,
        authored_columns_idx: field_idx_in_descriptor(
            terminal_descriptor,
            &schema.authored_columns_field,
        )?,
    })
}

fn tagged_deletion(value: Value) -> Result<Option<crate::tx::DeletionEvent>, super::Error> {
    match value {
        Value::Nullable(None) => Ok(None),
        Value::Nullable(Some(value)) => {
            let value = match *value {
                Value::U8(discriminant) => Value::EnumTag(discriminant),
                value => value,
            };
            deletion_event_from_value(value).map(Some)
        }
        _ => Err(super::Error::InvalidStoredValue(
            "tagged _deletion must be nullable",
        )),
    }
}

fn record_u64(record: BorrowedRecord<'_>, field: &str) -> Result<u64, super::Error> {
    match record.get_idx(field_idx(record, field)?)? {
        Value::U64(value) => Ok(value),
        _ => Err(super::Error::InvalidStoredValue("field must be u64")),
    }
}

fn record_u64_idx(record: BorrowedRecord<'_>, field_idx: usize) -> Result<u64, super::Error> {
    match record.get_idx(field_idx)? {
        Value::U64(value) => Ok(value),
        _ => Err(super::Error::InvalidStoredValue("field must be u64")),
    }
}

fn nullable_u64(record: BorrowedRecord<'_>, field: &str) -> Result<Option<u64>, super::Error> {
    match record.get_idx(field_idx(record, field)?)? {
        Value::Nullable(None) => Ok(None),
        Value::Nullable(Some(value)) => match *value {
            Value::U64(value) => Ok(Some(value)),
            _ => Err(super::Error::InvalidStoredValue(
                "nullable field payload must be u64",
            )),
        },
        Value::U64(value) => Ok(Some(value)),
        _ => Err(super::Error::InvalidStoredValue(
            "field must be nullable u64",
        )),
    }
}

fn field_idx(record: BorrowedRecord<'_>, field: &str) -> Result<usize, super::Error> {
    record
        .descriptor()
        .field_index(field)
        .ok_or(super::Error::InvalidStoredValue(
            "maintained view terminal missing field",
        ))
}

fn field_idx_in_descriptor(
    descriptor: RecordDescriptor,
    field: &str,
) -> Result<usize, super::Error> {
    descriptor
        .field_index(field)
        .ok_or(super::Error::InvalidStoredValue(
            "maintained view terminal missing field",
        ))
}

impl WeightedVersionIndex {
    fn footprint_bytes(&self) -> usize {
        btree_map_bytes(self.by_tx.len()) + btree_map_bytes(self.entry_count) + self.entry_bytes
    }

    fn apply_delta(&mut self, payload: Arc<VersionPayload>, weight: i64) {
        use std::collections::btree_map::Entry;

        let tx_id = payload.tx_id;
        let rows = match self.by_tx.entry(tx_id) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) if weight > 0 => entry.insert(BTreeMap::new()),
            Entry::Vacant(_) => return,
        };
        match rows.entry(payload.sort_key.clone()) {
            Entry::Occupied(mut entry) => {
                let new = entry.get().weight + weight;
                self.entry_bytes -= weighted_version_bytes(entry.get());
                if new > 0 {
                    let version = WeightedVersion {
                        payload,
                        weight: new,
                    };
                    self.entry_bytes += weighted_version_bytes(&version);
                    entry.insert(version);
                } else {
                    self.entry_bytes -= version_sort_key_bytes(entry.key());
                    self.entry_count -= 1;
                    entry.remove();
                }
            }
            Entry::Vacant(entry) if weight > 0 => {
                let version = WeightedVersion { payload, weight };
                self.entry_bytes +=
                    version_sort_key_bytes(entry.key()) + weighted_version_bytes(&version);
                self.entry_count += 1;
                entry.insert(version);
            }
            Entry::Vacant(_) => {}
        }
        if rows.is_empty() {
            self.by_tx.remove(&tx_id);
        }
    }

    fn rows_by_tx(&self, tx_id: TxId) -> impl Iterator<Item = &VersionRow> {
        self.by_tx
            .get(&tx_id)
            .into_iter()
            .flat_map(|rows| rows.values())
            .map(|version| &version.row)
    }

    fn versions_by_tx(&self, tx_id: TxId) -> Vec<VersionRow> {
        self.rows_by_tx(tx_id).cloned().collect()
    }
}

impl ReplacementIndex {
    fn footprint_bytes(&self) -> usize {
        btree_map_bytes(self.content_by_key.len() + self.deletion_by_key.len())
            + btree_map_bytes(self.entry_count)
            + self.key_bytes
            + self.entry_bytes
    }

    fn apply_delta(
        &mut self,
        key: ReplacementKey,
        identity: VersionIdentity,
        payload: Arc<VersionPayload>,
        weight: i64,
    ) {
        let by_key = match key.layer {
            VersionLayer::Content => &mut self.content_by_key,
            VersionLayer::Deletion => &mut self.deletion_by_key,
        };
        let row_versions = by_key.entry(key.clone()).or_default();
        let was_empty = row_versions.is_empty();
        use std::collections::btree_map::Entry;
        match row_versions.entry(identity) {
            Entry::Occupied(mut entry) => {
                let new = entry.get().weight + weight;
                self.entry_bytes -= weighted_version_bytes(entry.get());
                if new > 0 {
                    let version = WeightedVersion {
                        payload,
                        weight: new,
                    };
                    self.entry_bytes += weighted_version_bytes(&version);
                    entry.insert(version);
                } else {
                    self.entry_bytes -= version_identity_bytes(entry.key());
                    self.entry_count -= 1;
                    entry.remove();
                }
            }
            Entry::Vacant(entry) if weight > 0 => {
                let version = WeightedVersion { payload, weight };
                self.entry_bytes +=
                    version_identity_bytes(entry.key()) + weighted_version_bytes(&version);
                self.entry_count += 1;
                entry.insert(version);
            }
            Entry::Vacant(_) => {}
        }
        if was_empty && !row_versions.is_empty() {
            self.key_bytes += replacement_key_bytes(&key);
        } else if !was_empty && row_versions.is_empty() {
            self.key_bytes -= replacement_key_bytes(&key);
        }
        if row_versions.is_empty() {
            by_key.remove(&key);
        }
    }

    fn replacement_for(
        &self,
        table: &str,
        row_uuid: RowUuid,
    ) -> (Option<VersionRow>, Option<VersionRow>) {
        let table = groove::Intern::new(table.to_owned());
        let content = self.content_by_key.get(&ReplacementKey {
            table,
            row_uuid,
            layer: VersionLayer::Content,
        });
        let deletion = self.deletion_by_key.get(&ReplacementKey {
            table,
            row_uuid,
            layer: VersionLayer::Deletion,
        });
        (replacement_winner(content), replacement_winner(deletion))
    }

    fn entry_count(&self) -> usize {
        self.entry_count
    }

    #[cfg(test)]
    fn assert_footprint_matches_full_scan(&self) {
        assert_eq!(
            self.footprint_bytes(),
            replacement_map_bytes(&self.content_by_key)
                + replacement_map_bytes(&self.deletion_by_key)
        );
        assert_eq!(
            self.entry_count,
            self.content_by_key
                .values()
                .chain(self.deletion_by_key.values())
                .map(BTreeMap::len)
                .sum::<usize>()
        );
    }
}

#[cfg(test)]
fn replacement_map_bytes(
    by_key: &BTreeMap<ReplacementKey, BTreeMap<VersionIdentity, WeightedVersion>>,
) -> usize {
    btree_map_bytes(by_key.len())
        + by_key
            .iter()
            .map(|(key, row_versions)| {
                replacement_key_bytes(key)
                    + btree_map_bytes(row_versions.len())
                    + row_versions
                        .iter()
                        .map(|(identity, version)| {
                            version_identity_bytes(identity) + weighted_version_bytes(version)
                        })
                        .sum::<usize>()
            })
            .sum::<usize>()
}

fn btree_map_bytes(len: usize) -> usize {
    len * 96
}

fn btree_set_bytes(len: usize) -> usize {
    len * 64
}

fn intern_string_bytes(value: &groove::Intern<String>) -> usize {
    mem::size_of_val(value) + value.as_str().len()
}

fn vec_bytes<T>(value: &[T]) -> usize {
    mem::size_of::<Vec<T>>() + mem::size_of_val(value)
}

fn option_vec_bytes<T>(value: &Option<Vec<T>>) -> usize {
    value.as_deref().map(vec_bytes).unwrap_or_default()
}

fn result_member_entry_bytes(member: &ResultMemberEntry) -> usize {
    mem::size_of_val(member)
        + match member {
            ResultMemberEntry::Row(row) | ResultMemberEntry::TypedRow { row, .. } => {
                intern_string_bytes(&row.table)
                    + option_vec_bytes(&row.branch_or_prefix)
                    + option_vec_bytes(&row.row_digest)
            }
            ResultMemberEntry::Synthetic {
                table,
                row,
                replacement,
            } => table.len() + vec_bytes(row) + mem::size_of_val(replacement),
            ResultMemberEntry::PathTuple {
                path,
                source_table,
                target_table,
                edge_id,
                revision,
                ..
            } => {
                path.len()
                    + intern_string_bytes(source_table)
                    + intern_string_bytes(target_table)
                    + option_vec_bytes(edge_id)
                    + vec_bytes(revision)
            }
        }
}

fn result_member_payload_entry_bytes(payload: &ResultMemberPayloadEntry) -> usize {
    mem::size_of_val(payload)
        + result_member_entry_bytes(&payload.member)
        + vec_bytes(&payload.descriptor)
        + vec_bytes(&payload.record)
}

fn version_identity_bytes(identity: &VersionIdentity) -> usize {
    mem::size_of_val(identity) + intern_string_bytes(&identity.table) + identity.raw_record.len()
}

fn version_sort_key_bytes(sort_key: &VersionSortKey) -> usize {
    mem::size_of_val(sort_key) + intern_string_bytes(&sort_key.table) + sort_key.raw_record.len()
}

fn replacement_key_bytes(key: &ReplacementKey) -> usize {
    mem::size_of_val(key) + intern_string_bytes(&key.table)
}

fn weighted_version_bytes(version: &WeightedVersion) -> usize {
    mem::size_of_val(version)
        + mem::size_of::<VersionPayload>()
        + version_row_bytes(&version.row)
        + version_sort_key_bytes(&version.sort_key)
}

fn version_row_bytes(row: &VersionRow) -> usize {
    mem::size_of_val(row) + intern_string_bytes(&row.table) + row.record.raw().len()
}

impl VersionIdentity {
    fn for_row(row: &VersionRow) -> Self {
        Self {
            table: row.table,
            layer: row.layer(),
            raw_record: Arc::from(row.record.raw()),
        }
    }
}

impl VersionSortKey {
    fn for_row(row: &VersionRow, identity: &VersionIdentity) -> Self {
        Self {
            table: row.table,
            row_uuid: row.row_uuid(),
            layer: row.layer(),
            raw_record: Arc::clone(&identity.raw_record),
        }
    }
}

impl ReplacementKey {
    fn for_row(row: &VersionRow, layer: VersionLayer) -> Self {
        Self {
            table: row.table,
            row_uuid: row.row_uuid(),
            layer,
        }
    }
}

impl NetEvent {
    fn identity(&self) -> EventIdentity {
        match self {
            Self::Result(result) => EventIdentity::Result(Rc::clone(&result.0)),
            Self::AggregateResult(result) => EventIdentity::Result(Rc::clone(&result.member)),
            Self::Version(source, identity, _) => {
                EventIdentity::Version(source.clone(), identity.clone())
            }
            Self::Replacement(source, key, identity, _) => {
                EventIdentity::Replacement(source.clone(), key.clone(), identity.clone())
            }
            Self::SharedVersion(source, identity, _) => {
                EventIdentity::SharedVersion(source.clone(), identity.clone())
            }
            Self::ProgramFact(fact) => EventIdentity::ProgramFact(fact.clone()),
            Self::StructuredAppRow(root, record) => {
                EventIdentity::StructuredAppRow(*root, record.raw().to_vec())
            }
        }
    }
}

fn replacement_winner(
    versions: Option<&BTreeMap<VersionIdentity, WeightedVersion>>,
) -> Option<VersionRow> {
    let versions = versions?;
    versions
        .values()
        .filter(|version| version.weight > 0)
        .max_by_key(|version| version.tx_id)
        .map(|version| version.row.clone())
}

#[cfg(test)]
mod tests {
    use crate::node::supporting_frontier::{
        SOURCE_CLOSURE_POINT_LOOKUPS, SOURCE_CLOSURE_TRAVERSALS,
    };
    use crate::protocol::CoveredInputEntry;

    fn test_maintained() -> MaintainedSubscriptionView {
        let mut view = MaintainedSubscriptionView::default();
        view.physical_tables.insert(
            "todos".to_owned().into(),
            crate::ids::GlobalPhysicalTableId(uuid::Uuid::from_u128(1)),
        );
        view
    }

    fn physical_input(input: CoveredInputEntry) -> SupportingRow {
        SupportingRow {
            physical_table: crate::ids::GlobalPhysicalTableId(uuid::Uuid::from_u128(1)),
            version_table: input.version_table,
            row: input.source_row,
            version: input.version,
        }
    }

    use std::collections::{BTreeMap, BTreeSet};

    use groove::ivm::RecordDelta;
    use groove::records::{Value, ValueType};
    use groove::schema::ColumnType;

    use super::*;
    use crate::ids::{AuthorSubject, NodeUuid, SchemaVersionAlias};
    use crate::node::codec::{VersionRow, VersionRowParts};
    use crate::node::{Error, PhysicalColumnId};
    use crate::protocol::ResultRowEntry;
    use crate::schema::{ColumnSchema, TableSchema};
    use crate::time::TxTime;
    use crate::tx::DeletionEvent;

    fn node(byte: u8) -> NodeUuid {
        NodeUuid::from_bytes([byte; 16])
    }

    fn row(byte: u8) -> RowUuid {
        RowUuid::from_bytes([byte; 16])
    }

    fn tx(byte: u8, time: u64) -> TxId {
        TxId::new(TxTime(time), node(byte))
    }

    fn aliases() -> BTreeMap<NodeUuid, NodeAlias> {
        BTreeMap::from([(node(1), NodeAlias(10)), (node(2), NodeAlias(20))])
    }

    // Internal receipt: `row_digest` is a canonical runtime result identity, so
    // its exact bytes cannot be asserted through the public query API alone.
    #[test]
    fn flat_join_row_digest_uses_the_v1_groove_record_envelope() {
        let fields = vec![
            TypedOutputField {
                name: "ignored_alias".to_owned(),
                ty: ValueType::U64,
            },
            TypedOutputField {
                name: "title".to_owned(),
                ty: ValueType::String,
            },
        ];
        let values = vec![Value::U64(7), Value::String("blue".to_owned())];
        let preimage = flat_join_row_digest_preimage(&fields, &values).unwrap();
        let digest = flat_join_row_digest(&fields, &values).unwrap();

        // Frozen JFRD v1 receipt: magic/version, count, Groove descriptor,
        // then the record. Any source-alias rename leaves these bytes intact.
        assert_eq!(
            hex::encode(&preimage),
            "4a4652440100000002000000a8050000002a00000053000000690000009200000000000000000000000002000000120000000000000000010000000000000000010000002500000001666c61745f6a6f696e5f7061796c6f61645f300000000005000000000000000000000000120000000000000000010000000000000000010000002500000001666c61745f6a6f696e5f7061796c6f61645f31000000000a0000000000000000000000001200000000000000000000000d070000000000000002626c7565"
        );
        assert_eq!(
            hex::encode(&digest),
            "d4bacd5d453e647a4da1c55842ddbf8e39a263ceb1ddb07f3f8fac090ff9480b"
        );

        let renamed = vec![
            TypedOutputField {
                name: "different_alias".to_owned(),
                ty: ValueType::U64,
            },
            TypedOutputField {
                name: "different_title".to_owned(),
                ty: ValueType::String,
            },
        ];
        assert_eq!(flat_join_row_digest(&renamed, &values).unwrap(), digest);
        assert_ne!(
            flat_join_row_digest(&fields, &[Value::U64(7), Value::String("red".to_owned())],)
                .unwrap(),
            digest
        );
    }

    #[test]
    fn flat_join_row_digest_restores_a_proven_present_nullable_payload() {
        let fields = vec![TypedOutputField {
            name: "team_id".to_owned(),
            ty: ValueType::Nullable(Box::new(ValueType::Uuid)),
        }];
        let id = uuid::Uuid::from_bytes([0x71; 16]);

        // Inner-join lowering is permitted to use the proven-present UUID
        // directly, while the public result field remains nullable.
        let tightened = flat_join_row_digest(&fields, &[Value::Uuid(id)]).unwrap();
        let declared =
            flat_join_row_digest(&fields, &[Value::Nullable(Some(Box::new(Value::Uuid(id))))])
                .unwrap();

        assert_eq!(tightened, declared);
    }

    #[test]
    fn flat_join_row_digest_removes_a_present_runtime_nullable_carrier() {
        let fields = vec![TypedOutputField {
            name: "team_id".to_owned(),
            ty: ValueType::Uuid,
        }];
        let id = uuid::Uuid::from_bytes([0x72; 16]);

        let declared = flat_join_row_digest(&fields, &[Value::Uuid(id)]).unwrap();
        let carried =
            flat_join_row_digest(&fields, &[Value::Nullable(Some(Box::new(Value::Uuid(id))))])
                .unwrap();

        assert_eq!(declared, carried);
    }

    #[test]
    fn flat_join_row_digest_collapses_an_extra_present_null_carrier() {
        let fields = vec![TypedOutputField {
            name: "parent_id".to_owned(),
            ty: ValueType::Nullable(Box::new(ValueType::Uuid)),
        }];

        let declared = flat_join_row_digest(&fields, &[Value::Nullable(None)]).unwrap();
        let carried = flat_join_row_digest(
            &fields,
            &[Value::Nullable(Some(Box::new(Value::Nullable(None))))],
        )
        .unwrap();

        assert_eq!(declared, carried);
    }

    /// Production-shaped typed relation facts retain branch identity through
    /// decode, initial/reset installation, and ordinary removal.
    #[test]
    fn typed_branch_relation_edge_decodes_adds_and_removes_with_discriminator() {
        use crate::node::query_engine::{ContentVersionFields, RowRefSchema};

        let descriptor = RecordDescriptor::new([
            ("source_table", ValueType::String),
            ("source_row", ValueType::Uuid),
            ("source_tx_time", ValueType::U64),
            ("source_tx_node_id", ValueType::U64),
            ("source_branch_or_prefix", ValueType::Uuid),
            ("path", ValueType::String),
            ("target_table", ValueType::String),
            ("target_row", ValueType::Uuid),
            ("target_tx_time", ValueType::U64),
            ("target_tx_node_id", ValueType::U64),
            ("target_branch_or_prefix", ValueType::Uuid),
        ]);
        let branch = uuid::Uuid::from_bytes([0xb1; 16]);
        let raw = descriptor
            .create(&[
                Value::String("posts".to_owned()),
                Value::Uuid(row(0xb2).0),
                Value::U64(11),
                Value::U64(10),
                Value::Uuid(branch),
                Value::String("author".to_owned()),
                Value::String("users".to_owned()),
                Value::Uuid(row(0xb3).0),
                Value::U64(12),
                Value::U64(10),
                Value::Uuid(branch),
            ])
            .expect("encode typed branch relation edge");
        let versioned = |prefix: &str| VersionedRowRefSchema {
            row: RowRefSchema {
                source_field: format!("{prefix}_source"),
                table_field: format!("{prefix}_table"),
                row_field: format!("{prefix}_row"),
            },
            version: Some(ResultMembershipVersionSchema::Content(
                ContentVersionFields {
                    tx_time_field: format!("{prefix}_tx_time"),
                    tx_node_field: format!("{prefix}_tx_node_id"),
                },
            )),
            branch_or_prefix_field: Some(format!("{prefix}_branch_or_prefix")),
        };
        let schema = RelationEdgeSchema {
            source: versioned("source"),
            path_field: "path".to_owned(),
            target: versioned("target"),
            kind_field: "kind".to_owned(),
            depth_field: None,
            edge_id_field: None,
            branch_field: None,
            role_field: None,
            order_field: None,
            hole_state_field: None,
        };
        let tables = BTreeMap::from([
            (
                "posts".to_owned(),
                TableSchema::new("posts", [ColumnSchema::new("title", ColumnType::String)]),
            ),
            (
                "users".to_owned(),
                TableSchema::new("users", [ColumnSchema::new("name", ColumnType::String)]),
            ),
        ]);
        let edge = decode_typed_relation_edge(
            BorrowedRecord::new(&raw, &descriptor),
            &schema,
            &tables,
            &aliases(),
        )
        .expect("decode production-shaped branch edge");
        assert_eq!(
            edge.target_version
                .as_ref()
                .and_then(|version| version.branch_or_prefix.as_deref()),
            Some(branch.as_bytes().as_slice())
        );

        let fact = ProgramFactEntry::RelationEdge(edge);
        let mut maintained = test_maintained();
        let reset = maintained
            .apply_decoded_deltas(
                [(
                    DecodedMaintainedEvent::RelationEdge(match &fact {
                        ProgramFactEntry::RelationEdge(edge) => edge.clone(),
                        _ => unreachable!(),
                    }),
                    1,
                )],
                &aliases(),
            )
            .expect("install reset edge");
        assert_eq!(reset.program_fact_adds, vec![fact.clone()]);
        let remove = maintained
            .apply_decoded_deltas(
                [(
                    DecodedMaintainedEvent::RelationEdge(match &fact {
                        ProgramFactEntry::RelationEdge(edge) => edge.clone(),
                        _ => unreachable!(),
                    }),
                    -1,
                )],
                &aliases(),
            )
            .expect("remove branch edge");
        assert_eq!(remove.program_fact_removes, vec![fact]);
    }

    #[test]
    fn terminal_layout_includes_nested_public_slots_and_excludes_hidden_routes() {
        let descriptor = RecordDescriptor::new([
            ("row_uuid", ValueType::Uuid),
            (
                "user_title",
                ValueType::Nullable(Box::new(ValueType::String)),
            ),
            (
                "user___jazz_include_project",
                ValueType::Nullable(Box::new(ValueType::String)),
            ),
            (
                "__jazz_include_project",
                ValueType::Array(Box::new(ValueType::Record(Box::new(
                    RecordDescriptor::new([
                        ("row_uuid", ValueType::Uuid),
                        ("title", ValueType::String),
                    ]),
                )))),
            ),
            ("__route_org", ValueType::Uuid),
        ]);
        let rows = AppRowSchema {
            publication_fields: BTreeMap::from([
                (
                    "user_title".to_owned(),
                    crate::node::CurrentRowPublicationField::StoredColumn {
                        id: crate::ids::PhysicalColumnId(1),
                        output_name: "title".to_owned(),
                    },
                ),
                (
                    "user___jazz_include_project".to_owned(),
                    crate::node::CurrentRowPublicationField::StoredColumn {
                        id: crate::ids::PhysicalColumnId(2),
                        output_name: "__jazz_include_project".to_owned(),
                    },
                ),
                (
                    "__jazz_include_project".to_owned(),
                    crate::node::CurrentRowPublicationField::ResultField {
                        name: "project".to_owned(),
                        visibility: crate::node::CurrentRowResultVisibility::ApplicationCell,
                    },
                ),
            ]),
            descriptor: descriptor.clone(),
            hidden_fields: BTreeSet::from(["__route_org".to_owned()]),
            carrier: AppRowCarrier::Logical,
            field_carriers: BTreeMap::from([
                ("user_title".to_owned(), AppRowCarrier::CurrentRow),
                (
                    "user___jazz_include_project".to_owned(),
                    AppRowCarrier::CurrentRow,
                ),
                ("__jazz_include_project".to_owned(), AppRowCarrier::Logical),
            ]),
            public_field_names: BTreeMap::from([
                ("user_title".to_owned(), "title".to_owned()),
                (
                    "user___jazz_include_project".to_owned(),
                    "__jazz_include_project".to_owned(),
                ),
                ("__jazz_include_project".to_owned(), "project".to_owned()),
            ]),
            terminal: crate::node::query_engine::AppRowTerminal::RootCollector,
            root_union_arm: false,
        };
        let layout = terminal_root_layout(&rows);
        assert_eq!(
            layout.public_fields,
            vec![
                TerminalRootPublicField {
                    publication: crate::node::CurrentRowPublicationField::StoredColumn {
                        id: crate::ids::PhysicalColumnId(1),
                        output_name: "title".to_owned()
                    },
                    name: "title".to_owned(),
                    descriptor_field_name: "user_title".to_owned(),
                    slot: 1,
                    carrier: TerminalRootCarrier::CurrentRow,
                },
                TerminalRootPublicField {
                    publication: crate::node::CurrentRowPublicationField::StoredColumn {
                        id: crate::ids::PhysicalColumnId(2),
                        output_name: "__jazz_include_project".to_owned()
                    },
                    name: "__jazz_include_project".to_owned(),
                    descriptor_field_name: "user___jazz_include_project".to_owned(),
                    slot: 2,
                    carrier: TerminalRootCarrier::CurrentRow,
                },
                TerminalRootPublicField {
                    publication: crate::node::CurrentRowPublicationField::ResultField {
                        name: "project".to_owned(),
                        visibility: crate::node::CurrentRowResultVisibility::ApplicationCell
                    },
                    name: "project".to_owned(),
                    descriptor_field_name: "__jazz_include_project".to_owned(),
                    slot: 3,
                    carrier: TerminalRootCarrier::Logical,
                },
            ]
        );

        let mut without_nested = rows;
        without_nested
            .hidden_fields
            .insert("__jazz_include_project".to_owned());
        assert_ne!(layout.id, terminal_root_layout(&without_nested).id);
    }

    fn layout(descriptor: RecordDescriptor) -> TerminalRootLayout {
        TerminalRootLayout {
            id: "test-layout".to_owned(),
            root_key_slot: 0,
            root_key_field_name: "row_uuid".to_owned(),
            root_descriptor: descriptor,
            public_fields: Vec::new(),
            carrier: TerminalRootCarrier::Logical,
            root_union_arm: false,
        }
    }

    // This reducer test deliberately works below the public query API. The
    // runtime hands it opaque terminal keys after CollectBy has already
    // applied sort/window semantics, and a public root UUID cannot express
    // two flat occurrences of that root with different joined payloads.
    #[test]
    fn fresh_collector_roots_skip_order_scans_but_reinsert_repositions() {
        // Internal mechanism test: the public result cannot reveal a scan of
        // every existing key for each fresh insert. Also pin reinsertion's
        // occurrence identity and position, so skipping all scans is unsafe.
        let descriptor = RecordDescriptor::new([("row_uuid", ValueType::Uuid)]);
        let root = row(0x82);
        let value = descriptor.create(&[Value::Uuid(root.0)]).unwrap();
        let mut view = test_maintained();
        let insert = |i: u64, index| {
            let key = i.to_be_bytes().to_vec();
            TerminalOperation {
                root_descriptor: descriptor,
                root_key: key.clone(),
                path: Vec::new(),
                edit: TerminalEdit::Insert {
                    key,
                    index,
                    value: value.clone(),
                },
            }
        };
        for i in 0..2000 {
            view.apply_structured_terminal_operation(&insert(i, i as usize))
                .unwrap();
        }
        assert_eq!(view.root_order_insert_comparisons, 0);
        assert_eq!(view.structured_root_key_order.len(), 2000);
        // The same public root is allowed at many occurrence keys. Reinsert
        // only one occurrence, replacing its position without duplicating it.
        view.apply_structured_terminal_operation(&insert(999, 0))
            .unwrap();
        assert_eq!(view.structured_root_key_order.len(), 2000);
        assert_eq!(view.structured_root_key_order[0], 999_u64.to_be_bytes());
        assert_eq!(view.structured_root_key_order[1], 0_u64.to_be_bytes());
        assert_eq!(
            view.structured_root_key_order
                .iter()
                .filter(|key| **key == 999_u64.to_be_bytes())
                .count(),
            1
        );
    }

    #[test]
    fn collector_terminal_keys_preserve_same_root_payloads_order_and_edits() {
        let descriptor =
            RecordDescriptor::new([("row_uuid", ValueType::Uuid), ("title", ValueType::String)]);
        let root = row(0x81);
        let first_key = vec![0x10, 0x01];
        let second_key = vec![0x10, 0x02];
        let record = |title: &str| {
            descriptor
                .create(&[Value::Uuid(root.0), Value::String(title.to_owned())])
                .unwrap()
        };
        let insert = |key: Vec<u8>, index, value| TerminalOperation {
            root_descriptor: descriptor,
            root_key: key.clone(),
            path: Vec::new(),
            edit: TerminalEdit::Insert { key, index, value },
        };
        let update = |key: Vec<u8>, value| TerminalOperation {
            root_descriptor: descriptor,
            root_key: key.clone(),
            path: Vec::new(),
            edit: TerminalEdit::Update { key, value },
        };
        let mut maintained = test_maintained();

        // These indices are the already-lowered CollectBy order (for example
        // a query's custom sort after offset/limit), not map-key order.
        maintained
            .apply_structured_terminal_operation(&insert(second_key.clone(), 0, record("second")))
            .unwrap();
        maintained
            .apply_structured_terminal_operation(&insert(first_key.clone(), 1, record("first")))
            .unwrap();
        let titles = |view: &MaintainedSubscriptionView| {
            view.structured_app_rows_by_terminal_key()
                .unwrap()
                .into_iter()
                .map(|(key, row)| {
                    let Value::String(title) = row.borrowed().get("title").unwrap() else {
                        panic!("test record keeps a string title");
                    };
                    (key, title)
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(
            titles(&maintained),
            vec![
                (second_key.clone(), "second".to_owned()),
                (first_key.clone(), "first".to_owned()),
            ]
        );

        // An occurrence-local replacement must leave its same-root sibling
        // intact, then its move must retain the collector's declared order.
        maintained
            .apply_structured_terminal_operation(&update(
                first_key.clone(),
                record("first updated"),
            ))
            .unwrap();
        maintained
            .apply_structured_terminal_operation(&TerminalOperation {
                root_descriptor: descriptor,
                root_key: first_key.clone(),
                path: Vec::new(),
                edit: TerminalEdit::Move {
                    key: first_key.clone(),
                    index: 0,
                },
            })
            .unwrap();
        assert_eq!(
            titles(&maintained),
            vec![
                (first_key.clone(), "first updated".to_owned()),
                (second_key.clone(), "second".to_owned()),
            ]
        );

        maintained
            .apply_structured_terminal_operation(&TerminalOperation {
                root_descriptor: descriptor,
                root_key: first_key.clone(),
                path: Vec::new(),
                edit: TerminalEdit::Remove { key: first_key },
            })
            .unwrap();
        assert_eq!(titles(&maintained), vec![(second_key, "second".to_owned())]);
    }

    #[test]
    fn terminal_operation_rebinds_tightened_root_field_to_prepared_nullable_layout() {
        let source = RecordDescriptor::new([
            ("row_uuid", ValueType::Uuid),
            ("user_child", ValueType::Uuid),
        ]);
        let target = RecordDescriptor::new([
            ("row_uuid", ValueType::Uuid),
            ("user_child", ValueType::Nullable(Box::new(ValueType::Uuid))),
        ]);
        let row_uuid = row(0x71);
        let raw = source
            .create(&[Value::Uuid(row_uuid.0), Value::Uuid(row(0x72).0)])
            .unwrap();
        let operation = TerminalOperation {
            root_descriptor: source,
            root_key: row_uuid.0.as_bytes().to_vec(),
            path: Vec::new(),
            edit: TerminalEdit::Update {
                key: row_uuid.0.as_bytes().to_vec(),
                value: raw,
            },
        };

        let rebound =
            rebind_terminal_operation_to_layout(operation.clone(), &layout(target)).unwrap();
        assert_eq!(rebound.root_descriptor, target);
        let TerminalEdit::Update { value, .. } = rebound.edit else {
            panic!("operation remains an update");
        };
        assert_eq!(
            target.bind(&value).to_values().unwrap(),
            vec![
                Value::Uuid(row_uuid.0),
                Value::Nullable(Some(Box::new(Value::Uuid(row(0x72).0)))),
            ]
        );
    }

    #[test]
    fn terminal_operation_rebinds_nested_registry_only_record_layout() {
        let source_metadata = RecordDescriptor::new([(
            "status",
            ValueType::EnumTag(
                groove::records::ScalarEnumSchema::new("status", ["open"])
                    .unwrap()
                    .with_registry_id(11),
            ),
        )]);
        let target_metadata = RecordDescriptor::new([(
            "status",
            ValueType::EnumTag(
                groove::records::ScalarEnumSchema::new("status", ["open"])
                    .unwrap()
                    .with_registry_id(22),
            ),
        )]);
        let source = RecordDescriptor::new([
            ("row_uuid", ValueType::Uuid),
            ("metadata", ValueType::Record(Box::new(source_metadata))),
        ]);
        let target = RecordDescriptor::new([
            ("row_uuid", ValueType::Uuid),
            ("metadata", ValueType::Record(Box::new(target_metadata))),
        ]);
        let row_uuid = row(0x71);
        let raw = source
            .create(&[
                Value::Uuid(row_uuid.0),
                Value::Record(OwnedRecord::new(
                    source_metadata.create(&[Value::EnumTag(0)]).unwrap(),
                    source_metadata,
                )),
            ])
            .unwrap();
        let operation = TerminalOperation {
            root_descriptor: source,
            root_key: row_uuid.0.as_bytes().to_vec(),
            path: Vec::new(),
            edit: TerminalEdit::Update {
                key: row_uuid.0.as_bytes().to_vec(),
                value: raw,
            },
        };

        let rebound =
            rebind_terminal_operation_to_layout(operation.clone(), &layout(target)).unwrap();
        let TerminalEdit::Update { value, .. } = rebound.edit else {
            panic!("operation remains an update");
        };
        let values = target.bind(&value).to_values().unwrap();
        let Value::Record(metadata) = &values[1] else {
            panic!("nested metadata remains a record");
        };
        assert_eq!(metadata.descriptor(), &target_metadata);
        assert_eq!(metadata.to_values().unwrap(), vec![Value::EnumTag(0)]);
    }

    #[test]
    fn terminal_operation_rebind_rejects_unrelated_prepared_field() {
        let source = RecordDescriptor::new([
            ("row_uuid", ValueType::Uuid),
            ("user_child", ValueType::Uuid),
        ]);
        let target = RecordDescriptor::new([
            ("row_uuid", ValueType::Uuid),
            ("user_other", ValueType::Nullable(Box::new(ValueType::Uuid))),
        ]);
        let row_uuid = row(0x71);
        let operation = TerminalOperation {
            root_descriptor: source,
            root_key: row_uuid.0.as_bytes().to_vec(),
            path: Vec::new(),
            edit: TerminalEdit::Remove {
                key: row_uuid.0.as_bytes().to_vec(),
            },
        };

        assert!(matches!(
            rebind_terminal_operation_to_layout(operation.clone(), &layout(target)),
            Err(Error::InvalidStoredValue(
                "structured terminal operation descriptor disagrees with prepared root layout"
            ))
        ));
    }

    /// Internal descriptor-boundary test: public queries cannot manufacture
    /// arbitrary terminal edits. A nullable sibling must not alter any child
    /// edit bytes, keys, or positions when the child layout is unchanged.
    #[test]
    fn nested_terminal_edits_preserve_payload_when_only_root_sibling_tightens() {
        let child =
            RecordDescriptor::new([("row_uuid", ValueType::Uuid), ("name", ValueType::String)]);
        let collection = ValueType::Array(Box::new(ValueType::Record(Box::new(child))));
        let source = RecordDescriptor::new([
            ("row_uuid", ValueType::Uuid),
            ("member_ids", ValueType::Array(Box::new(ValueType::Uuid))),
            ("members", collection.clone()),
        ]);
        let target = RecordDescriptor::new([
            ("row_uuid", ValueType::Uuid),
            (
                "member_ids",
                ValueType::Nullable(Box::new(ValueType::Array(Box::new(ValueType::Uuid)))),
            ),
            ("members", collection),
        ]);
        let key = row(0x72).0.as_bytes().to_vec();
        let value = child
            .create(&[Value::Uuid(row(0x72).0), Value::String("Alice".to_owned())])
            .unwrap();
        let edits = [
            TerminalEdit::Insert {
                index: 1,
                key: key.clone(),
                value: value.clone(),
            },
            TerminalEdit::Update {
                key: key.clone(),
                value,
            },
            TerminalEdit::Move {
                key: key.clone(),
                index: 0,
            },
            TerminalEdit::Remove { key },
        ];
        for edit in edits {
            let operation = TerminalOperation {
                root_descriptor: source,
                root_key: row(0x71).0.as_bytes().to_vec(),
                path: vec![TerminalPathSegment::Collection("members".to_owned())],
                edit,
            };
            let rebound =
                rebind_terminal_operation_to_layout(operation.clone(), &layout(target)).unwrap();
            assert_eq!(rebound.root_descriptor, target);
            assert_eq!(rebound.root_key, operation.root_key);
            assert_eq!(rebound.path, operation.path);
            assert_eq!(rebound.edit, operation.edit);
        }
    }

    /// Malformed paths and differing child layouts cannot be produced through
    /// the public query builder, so exercise their rejection at this boundary.
    #[test]
    fn nested_terminal_rebind_rejects_unknown_paths_and_changed_child_layouts() {
        let child = RecordDescriptor::new([("row_uuid", ValueType::Uuid)]);
        let source = RecordDescriptor::new([
            ("row_uuid", ValueType::Uuid),
            ("flag", ValueType::Bool),
            (
                "members",
                ValueType::Array(Box::new(ValueType::Record(Box::new(child)))),
            ),
        ]);
        let target = RecordDescriptor::new([
            ("row_uuid", ValueType::Uuid),
            ("flag", ValueType::Nullable(Box::new(ValueType::Bool))),
            (
                "members",
                ValueType::Array(Box::new(ValueType::Record(Box::new(child)))),
            ),
        ]);
        for path in [
            vec![TerminalPathSegment::Key(vec![1])],
            vec![TerminalPathSegment::Collection("missing".to_owned())],
            vec![TerminalPathSegment::Collection("flag".to_owned())],
        ] {
            let operation = TerminalOperation {
                root_descriptor: source,
                root_key: row(0x71).0.as_bytes().to_vec(),
                path,
                edit: TerminalEdit::Remove { key: vec![1] },
            };
            assert!(
                rebind_terminal_operation_to_layout(operation.clone(), &layout(target)).is_err()
            );
        }
        let other_child =
            RecordDescriptor::new([("row_uuid", ValueType::Uuid), ("name", ValueType::String)]);
        let changed_target = RecordDescriptor::new([
            ("row_uuid", ValueType::Uuid),
            ("flag", ValueType::Nullable(Box::new(ValueType::Bool))),
            (
                "members",
                ValueType::Array(Box::new(ValueType::Record(Box::new(other_child)))),
            ),
        ]);
        let operation = TerminalOperation {
            root_descriptor: source,
            root_key: row(0x71).0.as_bytes().to_vec(),
            path: vec![TerminalPathSegment::Collection("members".to_owned())],
            edit: TerminalEdit::Remove { key: vec![1] },
        };
        assert!(
            rebind_terminal_operation_to_layout(operation.clone(), &layout(changed_target))
                .is_err()
        );
    }

    fn witness_schema() -> VersionWitnessSchema {
        VersionWitnessSchema {
            source: ProgramSourceId {
                table: "todos".to_owned().into(),
                path: vec![crate::protocol::ProgramSourceRole::Root],
            },
            descriptor: RecordDescriptor::new(std::iter::empty::<(String, ValueType)>()),
            identity: crate::node::query_engine::VersionIdentityFields {
                table_field: "table".to_owned(),
                row_field: "row_uuid".to_owned(),
                tx_time_field: "tx_time".to_owned(),
                tx_node_field: "tx_node_id".to_owned(),
                batch_id_field: None,
                branch_or_prefix_field: None,
                row_digest_field: None,
                schema_field: "schema_version".to_owned(),
                layer_field: "layer".to_owned(),
            },
            created_by_field: "created_by".to_owned(),
            created_at_field: "created_at".to_owned(),
            updated_by_field: "updated_by".to_owned(),
            updated_at_field: "updated_at".to_owned(),
            parents_field: "parents".to_owned(),
            authored_columns_field: "authored_columns".to_owned(),
            deletion_field: "_deletion".to_owned(),
            user_fields: BTreeMap::new(),
        }
    }

    #[test]
    fn deletion_witnesses_force_authoritative_membership_reconciliation() {
        assert!(
            MaintainedTerminalKind::VersionDeletion(witness_schema())
                .requires_authoritative_membership_reconcile()
        );
        assert!(
            MaintainedTerminalKind::ReplacementDeletion(witness_schema())
                .requires_authoritative_membership_reconcile()
        );
        assert!(
            !MaintainedTerminalKind::VersionContent(witness_schema())
                .requires_authoritative_membership_reconcile()
        );
    }

    fn table() -> TableSchema {
        TableSchema::new("todos", [ColumnSchema::new("title", ColumnType::String)])
    }

    fn version(row_uuid: RowUuid, time: u64, title: &str) -> VersionRow {
        VersionRow::from_parts_with_schema_version(
            &table(),
            VersionRowParts {
                table: "todos".to_owned(),
                branch_key: BranchKey::default(),
                row_uuid,
                tx_node_alias: NodeAlias(10),
                schema_version_alias: SchemaVersionAlias(0),
                tx_time: TxTime(time),
                parents: Vec::new(),
                created_by: AuthorSubject::system_at(NodeUuid(uuid::Uuid::from_u128(10))),
                created_at: TxTime(time),
                updated_by: AuthorSubject::system_at(NodeUuid(uuid::Uuid::from_u128(10))),
                updated_at: TxTime(time),
                cells: BTreeMap::from([("title".to_owned(), Value::String(title.to_owned()))]),
                authored_columns: Some(BTreeSet::from([PhysicalColumnId(1)])),
                deletion: None,
            },
            None,
            None,
        )
        .unwrap()
    }

    fn deletion(row_uuid: RowUuid, time: u64) -> VersionRow {
        VersionRow::from_parts_with_schema_version(
            &table(),
            VersionRowParts {
                table: "todos".to_owned(),
                branch_key: BranchKey::default(),
                row_uuid,
                tx_node_alias: NodeAlias(10),
                schema_version_alias: SchemaVersionAlias(0),
                tx_time: TxTime(time),
                parents: Vec::new(),
                created_by: AuthorSubject::system_at(NodeUuid(uuid::Uuid::from_u128(10))),
                created_at: TxTime(time),
                updated_by: AuthorSubject::system_at(NodeUuid(uuid::Uuid::from_u128(10))),
                updated_at: TxTime(time),
                cells: BTreeMap::new(),
                authored_columns: None,
                deletion: Some(DeletionEvent::Deleted),
            },
            None,
            None,
        )
        .unwrap()
    }

    fn result(row_uuid: RowUuid, time: u64) -> ResultRowEntry {
        ("todos".to_owned().into(), row_uuid, tx(1, time))
    }

    fn test_source() -> ProgramSourceId {
        ProgramSourceId {
            table: "todos".to_owned().into(),
            path: vec![crate::protocol::ProgramSourceRole::Root],
        }
    }

    fn version_content(row: VersionRow) -> DecodedMaintainedEvent {
        DecodedMaintainedEvent::VersionContent {
            source: test_source(),
            row,
        }
    }

    fn version_deletion(row: VersionRow) -> DecodedMaintainedEvent {
        DecodedMaintainedEvent::VersionDeletion {
            source: test_source(),
            row,
        }
    }

    // Internal: publication retry candidates and lookup counts have no public
    // API representation. Compare them with the independently enumerated set.
    #[test]
    fn unpublished_source_delta_is_bounded_and_retained_until_acknowledged() {
        for size in [10, 1500] {
            let mut maintained = test_maintained();
            for time in 1..=size {
                let fact = physical_input(
                    covered_input_for_version(
                        test_source(),
                        &version(
                            RowUuid::from_bytes((time as u128).to_be_bytes()),
                            time,
                            "source",
                        ),
                        &aliases(),
                    )
                    .unwrap(),
                );
                maintained.supporting.apply(0, fact, 1);
            }
            let previous = maintained
                .supporting_rows()
                .cloned()
                .collect::<BTreeSet<_>>();
            assert!(maintained.unpublished_supporting_delta().is_none());
            maintained.acknowledge_peer_source_closure();
            let fact = previous.iter().next().unwrap().clone();
            maintained.supporting.apply(0, fact.clone(), -1);
            SOURCE_CLOSURE_POINT_LOOKUPS.with(|count| count.set(0));
            SOURCE_CLOSURE_TRAVERSALS.with(|count| count.set(0));
            let delta = maintained.unpublished_supporting_delta().unwrap();
            assert_eq!(delta, (Vec::new(), vec![fact.clone()]));
            assert_eq!(SOURCE_CLOSURE_POINT_LOOKUPS.with(|count| count.get()), 1);
            assert_eq!(SOURCE_CLOSURE_TRAVERSALS.with(|count| count.get()), 0);
            // A failed/cancelled caller does not acknowledge. A fresh drain
            // with no events must still expose the same pending removal.
            assert_eq!(maintained.unpublished_supporting_delta().unwrap(), delta);
            let current = maintained
                .supporting_rows()
                .cloned()
                .collect::<BTreeSet<_>>();
            assert_eq!(
                previous.difference(&current).cloned().collect::<Vec<_>>(),
                delta.1
            );
            maintained.acknowledge_peer_source_closure();
            assert_eq!(
                maintained.unpublished_supporting_delta(),
                Some((vec![], vec![]))
            );
        }
    }

    // Internal: exercise independent signed terminal origins and selected
    // deletion witnesses against the exact predecessor, including retries.
    #[test]
    fn unpublished_source_delta_coalesces_origins_and_selected_witnesses() {
        let version = deletion(row(1), 1);
        let fact =
            physical_input(covered_input_for_version(test_source(), &version, &aliases()).unwrap());
        let mut maintained = test_maintained();
        maintained.acknowledge_peer_source_closure();
        // Selected witnesses can change without any companion terminal event.
        maintained
            .replace_selected_deletion_witnesses(BTreeMap::from([(fact.clone(), version.clone())]));
        assert_eq!(
            maintained.unpublished_supporting_delta(),
            Some((vec![fact.clone()], vec![]))
        );
        maintained.acknowledge_peer_source_closure();
        maintained.replace_selected_deletion_witnesses(BTreeMap::new());
        assert_eq!(
            maintained.unpublished_supporting_delta(),
            Some((vec![], vec![fact.clone()]))
        );
        maintained.acknowledge_peer_source_closure();

        maintained.supporting.apply(0, fact.clone(), 1);
        maintained.supporting.apply(0, fact.clone(), -1);
        assert_eq!(
            maintained.unpublished_supporting_delta(),
            Some((vec![], vec![]))
        );
        maintained.supporting.apply(0, fact.clone(), 1);
        assert_eq!(
            maintained.unpublished_supporting_delta(),
            Some((vec![fact.clone()], vec![]))
        );
        maintained.acknowledge_peer_source_closure();
        maintained.supporting.apply(0, fact.clone(), -1);
        maintained.supporting.apply(1, fact.clone(), 1);
        assert_eq!(
            maintained.unpublished_supporting_delta(),
            Some((vec![], vec![]))
        );
        maintained.replace_selected_deletion_witnesses(BTreeMap::from([(fact.clone(), version)]));
        maintained.supporting.apply(1, fact.clone(), -1);
        assert_eq!(
            maintained.unpublished_supporting_delta(),
            Some((vec![], vec![]))
        );
        maintained.acknowledge_peer_source_closure();
        maintained.replace_selected_deletion_witnesses(BTreeMap::new());
        assert_eq!(
            maintained.unpublished_supporting_delta(),
            Some((vec![], vec![fact.clone()]))
        );
        maintained.supporting.apply(2, fact.clone(), -1);
        maintained.supporting.apply(0, fact.clone(), 1);
        assert_eq!(
            maintained.unpublished_supporting_delta(),
            Some((vec![], vec![]))
        );
    }

    // Internal layout receipt: public values cannot reveal unused enum
    // alternatives inflating every transient tree key/value allocation.
    #[test]
    fn event_map_layout_has_no_inline_large_result_or_fact_variants() {
        let key_bytes = mem::size_of::<EventIdentity>();
        let event_bytes = mem::size_of::<NetEvent>();
        eprintln!("temporary event layout: key={key_bytes} event={event_bytes}");
        assert!(key_bytes <= 128 && event_bytes <= 192);
    }

    // Internal identity ownership cannot be observed through public rows.
    // It must share allocations without changing value-based equality/order,
    // and consuming the map key must permit moving the event value back out.
    #[test]
    fn temporary_event_keys_share_values_and_release_them_before_application() {
        let member = ResultMemberEntry::from(result(row(1), 10));
        let DecodedMaintainedEvent::ResultCurrent { payload, .. } = result_current(member.clone())
        else {
            unreachable!();
        };
        let event = NetEvent::Result(Box::new((Rc::new(member.clone()), payload)));
        let key = event.identity();
        let NetEvent::Result(result) = &event else {
            unreachable!();
        };
        let EventIdentity::Result(shared) = &key else {
            unreachable!();
        };
        assert!(Rc::ptr_eq(&result.0, shared));
        assert_eq!(Rc::strong_count(shared), 2);
        let independently_owned_key = EventIdentity::Result(Rc::new(member.clone()));
        assert_eq!(key, independently_owned_key);
        assert_eq!(key.cmp(&independently_owned_key), std::cmp::Ordering::Equal);
        drop(key);
        let NetEvent::Result(result) = event else {
            unreachable!();
        };
        assert_eq!(Rc::strong_count(&result.0), 1);
        assert_eq!(Rc::try_unwrap(result.0).unwrap(), member);

        let fact = ProgramFactEntry::CoveredInput(
            covered_input_for_version(
                test_source(),
                &version(row(2), 20, "shared identity"),
                &aliases(),
            )
            .unwrap(),
        );
        let event = NetEvent::ProgramFact(Rc::new(fact.clone()));
        let key = event.identity();
        let NetEvent::ProgramFact(shared) = &event else {
            unreachable!();
        };
        let EventIdentity::ProgramFact(key_fact) = &key else {
            unreachable!();
        };
        assert!(Rc::ptr_eq(shared, key_fact));
        assert_eq!(key, EventIdentity::ProgramFact(Rc::new(fact.clone())));
        let mut map = BTreeMap::from([(key, (event, 1i64))]);
        // Equal values at distinct addresses still coalesce into the first
        // event; pointer identity is never a comparison or ordering key.
        let duplicate = NetEvent::ProgramFact(Rc::new(fact.clone()));
        map.entry(duplicate.identity())
            .and_modify(|(_, weight)| *weight += 1)
            .or_insert((duplicate, 1));
        assert_eq!(map.len(), 1);
        let (key, (event, weight)) = map.into_iter().next().unwrap();
        assert_eq!(weight, 2);
        drop(key);
        let NetEvent::ProgramFact(shared) = event else {
            unreachable!();
        };
        assert_eq!(Rc::strong_count(&shared), 1);
        assert_eq!(Rc::try_unwrap(shared).unwrap(), fact);
    }

    #[test]
    fn shared_covered_input_publishes_only_first_add_and_final_remove() {
        let aliases = aliases();
        let row = version(row(0x51), 10, "shared source");
        let fact = physical_input(
            covered_input_for_version(test_source(), &row, &aliases)
                .expect("test version has a registered node alias"),
        );
        let mut maintained = test_maintained();

        // Two independent terminals can reach the same exact source version.
        // The peer closure is a set: the second witness is not a second add,
        // and removing either witness must retain the other.
        assert_eq!(maintained.supporting.apply(0, fact.clone(), 1), Some(true));
        assert_eq!(maintained.supporting.apply(1, fact.clone(), 1), None);
        assert_eq!(maintained.supporting.apply(2, fact.clone(), 1), None);
        assert_eq!(maintained.supporting.apply(0, fact.clone(), -1), None);
        assert_eq!(maintained.supporting.apply(1, fact.clone(), -1), None);
        assert_eq!(
            maintained.supporting.apply(2, fact.clone(), -1),
            Some(false)
        );
        assert!(maintained.supporting.is_empty());
    }

    // Internal: signed terminal weights can temporarily be negative, and
    // only this boundary exposes each witness origin's independent presence.
    #[test]
    fn signed_source_weights_do_not_cancel_another_origins_presence() {
        let row = version(row(0x53), 12, "signed source");
        let fact =
            physical_input(covered_input_for_version(test_source(), &row, &aliases()).unwrap());
        let mut maintained = test_maintained();
        for (origin, weight, transition, visible) in [
            (0usize, 1, Some(true), true),
            (1usize, -1, None, true),
            (0usize, -1, Some(false), false),
            (1usize, 1, None, false),
            (2usize, 2, Some(true), true),
            (0usize, -2, None, true),
            (2usize, -2, Some(false), false),
            (0usize, 2, None, false),
        ] {
            assert_eq!(
                maintained.supporting.apply(origin, fact.clone(), weight),
                transition,
            );
            assert_eq!(
                maintained
                    .supporting_rows()
                    .cloned()
                    .collect::<BTreeSet<_>>()
                    .contains(&fact),
                visible
            );
        }
        assert!(maintained.supporting.is_empty());
    }

    #[test]
    fn source_fact_changes_coalesce_remove_readd_within_one_drain() {
        let fact = physical_input(
            covered_input_for_version(
                test_source(),
                &version(row(0x52), 11, "coalesced"),
                &aliases(),
            )
            .unwrap(),
        );
        let mut maintained = test_maintained();
        maintained.supporting.apply(0, fact.clone(), 1);
        maintained.acknowledge_peer_source_closure();
        maintained.supporting.apply(0, fact.clone(), -1);
        maintained.supporting.apply(0, fact.clone(), 1);
        assert_eq!(
            maintained.unpublished_supporting_delta(),
            Some((vec![], vec![]))
        );
        maintained.supporting.apply(0, fact.clone(), 1);
        maintained.supporting.apply(0, fact, -1);
        assert_eq!(
            maintained.unpublished_supporting_delta(),
            Some((vec![], vec![]))
        );
    }

    // Internal: source replacement must release exact wire witnesses even
    // though deletion has no application tuple or visible graph row.
    #[test]
    fn selected_deletion_witness_replacement_releases_facts_and_versions() {
        let version = deletion(RowUuid(uuid::Uuid::from_u128(7)), 42);
        let aliases = BTreeMap::from([(NodeUuid(uuid::Uuid::from_u128(10)), NodeAlias(10))]);
        let input = covered_input_for_version(test_source(), &version, &aliases).unwrap();
        let tx = input.version.tx;
        let fact = physical_input(input);
        let mut maintained = test_maintained();
        maintained.acknowledge_peer_source_closure();
        assert!(
            maintained.replace_selected_deletion_witnesses(BTreeMap::from([(
                fact.clone(),
                version.clone()
            )]))
        );
        assert_eq!(
            maintained.unpublished_supporting_delta(),
            Some((vec![fact.clone()], vec![]))
        );
        maintained.acknowledge_peer_source_closure();
        assert_eq!(maintained.versions_by_tx(tx), vec![version]);
        assert!(
            maintained
                .supporting_rows()
                .cloned()
                .collect::<BTreeSet<_>>()
                .contains(&fact)
        );
        assert!(maintained.replace_selected_deletion_witnesses(BTreeMap::new()));
        assert_eq!(
            maintained.unpublished_supporting_delta(),
            Some((vec![], vec![fact]))
        );
        assert!(maintained.versions_by_tx(tx).is_empty());
        assert!(
            maintained
                .supporting_rows()
                .cloned()
                .collect::<BTreeSet<_>>()
                .is_empty()
        );
    }

    #[test]
    fn multisink_shared_source_fact_retracts_only_after_its_last_terminal() {
        // The obsolete coverage-fact identity is gone. Two distinct query
        // occurrences now contribute to the same physical row frontier.
        let record = version(row(0x51), 10, "shared");
        let first = version_content(record.clone());
        let second = DecodedMaintainedEvent::VersionContent {
            source: ProgramSourceId {
                table: "todos".to_owned().into(),
                path: vec![crate::protocol::ProgramSourceRole::Alias("peer".to_owned())],
            },
            row: record.clone(),
        };
        let input =
            physical_input(covered_input_for_version(test_source(), &record, &aliases()).unwrap());
        let mut maintained = test_maintained();
        maintained.acknowledge_peer_source_closure();
        maintained
            .apply_decoded_deltas([(first.clone(), 1), (second.clone(), 1)], &aliases())
            .unwrap();
        assert_eq!(
            maintained.unpublished_supporting_delta(),
            Some((vec![input.clone()], vec![]))
        );
        maintained.acknowledge_peer_source_closure();
        maintained
            .apply_decoded_deltas([(first.clone(), -1)], &aliases())
            .unwrap();
        assert_eq!(
            maintained.unpublished_supporting_delta(),
            Some((vec![], vec![]))
        );
        maintained
            .apply_decoded_deltas([(second.clone(), -1)], &aliases())
            .unwrap();
        assert_eq!(
            maintained.unpublished_supporting_delta(),
            Some((vec![], vec![input.clone()]))
        );
        maintained.acknowledge_peer_source_closure();
        maintained
            .apply_decoded_deltas([(first.clone(), 1)], &aliases())
            .unwrap();
        assert_eq!(
            maintained.unpublished_supporting_delta(),
            Some((vec![input.clone()], vec![]))
        );
        maintained.acknowledge_peer_source_closure();
        maintained
            .apply_decoded_deltas([(first, -1), (second.clone(), 1)], &aliases())
            .unwrap();
        assert_eq!(
            maintained.unpublished_supporting_delta(),
            Some((vec![], vec![]))
        );
        assert_eq!(maintained.supporting_rows().count(), 1);
        maintained
            .apply_decoded_deltas([(second, -1)], &aliases())
            .unwrap();
        assert_eq!(
            maintained.unpublished_supporting_delta(),
            Some((vec![], vec![input]))
        );
    }

    fn replacement_content(row: VersionRow) -> DecodedMaintainedEvent {
        DecodedMaintainedEvent::ReplacementContent {
            source: test_source(),
            row,
        }
    }

    // Internal ownership/role oracle: public row equality cannot distinguish
    // shared allocations or independent retained role weights.
    #[test]
    fn shared_witness_payload_retains_independent_role_lifetimes() {
        for is_deletion in [false, true] {
            let mut maintained = test_maintained();
            let record = if is_deletion {
                deletion(row(1), 100)
            } else {
                version(row(1), 100, "shared")
            };
            let identity = VersionIdentity::for_row(&record);
            let key = ReplacementKey::for_row(&record, identity.layer);
            let sort_key = VersionSortKey::for_row(&record, &identity);
            let tx_id = version_tx_id_from_aliases(&record, &aliases()).unwrap();
            let shared = DecodedMaintainedEvent::SharedVersion {
                source: test_source(),
                row: record.clone(),
            };
            let first = maintained
                .apply_decoded_deltas([(shared.clone(), 1)], &aliases())
                .unwrap();
            assert!(first.supporting_changed);
            assert_eq!(maintained.supporting_rows().count(), 1);
            let replacements = if is_deletion {
                &maintained.replacements.deletion_by_key
            } else {
                &maintained.replacements.content_by_key
            };
            let version_payload = &maintained.versions.by_tx[&tx_id][&sort_key].payload;
            let replacement_payload = &replacements[&key][&identity].payload;
            assert!(Arc::ptr_eq(version_payload, replacement_payload));
            assert!(Arc::ptr_eq(
                &maintained.versions.by_tx[&tx_id]
                    .keys()
                    .next()
                    .unwrap()
                    .raw_record,
                &replacements[&key].keys().next().unwrap().raw_record
            ));
            let version_event = if is_deletion {
                version_deletion(record.clone())
            } else {
                version_content(record.clone())
            };
            let replacement_event = if is_deletion {
                replacement_deletion(record.clone())
            } else {
                replacement_content(record.clone())
            };
            let withdrew = maintained
                .apply_decoded_deltas([(replacement_event.clone(), -1)], &aliases())
                .unwrap();
            assert!(withdrew.program_fact_removes.is_empty());
            assert_eq!(maintained.versions.by_tx[&tx_id][&sort_key].weight, 1);
            assert_eq!(maintained.replacements.entry_count(), 0);
            maintained
                .apply_decoded_deltas(
                    [(version_event, -1), (replacement_event.clone(), 1)],
                    &aliases(),
                )
                .unwrap();
            assert_eq!(
                maintained
                    .supporting_rows()
                    .cloned()
                    .collect::<BTreeSet<_>>()
                    .len(),
                1
            );
            assert!(maintained.versions.by_tx.is_empty());
            assert_eq!(maintained.replacements.entry_count(), 1);
            let removed = maintained
                .apply_decoded_deltas([(replacement_event, -1)], &aliases())
                .unwrap();
            assert!(removed.supporting_changed);
            assert!(
                maintained
                    .supporting_rows()
                    .cloned()
                    .collect::<BTreeSet<_>>()
                    .is_empty()
            );
            let cancelled = maintained
                .apply_decoded_deltas([(shared.clone(), 1), (shared, -1)], &aliases())
                .unwrap();
            assert!(cancelled.program_fact_adds.is_empty());
            assert!(cancelled.program_fact_removes.is_empty());
            assert!(maintained.versions.by_tx.is_empty());
            assert_eq!(maintained.replacements.entry_count(), 0);
        }
    }

    fn replacement_deletion(row: VersionRow) -> DecodedMaintainedEvent {
        DecodedMaintainedEvent::ReplacementDeletion {
            source: test_source(),
            row,
        }
    }

    fn result_current(member: ResultMemberEntry) -> DecodedMaintainedEvent {
        DecodedMaintainedEvent::ResultCurrent {
            payload: ResultMemberPayloadEntry {
                member: member.clone(),
                descriptor: Vec::new(),
                record: Vec::new(),
            },
            member,
        }
    }

    #[test]
    fn result_single_enter_then_leave_emits_add_then_remove() {
        let aliases = aliases();
        let entry = result(row(1), 10);
        let member = ResultMemberEntry::from(entry);
        let mut maintained = test_maintained();

        let first = maintained
            .apply_decoded_deltas([(result_current(member.clone()), 1)], &aliases)
            .unwrap();
        assert_eq!(first.adds, vec![member.clone()]);
        assert!(first.removes.is_empty());

        let second = maintained
            .apply_decoded_deltas([(result_current(member.clone()), -1)], &aliases)
            .unwrap();
        assert!(second.adds.is_empty());
        assert_eq!(second.removes, vec![member]);
        assert!(maintained.result_weights.is_empty());
    }

    #[test]
    fn membership_waits_for_later_content_witness_before_publication() {
        // Model two separately delivered multisink deltas: Stream A reports
        // membership first, while a cold Stream B finishes the exact history
        // witness only on the later runtime turn. Publishing the first delta
        // would make the wire builder fail closed for the missing bundle.
        let aliases = aliases();
        let member = ResultMemberEntry::from(result(row(1), 10));
        let mut maintained = test_maintained();

        let mut first = maintained
            .apply_decoded_deltas([(result_current(member.clone()), 1)], &aliases)
            .unwrap();
        maintained.finalize_multisink_transitions(&mut first, &aliases);
        assert!(
            first.adds.is_empty(),
            "Stream A must remain pending without Stream B"
        );
        assert!(first.removes.is_empty());
        assert!(first.result_payload_adds.is_empty());
        assert!(first.result_payload_removes.is_empty());

        let mut second = maintained
            .apply_decoded_deltas(
                [(version_content(version(row(1), 10, "ready")), 1)],
                &aliases,
            )
            .unwrap();
        maintained.finalize_multisink_transitions(&mut second, &aliases);
        assert_eq!(second.adds, vec![member.clone()]);
        assert!(second.removes.is_empty());
        assert!(second.result_payload_adds.is_empty());
        assert!(second.result_payload_removes.is_empty());

        let mut third = maintained
            .apply_decoded_deltas([(result_current(member.clone()), -1)], &aliases)
            .unwrap();
        maintained.finalize_multisink_transitions(&mut third, &aliases);
        assert!(third.adds.is_empty());
        assert_eq!(third.removes, vec![member]);
        assert!(third.result_payload_adds.is_empty());
        assert!(third.result_payload_removes.is_empty());
    }

    // Internal: the journal must survive discarded intermediate transitions,
    // and public rows cannot expose how many retained members were revisited.
    // Compare exact publication deltas/state with the existing full algorithm.
    #[test]
    fn storage_backed_membership_reconciles_only_retained_touched_candidates() {
        for count in [8, 1024] {
            let members = (0..=count)
                .map(|index| {
                    let member = RealRowMemberEntry::current_content(result(
                        RowUuid(uuid::Uuid::from_u128(index as u128 + 1)),
                        10,
                    ));
                    ResultMemberEntry::from(if index % 2 == 0 {
                        member.with_row_digest(vec![0xd1, 0x6e])
                    } else {
                        member
                    })
                })
                .collect::<Vec<_>>();
            let mut incremental = test_maintained();
            incremental.enable_storage_backed_result_materialization();
            incremental
                .apply_decoded_deltas(
                    members[..count]
                        .iter()
                        .cloned()
                        .map(|member| (result_current(member), 1)),
                    &aliases(),
                )
                .unwrap();
            let initial = incremental.reconcile_publishable_result_members(&aliases());
            assert_eq!(initial.0.len(), count);
            assert_eq!(incremental.result_member_reconcile_visits, count);
            let mut reference = incremental.clone();

            for changes in [
                vec![(0, -1), (1, 1), (count, 1)],
                vec![(0, 1), (1, -1), (count, -1)],
                vec![(2, -2)],
                vec![(2, 1)],
                vec![(2, 1)],
                vec![(3, -1), (3, 1)],
                vec![],
            ] {
                for view in [&mut incremental, &mut reference] {
                    for &(index, weight) in &changes {
                        // A caller can drop a transition before completing a
                        // later drain. The view, not that return value, owns
                        // every candidate until reconciliation succeeds.
                        drop(
                            view.apply_decoded_deltas(
                                [(result_current(members[index].clone()), weight)],
                                &aliases(),
                            )
                            .unwrap(),
                        );
                    }
                }
                let touched = changes
                    .iter()
                    .map(|(index, _)| *index)
                    .collect::<BTreeSet<_>>();
                incremental.result_member_reconcile_visits = 0;
                reference.unreconciled_result_members = None;
                let expected = reference.reconcile_publishable_result_members(&aliases());
                let actual = incremental.reconcile_publishable_result_members(&aliases());
                assert_eq!(actual, expected);
                assert_eq!(incremental.result_member_reconcile_visits, touched.len());
                assert_eq!(incremental.result_weights, reference.result_weights);
                assert_eq!(
                    incremental.published_result_members,
                    reference.published_result_members
                );
                assert_eq!(
                    incremental.published_result_payloads,
                    reference.published_result_payloads
                );
                assert!(
                    incremental
                        .unreconciled_result_members
                        .as_ref()
                        .unwrap()
                        .is_empty()
                );
            }
        }

        // Switching an already-populated witness-gated view must establish a
        // full baseline, promoting a previously withheld membership even when
        // the switching call carries no new member delta.
        let member = ResultMemberEntry::from(result(row(1), 10));
        let mut switched = test_maintained();
        switched
            .apply_decoded_deltas([(result_current(member.clone()), 1)], &aliases())
            .unwrap();
        assert!(
            switched
                .reconcile_publishable_result_members(&aliases())
                .0
                .is_empty()
        );
        assert!(switched.unreconciled_result_members.is_none());
        switched.enable_storage_backed_result_materialization();
        assert_eq!(
            switched.reconcile_publishable_result_members(&aliases()).0,
            vec![member]
        );
        switched.result_member_reconcile_visits = 0;
        assert!(
            switched
                .reconcile_publishable_result_members(&aliases())
                .0
                .is_empty()
        );
        assert_eq!(switched.result_member_reconcile_visits, 0);
    }

    #[test]
    fn storage_backed_membership_publishes_without_bundle_witness() {
        // This must remain an internal receipt: public test routes cannot
        // synthesize the deliberately omitted Stream B terminal. The
        // storage-backed subset instead resolves the exact member `(table,
        // row, tx)` from node storage at materialization time.
        let aliases = aliases();
        let member = ResultMemberEntry::from(result(row(2), 20));
        let mut maintained = test_maintained();
        maintained.enable_storage_backed_result_materialization();

        let mut transitions = maintained
            .apply_decoded_deltas([(result_current(member.clone()), 1)], &aliases)
            .unwrap();
        maintained.finalize_multisink_transitions(&mut transitions, &aliases);

        assert_eq!(transitions.adds, vec![member]);
        assert!(transitions.removes.is_empty());
    }

    #[test]
    fn row_digest_payload_waits_for_its_membership_witness_boundary() {
        let aliases = aliases();
        let member = ResultMemberEntry::from(
            RealRowMemberEntry::current_content(result(row(1), 10))
                .with_row_digest(vec![0xd1, 0x6e]),
        );
        let payload = ResultMemberPayloadEntry {
            member: member.clone(),
            descriptor: vec![0x01],
            record: vec![0x02],
        };
        let mut maintained = test_maintained();

        // The raw result terminal carries both Stream-A fields, but neither
        // may be published before Stream B proves the content row.
        let mut raw = maintained
            .apply_decoded_deltas(
                [(
                    DecodedMaintainedEvent::ResultCurrent {
                        member: member.clone(),
                        payload: payload.clone(),
                    },
                    1,
                )],
                &aliases,
            )
            .unwrap();
        assert_eq!(
            raw.result_payload_adds,
            vec![(member.clone(), payload.clone())]
        );
        maintained.finalize_multisink_transitions(&mut raw, &aliases);
        assert!(raw.adds.is_empty());
        assert!(raw.removes.is_empty());
        assert!(raw.result_payload_adds.is_empty());
        assert!(raw.result_payload_removes.is_empty());

        // A pending membership may disappear and re-enter with a replacement
        // payload before its witness arrives. Neither half becomes visible,
        // and the later promotion must use the replacement payload only.
        let mut pending_remove = maintained
            .apply_decoded_deltas([(result_current(member.clone()), -1)], &aliases)
            .unwrap();
        maintained.finalize_multisink_transitions(&mut pending_remove, &aliases);
        assert!(pending_remove.adds.is_empty());
        assert!(pending_remove.removes.is_empty());
        assert!(pending_remove.result_payload_adds.is_empty());
        assert!(pending_remove.result_payload_removes.is_empty());

        let replacement_payload = ResultMemberPayloadEntry {
            member: member.clone(),
            descriptor: vec![0x03],
            record: vec![0x04],
        };
        let mut pending_readd = maintained
            .apply_decoded_deltas(
                [(
                    DecodedMaintainedEvent::ResultCurrent {
                        member: member.clone(),
                        payload: replacement_payload.clone(),
                    },
                    1,
                )],
                &aliases,
            )
            .unwrap();
        maintained.finalize_multisink_transitions(&mut pending_readd, &aliases);
        assert!(pending_readd.adds.is_empty());
        assert!(pending_readd.removes.is_empty());
        assert!(pending_readd.result_payload_adds.is_empty());
        assert!(pending_readd.result_payload_removes.is_empty());

        let mut content = maintained
            .apply_decoded_deltas(
                [(version_content(version(row(1), 10, "ready")), 1)],
                &aliases,
            )
            .unwrap();
        maintained.finalize_multisink_transitions(&mut content, &aliases);
        assert_eq!(content.adds, vec![member.clone()]);
        assert!(content.removes.is_empty());
        assert_eq!(
            content.result_payload_adds,
            vec![(member.clone(), replacement_payload.clone())]
        );
        assert!(content.result_payload_removes.is_empty());

        // Losing an already-published content witness must withdraw both
        // stream halves; restoring that witness emits the current pair again.
        let mut content_retraction = maintained
            .apply_decoded_deltas(
                [(version_content(version(row(1), 10, "ready")), -1)],
                &aliases,
            )
            .unwrap();
        maintained.finalize_multisink_transitions(&mut content_retraction, &aliases);
        assert!(content_retraction.adds.is_empty());
        assert_eq!(content_retraction.removes, vec![member.clone()]);
        assert!(content_retraction.result_payload_adds.is_empty());
        assert_eq!(
            content_retraction.result_payload_removes,
            vec![member.clone()]
        );

        let mut content_restore = maintained
            .apply_decoded_deltas(
                [(version_content(version(row(1), 10, "ready")), 1)],
                &aliases,
            )
            .unwrap();
        maintained.finalize_multisink_transitions(&mut content_restore, &aliases);
        assert_eq!(content_restore.adds, vec![member.clone()]);
        assert!(content_restore.removes.is_empty());
        assert_eq!(
            content_restore.result_payload_adds,
            vec![(member.clone(), replacement_payload)]
        );
        assert!(content_restore.result_payload_removes.is_empty());

        let mut removal = maintained
            .apply_decoded_deltas([(result_current(member.clone()), -1)], &aliases)
            .unwrap();
        maintained.finalize_multisink_transitions(&mut removal, &aliases);
        assert!(removal.adds.is_empty());
        assert_eq!(removal.removes, vec![member.clone()]);
        assert!(removal.result_payload_adds.is_empty());
        assert_eq!(removal.result_payload_removes, vec![member]);
    }

    // Internal because malformed typed terminal events are below the public
    // query API. A late decode failure must not publish the earlier rows.
    #[test]
    fn late_decoded_event_error_leaves_retained_rows_unchanged() {
        let descriptor = RecordDescriptor::new([("row_uuid", ValueType::Uuid)]);
        let event = || {
            (
                DecodedMaintainedEvent::StructuredAppRow {
                    root: row(1),
                    record: OwnedRecord::new(
                        descriptor.create(&[Value::Uuid(row(1).0)]).unwrap(),
                        descriptor,
                    ),
                },
                1,
            )
        };
        let mut maintained = test_maintained();
        let result = maintained.apply_decoded_delta_results(
            [
                Ok(event()),
                Err(super::super::Error::InvalidStoredValue(
                    "late terminal decode",
                )),
            ],
            &aliases(),
        );
        assert!(matches!(
            result,
            Err(super::super::Error::InvalidStoredValue(
                "late terminal decode"
            ))
        ));
        assert!(maintained.structured_app_rows().is_empty());
        // Positive control: the valid prefix would be observable if applied.
        maintained
            .apply_decoded_deltas([event()], &aliases())
            .unwrap();
        assert_eq!(maintained.structured_app_rows().len(), 1);
    }

    #[test]
    fn discarded_structured_app_row_collector_does_not_retain_later_deltas() {
        let descriptor =
            RecordDescriptor::new([("row_uuid", ValueType::Uuid), ("title", ValueType::String)]);
        let record = OwnedRecord::new(
            descriptor
                .create(&[
                    Value::Uuid(row(1).0),
                    Value::String("later terminal row".to_owned()),
                ])
                .unwrap(),
            descriptor,
        );
        let mut maintained = test_maintained();

        maintained.discard_structured_app_rows();
        maintained
            .apply_decoded_deltas(
                [(
                    DecodedMaintainedEvent::StructuredAppRow {
                        root: row(1),
                        record,
                    },
                    1,
                )],
                &aliases(),
            )
            .unwrap();

        assert!(maintained.structured_app_rows().is_empty());
        assert_eq!(maintained.footprint().structured_app_rows, 0);
        assert_eq!(maintained.footprint().structured_app_rows_bytes, 0);
    }

    #[test]
    fn typed_union_terminal_removes_one_arm_and_rehydrates_the_other() {
        let descriptor = RecordDescriptor::new([
            ("table", groove::records::ValueType::String),
            ("row_uuid", groove::records::ValueType::Uuid),
            ("joined_uuid", groove::records::ValueType::Uuid),
            ("union_arm", groove::records::ValueType::String),
            ("tx_time", groove::records::ValueType::U64),
            ("tx_node", groove::records::ValueType::U64),
        ]);
        let schema = ResultMembershipSchema {
            table_field: "table".to_owned(),
            row_field: "row_uuid".to_owned(),
            occurrence_id_fields: vec!["row_uuid".to_owned(), "joined_uuid".to_owned()],
            occurrence_union_arm_fields: BTreeMap::from([(0, "union_arm".to_owned())]),
            payload_fields: Vec::new(),
            payload_publication_fields: BTreeMap::new(),
            branch_or_prefix_field: None,
            version: ResultMembershipVersionSchema::Content(
                super::super::query_engine::ContentVersionFields {
                    tx_time_field: "tx_time".to_owned(),
                    tx_node_field: "tx_node".to_owned(),
                },
            ),
            settle_position_field: None,
            routing_param_fields: BTreeSet::new(),
        };
        let schemas = MaintainedTerminalSchemas {
            sinks: BTreeMap::from([(
                "maintained.result_current".to_owned(),
                MaintainedTerminalKind::ResultCurrent(schema),
            )]),
        };
        let tables = BTreeMap::from([("todos".to_owned(), table())]);
        let encoded = |label: &str, weight| RecordDeltas {
            descriptor: descriptor.clone(),
            deltas: vec![RecordDelta {
                record: descriptor
                    .create(&[
                        Value::String("todos".to_owned()),
                        Value::Uuid(row(1).0),
                        Value::Uuid(row(2).0),
                        Value::String(label.to_owned()),
                        Value::U64(10),
                        Value::U64(10),
                    ])
                    .unwrap()
                    .into(),
                weight,
            }],
        };
        let mut maintained = test_maintained();
        let direct = maintained
            .apply_typed_deltas(
                "maintained.result_current",
                &encoded("direct", 1),
                &schemas,
                &tables,
                &aliases(),
            )
            .unwrap()
            .adds
            .pop()
            .unwrap();
        let inherited = maintained
            .apply_typed_deltas(
                "maintained.result_current",
                &encoded("inherited", 1),
                &schemas,
                &tables,
                &aliases(),
            )
            .unwrap()
            .adds
            .pop()
            .unwrap();
        assert_ne!(
            direct.output_occurrence_id(),
            inherited.output_occurrence_id()
        );

        let removed = maintained
            .apply_typed_deltas(
                "maintained.result_current",
                &encoded("direct", -1),
                &schemas,
                &tables,
                &aliases(),
            )
            .unwrap();
        assert_eq!(removed.removes, [direct]);
        assert_eq!(maintained.result_weights.get(&inherited), Some(&1));

        let mut reopened = test_maintained();
        let rehydrated = reopened
            .apply_typed_deltas(
                "maintained.result_current",
                &encoded("inherited", 1),
                &schemas,
                &tables,
                &aliases(),
            )
            .unwrap();
        assert_eq!(rehydrated.adds, std::slice::from_ref(&inherited));
        assert_eq!(reopened.result_weights.get(&inherited), Some(&1));
    }

    #[test]
    fn result_non_consolidated_drain_nets_to_one_add() {
        let aliases = aliases();
        let entry = result(row(1), 10);
        let member = ResultMemberEntry::from(entry);
        let mut maintained = test_maintained();

        let transitions = maintained
            .apply_decoded_deltas(
                [
                    (result_current(member.clone()), 1),
                    (result_current(member.clone()), 1),
                    (result_current(member.clone()), -1),
                ],
                &aliases,
            )
            .unwrap();

        assert_eq!(transitions.adds, vec![member.clone()]);
        assert!(transitions.removes.is_empty());
        assert_eq!(maintained.result_weights.get(&member), Some(&1));
    }

    #[test]
    fn result_weight_magnitude_greater_than_one_tracks_active_membership() {
        let aliases = aliases();
        let entry = result(row(1), 10);
        let member = ResultMemberEntry::from(entry);
        let mut maintained = test_maintained();

        let active = maintained
            .apply_decoded_deltas([(result_current(member.clone()), 2)], &aliases)
            .unwrap();
        assert_eq!(active.adds, vec![member.clone()]);
        assert!(active.removes.is_empty());

        let inactive = maintained
            .apply_decoded_deltas([(result_current(member.clone()), -2)], &aliases)
            .unwrap();
        assert!(inactive.adds.is_empty());
        assert_eq!(inactive.removes, vec![member]);
        assert!(maintained.result_weights.is_empty());
    }

    // Internal accounting oracle: public queries cannot inspect retained byte
    // totals. Exercise the same mutation wrappers used by reconciliation,
    // including signed weights, resized payloads, journal resets, and clones.
    #[test]
    fn retained_result_accounting_matches_full_scans_through_mutations() {
        let mut maintained = test_maintained();
        let members = (0..32)
            .map(|i| {
                ResultMemberEntry::from(
                    RealRowMemberEntry::current_content(result(row(i + 1), 10))
                        .with_row_digest(vec![i; usize::from(i) + 1]),
                )
            })
            .collect::<Vec<_>>();
        let mut seed = 29_u64;
        for step in 0..600 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let member = members[(seed >> 32) as usize % members.len()].clone();
            let old = maintained.result_weights.get(&member).copied().unwrap_or(0);
            let new = old + [1, -2, 0, 3, -4][step % 5];
            let payload = ResultMemberPayloadEntry {
                member: member.clone(),
                descriptor: vec![1; step % 41],
                record: vec![2; step % 137],
            };
            if new == 0 {
                maintained.result_weights.remove(&member);
            } else {
                maintained.result_weights.insert(member.clone(), new);
            }
            if new > 0 {
                maintained
                    .result_payloads
                    .insert(member.clone(), payload.clone());
                maintained
                    .published_result_payloads
                    .insert(member.clone(), payload);
                maintained.published_result_members.insert(member.clone());
            } else {
                maintained.result_payloads.remove(&member);
                maintained.published_result_payloads.remove(&member);
                maintained.published_result_members.remove(&member);
            }
            maintained
                .unreconciled_result_members
                .get_or_insert_with(Default::default)
                .insert(member);
            maintained.assert_incremental_footprint_matches_full_scan();
            if step % 17 == 0 {
                let retained = maintained.clone();
                let retained_footprint = retained.footprint();
                // Both taking a completed journal and the full-reconcile
                // replacement path must install independent fresh counters.
                maintained.unreconciled_result_members.take();
                maintained.published_result_members = maintained
                    .published_result_members
                    .iter()
                    .cloned()
                    .collect::<BTreeSet<_>>()
                    .into();
                maintained.assert_incremental_footprint_matches_full_scan();
                assert_eq!(retained.footprint(), retained_footprint);
            }
        }
        let retained = maintained.clone();
        for member in &members {
            maintained.result_weights.remove(member);
            maintained.result_payloads.remove(member);
            maintained.published_result_payloads.remove(member);
            maintained.published_result_members.remove(member);
        }
        maintained.unreconciled_result_members = None;
        assert_eq!(maintained.footprint().result_weights_bytes, 0);
        assert_eq!(maintained.footprint().result_payloads_bytes, 0);
        assert_eq!(maintained.footprint().result_rows, 0);
        retained.assert_incremental_footprint_matches_full_scan();
        assert!(!retained.result_weights.is_empty());
    }

    // Internal differential/accounting oracle: public rows cannot verify
    // replacement weights, exact modeled bytes, or payload ownership.
    #[test]
    fn replacement_accounting_matches_signed_mutation_oracle() {
        let aliases = aliases();
        let payloads = (0..32)
            .map(|i| {
                let record = if i % 2 == 0 {
                    version(row(i % 4 + 1), u64::from(10 + i / 4), "content")
                } else {
                    deletion(row(i % 4 + 1), u64::from(10 + i / 4))
                };
                VersionPayload::prepare(
                    record.clone(),
                    &VersionIdentity::for_row(&record),
                    &aliases,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let mut index = ReplacementIndex::default();
        let mut oracle = BTreeMap::<VersionIdentity, WeightedVersion>::new();
        let mut seed = 41_u64;
        for step in 0..600 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let payload = Arc::clone(&payloads[(seed >> 32) as usize % payloads.len()]);
            let identity = VersionIdentity::for_row(&payload.row);
            let key = ReplacementKey::for_row(&payload.row, payload.row.layer());
            let weight = [1, -1, 0, -4, 2, 3, -2][step % 7];
            let new = oracle.get(&identity).map_or(0, |version| version.weight) + weight;
            if new > 0 {
                oracle.insert(
                    identity.clone(),
                    WeightedVersion {
                        payload: Arc::clone(&payload),
                        weight: new,
                    },
                );
            } else {
                oracle.remove(&identity);
            }
            index.apply_delta(key, identity, payload, weight);
            index.assert_footprint_matches_full_scan();
            assert_eq!(index.entry_count(), oracle.len());
            for rows in index
                .content_by_key
                .values()
                .chain(index.deletion_by_key.values())
            {
                assert!(!rows.is_empty());
                for (identity, actual) in rows {
                    assert_eq!(actual.weight, oracle[identity].weight);
                    assert!(Arc::ptr_eq(&actual.payload, &oracle[identity].payload));
                }
            }
        }
        let retained = index.clone();
        let retained_bytes = retained.footprint_bytes();
        for (identity, version) in &oracle {
            index.apply_delta(
                ReplacementKey::for_row(&version.row, version.row.layer()),
                identity.clone(),
                Arc::clone(&version.payload),
                -version.weight - 1,
            );
            index.assert_footprint_matches_full_scan();
        }
        assert_eq!(index.entry_count(), 0);
        assert_eq!(index.footprint_bytes(), 0);
        assert_eq!(retained.footprint_bytes(), retained_bytes);
        retained.assert_footprint_matches_full_scan();
        let payload = Arc::clone(&payloads[0]);
        index.apply_delta(
            ReplacementKey::for_row(&payload.row, payload.row.layer()),
            VersionIdentity::for_row(&payload.row),
            payload,
            1,
        );
        assert_eq!(index.entry_count(), 1);
        index.assert_footprint_matches_full_scan();
    }

    #[test]
    // Internal differential/accounting oracle: public row equality cannot
    // establish exact private footprint counters or retained index ownership.
    fn transaction_version_index_matches_identity_oracle_and_footprint() {
        let aliases = aliases();
        let mut records = (0..32)
            .map(|i| {
                let row_uuid = row(i % 4 + 1);
                let time = u64::from(10 + i / 8);
                if i % 8 < 4 {
                    version(row_uuid, time, "content")
                } else {
                    deletion(row_uuid, time)
                }
            })
            .collect::<Vec<_>>();
        // Same row/transaction/layer, distinct encoded identity.
        records.push(version(row(1), 10, "other content"));
        let payloads = records
            .iter()
            .map(|record| {
                VersionPayload::prepare(record.clone(), &VersionIdentity::for_row(record), &aliases)
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let mut index = WeightedVersionIndex::default();
        let mut oracle = BTreeMap::<VersionIdentity, WeightedVersion>::new();
        let mut seed = 17_u64;
        for step in 0..600 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let payload = Arc::clone(&payloads[(seed >> 32) as usize % payloads.len()]);
            let weight = [1, 2, -1, -4, 0, 3, -2][step % 7];
            let identity = VersionIdentity::for_row(&payload.row);
            let new = oracle.get(&identity).map_or(0, |version| version.weight) + weight;
            if new > 0 {
                oracle.insert(
                    identity,
                    WeightedVersion {
                        payload: Arc::clone(&payload),
                        weight: new,
                    },
                );
            } else {
                oracle.remove(&identity);
            }
            index.apply_delta(payload, weight);

            assert_eq!(index.entry_count, oracle.len(), "step {step}");
            assert!(index.by_tx.values().all(|rows| !rows.is_empty()));
            let entry_bytes = index
                .by_tx
                .values()
                .flat_map(|rows| rows.iter())
                .map(|(key, value)| version_sort_key_bytes(key) + weighted_version_bytes(value))
                .sum::<usize>();
            assert_eq!(index.entry_bytes, entry_bytes, "step {step}");
            assert_eq!(
                index.footprint_bytes(),
                btree_map_bytes(index.by_tx.len()) + btree_map_bytes(oracle.len()) + entry_bytes
            );
            for time in 10..=14 {
                let tx_id = tx(1, time);
                let mut expected = oracle
                    .values()
                    .filter(|version| version.tx_id == tx_id)
                    .collect::<Vec<_>>();
                expected.sort_by(|left, right| left.sort_key.cmp(&right.sort_key));
                assert_eq!(
                    index.versions_by_tx(tx_id),
                    expected
                        .iter()
                        .map(|version| version.row.clone())
                        .collect::<Vec<_>>()
                );
                for version in expected {
                    let actual = &index.by_tx[&tx_id][&version.sort_key];
                    assert_eq!(actual.weight, version.weight);
                    assert!(Arc::ptr_eq(&actual.payload, &version.payload));
                }
            }
        }
        // Removing the current view must not mutate a retained clone. A later
        // positive weight revives from zero, not from a discarded negative.
        let retained = index.clone();
        for version in oracle.values() {
            index.apply_delta(Arc::clone(&version.payload), -version.weight - 1);
        }
        assert!(index.by_tx.is_empty());
        assert_eq!(index.entry_count, 0);
        assert_eq!(index.footprint_bytes(), 0);
        assert_eq!(retained.entry_count, oracle.len());
        for version in oracle.values() {
            assert_eq!(
                retained.by_tx[&version.tx_id][&version.sort_key].weight,
                version.weight
            );
        }
        index.apply_delta(Arc::clone(&payloads[0]), 1);
        assert_eq!(index.entry_count, 1);
        assert_eq!(
            index.by_tx[&payloads[0].tx_id][&payloads[0].sort_key].weight,
            1
        );
    }

    #[test]
    fn versions_by_tx_contains_distinct_identities_sorted_and_prunes_retracted_one() {
        let aliases = aliases();
        let tx_id = tx(1, 10);
        let row_b = row(2);
        let row_a = row(1);
        let version_b = version(row_b, 10, "b");
        let version_a = version(row_a, 10, "a");
        let mut maintained = test_maintained();

        maintained
            .apply_decoded_deltas(
                [
                    (version_content(version_b.clone()), 1),
                    (version_content(version_a.clone()), 1),
                ],
                &aliases,
            )
            .unwrap();

        let versions = maintained.versions_by_tx(tx_id);
        assert_eq!(versions, vec![version_a.clone(), version_b]);
        let ordering = versions
            .iter()
            .map(|version| {
                (
                    version.table().to_owned(),
                    version.row_uuid(),
                    version.layer(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            ordering,
            vec![
                ("todos".to_owned(), row_a, VersionLayer::Content),
                ("todos".to_owned(), row_b, VersionLayer::Content),
            ]
        );

        maintained
            .apply_decoded_deltas([(version_content(version_a.clone()), -1)], &aliases)
            .unwrap();
        assert_eq!(
            maintained.versions_by_tx(tx_id),
            vec![version(row_b, 10, "b")]
        );
    }

    #[test]
    fn replacement_winner_change_leaves_one_active_winner() {
        let aliases = aliases();
        let row_uuid = row(1);
        let old = version(row_uuid, 10, "old");
        let new = version(row_uuid, 11, "new");
        let deletion = deletion(row_uuid, 12);
        let mut maintained = test_maintained();

        maintained
            .apply_decoded_deltas([(replacement_content(old.clone()), 1)], &aliases)
            .unwrap();
        assert_eq!(
            maintained.replacement_for("todos", row_uuid).0,
            Some(old.clone())
        );

        maintained
            .apply_decoded_deltas(
                [
                    (replacement_content(old), -1),
                    (replacement_content(new.clone()), 1),
                ],
                &aliases,
            )
            .unwrap();
        assert_eq!(
            maintained.replacement_for("todos", row_uuid),
            (Some(new), None)
        );

        maintained
            .apply_decoded_deltas([(replacement_deletion(deletion.clone()), 1)], &aliases)
            .unwrap();
        assert_eq!(
            maintained.replacement_for("todos", row_uuid),
            (Some(version(row_uuid, 11, "new")), Some(deletion))
        );
    }

    #[test]
    fn version_identity_retraction_removes_from_by_tx_and_prunes_tx_entry() {
        let aliases = aliases();
        let tx_id = tx(1, 10);
        let version = deletion(row(1), 10);
        let mut maintained = test_maintained();

        maintained
            .apply_decoded_deltas([(version_deletion(version.clone()), 1)], &aliases)
            .unwrap();
        assert_eq!(maintained.versions_by_tx(tx_id), vec![version.clone()]);

        maintained
            .apply_decoded_deltas([(version_deletion(version), -1)], &aliases)
            .unwrap();
        assert!(maintained.versions_by_tx(tx_id).is_empty());
        assert!(!maintained.versions.by_tx.contains_key(&tx_id));
    }
}

#[cfg(test)]
mod terminal_role_hash_tests {
    use super::*;
    use crate::node::query_engine::AppRowTerminal;
    use crate::node::{CurrentRowPublicationField, CurrentRowResultVisibility};
    use groove::records::{DescriptorField, FieldIdentity};

    fn schema(local_id: u64, slot: u64, carrier: &str) -> AppRowSchema {
        let nested = RecordDescriptor::new_with_fields([DescriptorField::new(
            "nested_name",
            ValueType::U64,
        )
        .with_identity(FieldIdentity::Slot(slot + 1))]);
        AppRowSchema {
            descriptor: RecordDescriptor::new_with_fields([
                DescriptorField::new("row_uuid", ValueType::Uuid)
                    .with_identity(FieldIdentity::Slot(slot)),
                DescriptorField::new(carrier, ValueType::Record(Box::new(nested))).with_identity(
                    FieldIdentity::NamedSlot {
                        name: "title".to_owned(),
                        slot: slot + 2,
                    },
                ),
            ]),
            publication_fields: BTreeMap::from([(
                carrier.to_owned(),
                CurrentRowPublicationField::StoredColumn {
                    id: crate::ids::PhysicalColumnId(local_id),
                    output_name: "title".to_owned(),
                },
            )]),
            hidden_fields: BTreeSet::new(),
            carrier: AppRowCarrier::Logical,
            field_carriers: BTreeMap::new(),
            public_field_names: BTreeMap::new(),
            terminal: AppRowTerminal::RootCollector,
            root_union_arm: false,
        }
    }

    #[test]
    fn terminal_layout_hash_uses_public_roles_not_local_catalogue_or_compiler_ids() {
        let first = terminal_root_layout(&schema(1, 10, "_app_1"));
        assert_eq!(
            first.id,
            "terminal:4091a932120f6c4bc4648cf92acff0530eded569f67b9fc0a4efabb739bfe69f"
        );
        let second = terminal_root_layout(&schema(99, 900, "_app_99"));
        assert_eq!(first.id, second.id);
        assert_eq!(
            first.root_descriptor.fields()[0].identity,
            Some(FieldIdentity::Name("row_uuid".to_owned()))
        );
        let ValueType::Record(nested) = &first.root_descriptor.fields()[1].value_type else {
            panic!("nested value")
        };
        assert_eq!(
            nested.fields()[0].identity,
            Some(FieldIdentity::Name("nested_name".to_owned()))
        );
        let mut changed_role = schema(1, 10, "_app_1");
        changed_role.publication_fields.insert(
            "_app_1".to_owned(),
            CurrentRowPublicationField::ResultField {
                name: "title".to_owned(),
                visibility: CurrentRowResultVisibility::ApplicationCell,
            },
        );
        assert_ne!(first.id, terminal_root_layout(&changed_role).id);
        let mut changed_name = schema(1, 10, "_app_1");
        changed_name.publication_fields.insert(
            "_app_1".to_owned(),
            CurrentRowPublicationField::StoredColumn {
                id: crate::ids::PhysicalColumnId(1),
                output_name: "other_title".to_owned(),
            },
        );
        assert_ne!(first.id, terminal_root_layout(&changed_name).id);
    }
}

#[cfg(test)]
std::thread_local! {
    pub(crate) static SOURCE_CLOSURE_POINT_LOOKUPS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    pub(crate) static SOURCE_CLOSURE_TRAVERSALS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
