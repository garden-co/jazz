use std::collections::{BTreeMap, BTreeSet};
use std::mem;

use groove::ivm::{MultisinkDeltas, RecordDeltas, TerminalEdit, TerminalOperation};
use groove::records::{
    BorrowedRecord, EnumValue, OwnedRecord, RecordDescriptor, RecordProjector, Value, ValueType,
    encode_record_descriptor,
};

use super::codec::{
    VersionLayer, VersionRow, VersionRowParts, authored_column_ids_from_value,
    deletion_event_from_value, history_values_from_parts, nullable_value,
    owned_record_from_storage_values_with_descriptor, register_values_from_parts,
    settled_result_value_storage_bytes, tx_ids_from_value, version_tx_id_from_aliases,
};
use super::query_engine::{
    AggregateResultSchema, AppRowCarrier, AppRowSchema, OutputTerminalSchema, ProgramFactKey,
    ProgramFactSchema, ProgramFactTerminal, QueryProgram, RelationEdgeSchema,
    ResultMembershipSchema, ResultMembershipVersionSchema, TypedOutputField, VersionWitnessSchema,
    VersionedRowRefSchema, logical_user_column,
};
use crate::db::{TerminalRootCarrier, TerminalRootLayout, TerminalRootPublicField};
use crate::ids::{AuthorSubject, NodeAlias, NodeUuid, RowUuid};
use crate::protocol::{
    BranchKey, CoveredInputEntry, ProgramFactEntry, ProgramSourceId, RealRowMemberEntry,
    RelationEdgeEntry, ResultMemberEntry, ResultMemberPayloadEntry, ResultRowLayer,
    RowVersionRefEntry, SyntheticReplacementToken,
};
use crate::schema::{RuntimeSchema, TableSchema};
use crate::time::{GlobalTime, TxTime};
use crate::tools::{ObjectId, OutputOccurrenceId};
use crate::tx::TxId;

type TableSchemas = BTreeMap<String, TableSchema>;
type VersionDecodePlanCache = BTreeMap<(String, VersionLayer), VersionDecodePlan>;

/// Distinguishes independent maintained terminals that can witness the same
/// peer source fact. Their union, rather than a summed terminal refcount, is
/// the exact receiver closure: a replacement witness disappearing must not
/// retract a still-live version witness (or vice versa).
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum SourceFactOrigin {
    Version,
    Replacement,
    ProgramFact,
}

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
    result_weights: BTreeMap<ResultMemberEntry, i64>,
    /// Result memberships already exposed to the subscription consumer. A
    /// result-current terminal can advance before the companion content
    /// witness terminal, so raw membership alone is not publishable.
    published_result_members: BTreeSet<ResultMemberEntry>,
    result_payloads: BTreeMap<ResultMemberEntry, ResultMemberPayloadEntry>,
    /// Payloads paired with memberships already exposed to a consumer. Keep
    /// this separate from `result_payloads`: the latter records the raw
    /// result-terminal state while a membership waits for its content witness.
    published_result_payloads: BTreeMap<ResultMemberEntry, ResultMemberPayloadEntry>,
    /// Incrementally maintained collector output. The key is the root row and
    /// the encoded tree so a -/+ replacement for one root never requires
    /// touching the rendered trees for other roots.
    structured_app_rows: BTreeMap<RowUuid, BTreeMap<Vec<u8>, i64>>,
    /// Collector-owned root sequence from the initial Groove terminal
    /// snapshot.  This is intentionally a sequence rather than a map: Jazz
    /// must not reconstruct query order from row values after lowering.
    structured_app_row_order: Vec<RowUuid>,
    /// Runtime terminal edits address roots by their opaque Groove key, while
    /// the retained collector tree is keyed by its public row UUID. Keep the
    /// compiler-emitted association so reset and incremental terminal edits
    /// fold through one local reducer without re-running the query.
    structured_root_keys: BTreeMap<Vec<u8>, RowUuid>,
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
    /// Exact active source-closure facts, independent of the transient
    /// multisink batches used to reach the current graph state. Peer
    /// publication diffs this set against its acknowledged predecessor so a
    /// +/− pair observed in one drain is never serialized as an ambiguous
    /// ordered operation.
    source_fact_weights: BTreeMap<ProgramFactEntry, BTreeMap<SourceFactOrigin, i64>>,
    versions: WeightedVersionIndex,
    replacements: ReplacementIndex,
}

impl Default for MaintainedSubscriptionView {
    fn default() -> Self {
        Self {
            result_weights: BTreeMap::new(),
            published_result_members: BTreeSet::new(),
            result_payloads: BTreeMap::new(),
            published_result_payloads: BTreeMap::new(),
            structured_app_rows: BTreeMap::new(),
            structured_app_row_order: Vec::new(),
            structured_root_keys: BTreeMap::new(),
            structured_app_row_descriptor: None,
            retains_structured_app_rows: true,
            storage_backed_result_materialization: false,
            inline_content_branch_keys: BTreeSet::new(),
            source_fact_weights: BTreeMap::new(),
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
    pub(crate) replacements_bytes: usize,
    pub(crate) total_heap_bytes: usize,
}

#[derive(Clone, Debug, Default)]
struct WeightedVersionIndex {
    by_identity: BTreeMap<VersionIdentity, WeightedVersion>,
    by_tx: BTreeMap<TxId, BTreeMap<VersionSortKey, BTreeSet<VersionIdentity>>>,
}

#[derive(Clone, Debug)]
struct WeightedVersion {
    row: VersionRow,
    tx_id: TxId,
    sort_key: VersionSortKey,
    weight: i64,
}

#[derive(Clone, Debug, Default)]
struct ReplacementIndex {
    content_by_key: BTreeMap<ReplacementKey, BTreeMap<VersionIdentity, WeightedVersion>>,
    deletion_by_key: BTreeMap<ReplacementKey, BTreeMap<VersionIdentity, WeightedVersion>>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct VersionIdentity {
    table: groove::Intern<String>,
    layer: VersionLayer,
    raw_record: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct VersionSortKey {
    table: groove::Intern<String>,
    row_uuid: RowUuid,
    layer: VersionLayer,
    raw_record: Vec<u8>,
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
    AggregateAppRows(AggregateResultSchema),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum EventIdentity {
    Result(ResultMemberEntry),
    Version(ProgramSourceId, VersionIdentity),
    Replacement(ProgramSourceId, ReplacementKey, VersionIdentity),
    ProgramFact(ProgramFactEntry),
    StructuredAppRow(RowUuid, Vec<u8>),
}

#[derive(Clone, Debug)]
enum NetEvent {
    Result(ResultMemberEntry, ResultMemberPayloadEntry),
    AggregateResult(
        ResultMemberEntry,
        ResultMemberPayloadEntry,
        super::query_engine::SyntheticResultMembershipSchema,
        Vec<String>,
    ),
    Version(ProgramSourceId, VersionIdentity, VersionRow),
    Replacement(ProgramSourceId, ReplacementKey, VersionIdentity, VersionRow),
    ProgramFact(ProgramFactEntry),
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
    pub(crate) fn uses_storage_backed_result_materialization(&self) -> bool {
        self.storage_backed_result_materialization
    }

    pub(crate) fn enable_storage_backed_result_materialization(&mut self) {
        self.storage_backed_result_materialization = true;
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
        let decoded = deltas
            .iter()
            .map(|(record, weight)| {
                decode_typed_terminal_record(
                    record,
                    kind,
                    tables,
                    node_aliases,
                    &mut decode_plan_cache,
                )
                .map(|event| (event, weight))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut transitions = self.apply_decoded_deltas(decoded, node_aliases)?;
        if observed_result_delta_batch {
            transitions.observed_result_delta_batches += 1;
        }
        transitions.requires_authoritative_membership_reconcile =
            requires_authoritative_membership_reconcile;
        Ok(transitions)
    }

    pub(crate) fn apply_multisink_deltas(
        &mut self,
        deltas: MultisinkDeltas,
        schemas: &MaintainedTerminalSchemas,
        tables: &TableSchemas,
        node_aliases: &BTreeMap<NodeUuid, NodeAlias>,
    ) -> Result<ResultTransitions, super::Error> {
        let mut transitions = ResultTransitions::default();
        for (sink, terminal) in &deltas.terminal_sinks {
            if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some()
                && !terminal.operations.is_empty()
            {
                eprintln!(
                    "JAZZ_COVERED_INPUT_TRACE stage=terminal_operations sink={sink} kind={:?} operations={}",
                    schemas.get(sink)?,
                    terminal.operations.len(),
                );
            }
            if let MaintainedTerminalKind::RootCollectorAppRows { layout, .. } =
                schemas.get(sink)?
            {
                for operation in &terminal.operations {
                    let operation = rebind_terminal_operation_to_layout(operation, layout)?;
                    self.apply_structured_terminal_operation(&operation)?;
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
            let root_collector_schema = match schemas.get(&sink)? {
                MaintainedTerminalKind::RootCollectorAppRows { schema, .. } => Some(schema.clone()),
                _ => None,
            };
            let delta_transitions =
                self.apply_typed_deltas(&sink, &deltas, schemas, tables, node_aliases)?;
            if let Some(schema) = root_collector_schema {
                self.replace_structured_app_row_order_from_snapshot(&schema, &deltas)?;
            }
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

    pub(crate) fn apply_decoded_deltas(
        &mut self,
        rows: impl IntoIterator<Item = (DecodedMaintainedEvent, i64)>,
        node_aliases: &BTreeMap<NodeUuid, NodeAlias>,
    ) -> Result<ResultTransitions, super::Error> {
        let mut net = BTreeMap::<EventIdentity, (NetEvent, i64)>::new();
        for (event, weight) in rows {
            let net_event = match event {
                DecodedMaintainedEvent::ResultCurrent { member, payload } => {
                    NetEvent::Result(member, payload)
                }
                DecodedMaintainedEvent::AggregateResult {
                    member,
                    payload,
                    synthetic,
                    value_fields,
                } => NetEvent::AggregateResult(member, payload, synthetic, value_fields),
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
                DecodedMaintainedEvent::ProgramSourceCoverage(coverage) => {
                    NetEvent::ProgramFact(ProgramFactEntry::ProgramSourceCoverage(coverage))
                }
                DecodedMaintainedEvent::RelationEdge(edge) => {
                    NetEvent::ProgramFact(ProgramFactEntry::RelationEdge(edge))
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

        let mut transitions = ResultTransitions::default();
        for (_, (event, weight)) in net {
            if weight == 0 {
                continue;
            }
            if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
                eprintln!(
                    "JAZZ_COVERED_INPUT_TRACE stage=apply_decoded_event event={event:?} weight={weight}"
                );
            }
            match event {
                NetEvent::Result(entry, payload) => {
                    self.apply_result_delta(entry, payload, weight, &mut transitions);
                }
                NetEvent::AggregateResult(member, payload, synthetic, value_fields) => {
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
                    let covered_input = covered_input_for_version(source, &row, node_aliases)?;
                    self.versions
                        .apply_delta(identity, row, weight, node_aliases)?;
                    self.apply_source_fact_delta(
                        SourceFactOrigin::Version,
                        ProgramFactEntry::CoveredInput(covered_input.clone()),
                        weight,
                    );
                    if weight > 0 {
                        transitions
                            .program_fact_adds
                            .push(ProgramFactEntry::CoveredInput(covered_input));
                    } else {
                        transitions
                            .program_fact_removes
                            .push(ProgramFactEntry::CoveredInput(covered_input));
                    }
                }
                NetEvent::Replacement(source, key, identity, row) => {
                    let covered_input = covered_input_for_version(source, &row, node_aliases)?;
                    self.replacements
                        .apply_delta(key, identity, row, weight, node_aliases)?;
                    self.apply_source_fact_delta(
                        SourceFactOrigin::Replacement,
                        ProgramFactEntry::CoveredInput(covered_input.clone()),
                        weight,
                    );
                    if weight > 0 {
                        transitions
                            .program_fact_adds
                            .push(ProgramFactEntry::CoveredInput(covered_input));
                    } else {
                        transitions
                            .program_fact_removes
                            .push(ProgramFactEntry::CoveredInput(covered_input));
                    }
                }
                NetEvent::ProgramFact(fact) => {
                    self.apply_source_fact_delta(
                        SourceFactOrigin::ProgramFact,
                        fact.clone(),
                        weight,
                    );
                    if weight > 0 {
                        transitions.program_fact_adds.push(fact);
                    } else {
                        transitions.program_fact_removes.push(fact);
                    }
                }
                NetEvent::StructuredAppRow(root, record) => {
                    if self.retains_structured_app_rows {
                        self.apply_structured_app_row_delta(root, record, weight);
                    }
                }
            }
        }
        Ok(transitions)
    }

    pub(crate) fn versions_by_tx(&self, tx_id: TxId) -> Vec<VersionRow> {
        self.versions.versions_by_tx(tx_id)
    }

    /// The final peer-safe source closure after every drained terminal batch.
    /// This intentionally exposes neither rendered result rows nor internal
    /// proof/relationship facts.
    pub(crate) fn active_peer_source_closure_facts(&self) -> BTreeSet<ProgramFactEntry> {
        self.source_fact_weights
            .iter()
            .filter(|(fact, weights)| {
                weights.values().any(|weight| *weight > 0) && fact.is_peer_source_closure_fact()
            })
            .map(|(fact, _)| fact.clone())
            .collect()
    }

    fn apply_source_fact_delta(
        &mut self,
        origin: SourceFactOrigin,
        fact: ProgramFactEntry,
        weight: i64,
    ) {
        let weights = self.source_fact_weights.entry(fact.clone()).or_default();
        let next = weights.get(&origin).copied().unwrap_or(0) + weight;
        if next == 0 {
            weights.remove(&origin);
        } else {
            weights.insert(origin, next);
        }
        if weights.is_empty() {
            self.source_fact_weights.remove(&fact);
        }
    }

    pub(crate) fn replacement_for(
        &self,
        table: &str,
        row_uuid: RowUuid,
    ) -> (Option<VersionRow>, Option<VersionRow>) {
        self.replacements.replacement_for(table, row_uuid)
    }

    pub(crate) fn footprint(&self) -> MaintainedSubscriptionViewFootprint {
        let result_weights_bytes = btree_map_bytes(self.result_weights.len())
            + self
                .result_weights
                .keys()
                .map(|member| result_member_entry_bytes(member) + mem::size_of::<i64>())
                .sum::<usize>()
            + btree_map_bytes(self.published_result_members.len())
            + self
                .published_result_members
                .iter()
                .map(result_member_entry_bytes)
                .sum::<usize>();
        let result_payloads_bytes = btree_map_bytes(self.result_payloads.len())
            + self
                .result_payloads
                .iter()
                .map(|(member, payload)| {
                    result_member_entry_bytes(member) + result_member_payload_entry_bytes(payload)
                })
                .sum::<usize>()
            + btree_map_bytes(self.published_result_payloads.len())
            + self
                .published_result_payloads
                .iter()
                .map(|(member, payload)| {
                    result_member_entry_bytes(member) + result_member_payload_entry_bytes(payload)
                })
                .sum::<usize>();
        let versions_bytes = self.versions.footprint_bytes();
        let replacements_bytes = self.replacements.footprint_bytes();
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
            + btree_map_bytes(self.structured_app_rows.len());
        MaintainedSubscriptionViewFootprint {
            result_rows: self
                .result_weights
                .values()
                .filter(|weight| **weight > 0)
                .count(),
            result_weights: self.result_weights.len(),
            result_payloads: self.result_payloads.len(),
            structured_app_rows: self
                .structured_app_rows
                .values()
                .map(|records| records.values().filter(|weight| **weight > 0).count())
                .sum(),
            version_identities: self.versions.by_identity.len(),
            version_tx_entries: self
                .versions
                .by_tx
                .values()
                .flat_map(|by_sort_key| by_sort_key.values())
                .map(BTreeSet::len)
                .sum(),
            replacement_entries: self.replacements.entry_count(),
            result_weights_bytes,
            result_payloads_bytes,
            structured_app_rows_bytes,
            versions_bytes,
            replacements_bytes,
            total_heap_bytes: result_weights_bytes
                + result_payloads_bytes
                + structured_app_rows_bytes
                + versions_bytes
                + replacements_bytes,
        }
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

    /// Returns the collector's current recursive row for one changed root.
    ///
    /// The incremental update builder uses this to replace just that root.
    pub(crate) fn structured_app_row(&self, root: RowUuid) -> Option<OwnedRecord> {
        let descriptor = self.structured_app_row_descriptor?;
        self.structured_app_rows
            .get(&root)?
            .iter()
            .filter(|(_, weight)| **weight > 0)
            .map(|(raw, _)| OwnedRecord::new(raw.clone(), descriptor))
            .next()
    }

    #[cfg(test)]
    pub(crate) fn structured_app_rows(&self) -> Vec<(RowUuid, OwnedRecord)> {
        self.structured_app_rows
            .keys()
            .filter_map(|root| self.structured_app_row(*root).map(|record| (*root, record)))
            .collect()
    }

    /// The exact public root order emitted by the collector's initial
    /// snapshot.  Later terminal operations update the subscription snapshot
    /// directly, so this is only the reset/initial materialization source.
    pub(crate) fn structured_app_rows_in_terminal_order(&self) -> Vec<(RowUuid, OwnedRecord)> {
        self.structured_app_row_order
            .iter()
            .filter_map(|root| self.structured_app_row(*root).map(|record| (*root, record)))
            .collect()
    }

    /// Return the opaque collector key for one retained root. A root collector
    /// may only expose a reset once this association is exact: the key carries
    /// joined occurrence identity which is deliberately absent from the public
    /// app-row record.
    pub(crate) fn structured_terminal_root_key(
        &self,
        root: RowUuid,
    ) -> Result<&[u8], super::Error> {
        let mut keys = self
            .structured_root_keys
            .iter()
            .filter_map(|(key, candidate)| (*candidate == root).then_some(key.as_slice()));
        let Some(key) = keys.next() else {
            return Err(super::Error::InvalidStoredValue(
                "collector root snapshot has no terminal key",
            ));
        };
        if keys.next().is_some() {
            return Err(super::Error::InvalidStoredValue(
                "collector root snapshot has ambiguous terminal keys",
            ));
        }
        Ok(key)
    }

    /// Release the app-row collector after a flat subscription's reset has
    /// been materialized. Only structured array output reads this state after
    /// opening; flat subscriptions publish subsequent rows from membership
    /// and version witnesses.
    pub(crate) fn discard_structured_app_rows(&mut self) {
        self.structured_app_rows.clear();
        self.structured_app_row_order.clear();
        self.structured_root_keys.clear();
        self.structured_app_row_descriptor = None;
        self.retains_structured_app_rows = false;
    }

    fn apply_structured_app_row_delta(&mut self, root: RowUuid, record: OwnedRecord, weight: i64) {
        self.structured_app_row_descriptor = Some(*record.descriptor());
        let records = self.structured_app_rows.entry(root).or_default();
        let new_weight = records.get(record.raw()).copied().unwrap_or(0) + weight;
        if new_weight == 0 {
            records.remove(record.raw());
        } else {
            records.insert(record.into_raw(), new_weight);
        }
        if records.is_empty() {
            self.structured_app_rows.remove(&root);
            self.structured_app_row_order
                .retain(|candidate| candidate != &root);
        } else if !self.structured_app_row_order.contains(&root) {
            self.structured_app_row_order.push(root);
        }
    }

    /// Fold root terminal edits into the same retained collector tree used by
    /// an initial/reset snapshot. This is deliberately receiver-local: it
    /// never re-runs the query or reads authority output.
    fn apply_structured_terminal_operation(
        &mut self,
        operation: &TerminalOperation,
    ) -> Result<(), super::Error> {
        if !operation.path.is_empty() {
            return Ok(());
        }
        let root = match &operation.edit {
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
                let record = OwnedRecord::new(value.clone(), operation.root_descriptor);
                self.structured_app_rows.remove(&root);
                self.apply_structured_app_row_delta(root, record, 1);
                self.structured_app_row_order
                    .retain(|candidate| candidate != &root);
                self.structured_app_row_order
                    .insert((*index).min(self.structured_app_row_order.len()), root);
            }
            TerminalEdit::Update { value, .. } => {
                let record = OwnedRecord::new(value.clone(), operation.root_descriptor);
                self.structured_app_rows.remove(&root);
                self.apply_structured_app_row_delta(root, record, 1);
            }
            TerminalEdit::Remove { .. } => {
                self.structured_root_keys.remove(&operation.root_key);
                self.structured_app_rows.remove(&root);
                self.structured_app_row_order
                    .retain(|candidate| candidate != &root);
            }
            TerminalEdit::Move { index, .. } => {
                let previous = self
                    .structured_app_row_order
                    .iter()
                    .position(|candidate| candidate == &root)
                    .ok_or(super::Error::InvalidStoredValue(
                        "root collector terminal move addresses an absent root",
                    ))?;
                self.structured_app_row_order.remove(previous);
                self.structured_app_row_order
                    .insert((*index).min(self.structured_app_row_order.len()), root);
            }
        }
        Ok(())
    }

    fn replace_structured_app_row_order_from_snapshot(
        &mut self,
        schema: &AppRowSchema,
        deltas: &RecordDeltas,
    ) -> Result<(), super::Error> {
        let row_uuid =
            schema
                .descriptor
                .field_index("row_uuid")
                .ok_or(super::Error::InvalidStoredValue(
                    "root collector terminal has no row_uuid field",
                ))?;
        let mut order = Vec::new();
        for (record, weight) in deltas.iter() {
            if weight <= 0 {
                continue;
            }
            let root = RowUuid(record.get_uuid(row_uuid)?);
            if !order.contains(&root) {
                order.push(root);
            }
        }
        self.structured_app_row_order = order;
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
        self.published_result_members = publishable;
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
        self.versions_by_tx(tx_id).iter().any(|version| {
            version.table() == table.as_str()
                && version.row_uuid() == row_uuid
                && version.deletion().is_none()
        }) || self
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
/// present nullable cell.  Nested edits remain byte-addressed by their root
/// descriptor, so they must already agree exactly.
fn rebind_terminal_operation_to_layout(
    operation: &TerminalOperation,
    layout: &TerminalRootLayout,
) -> Result<TerminalOperation, super::Error> {
    if operation.root_descriptor == layout.root_descriptor {
        return Ok(operation.clone());
    }
    if !terminal_descriptor_can_rebind_to_layout(
        &operation.root_descriptor,
        &layout.root_descriptor,
    ) || (!operation.path.is_empty() && operation.root_descriptor != layout.root_descriptor)
    {
        return Err(super::Error::InvalidStoredValue(
            "structured terminal operation descriptor disagrees with prepared root layout",
        ));
    }

    let mut rebound = operation.clone();
    match &mut rebound.edit {
        TerminalEdit::Insert { value, .. } | TerminalEdit::Update { value, .. } => {
            *value = reencode_terminal_root_record(
                operation.root_descriptor,
                &layout.root_descriptor,
                value,
            )?;
        }
        TerminalEdit::Remove { .. } | TerminalEdit::Move { .. } => {}
    }
    rebound.root_descriptor = layout.root_descriptor;
    Ok(rebound)
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
                    crate::node::query_engine::AppRowTerminal::Aggregate(schema) => {
                        MaintainedTerminalKind::AggregateAppRows(schema.clone())
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
            let kind = match (&fact.key, fact.terminal, &fact.schema) {
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
    let root_key_slot = rows
        .descriptor
        .field_index("row_uuid")
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
            (slot != root_key_slot && !rows.hidden_fields.contains(name)).then(|| {
                let carrier = rows
                    .field_carriers
                    .get(name)
                    .copied()
                    .unwrap_or(rows.carrier);
                TerminalRootPublicField {
                    // The compiler binds this identity before terminal
                    // publication. Carrier describes encoding, not naming:
                    // a logical include may legitimately begin with `user_`.
                    name: rows.public_field_names.get(name).cloned().unwrap_or_else(
                        || match carrier {
                            AppRowCarrier::CurrentRow => logical_user_column(name).to_owned(),
                            AppRowCarrier::Logical => name.to_owned(),
                        },
                    ),
                    descriptor_field_name: name.to_owned(),
                    slot,
                    carrier: match carrier {
                        AppRowCarrier::CurrentRow => TerminalRootCarrier::CurrentRow,
                        AppRowCarrier::Logical => TerminalRootCarrier::Logical,
                    },
                }
            })
        })
        .collect::<Vec<_>>();
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"jazz terminal root layout v1");
    hasher.update(
        &encode_record_descriptor(&rows.descriptor)
            .expect("terminal layouts contain valid Groove record descriptors"),
    );
    hasher.update(&(root_key_slot as u64).to_le_bytes());
    hasher.update(&[match rows.carrier {
        AppRowCarrier::CurrentRow => 0,
        AppRowCarrier::Logical => 1,
    }]);
    for field in &public_fields {
        hasher.update(field.name.as_bytes());
        hasher.update(&[0]);
        hasher.update(field.descriptor_field_name.as_bytes());
        hasher.update(&[0]);
        hasher.update(&(field.slot as u64).to_le_bytes());
        hasher.update(&[match field.carrier {
            TerminalRootCarrier::CurrentRow => 0,
            TerminalRootCarrier::Logical => 1,
        }]);
    }
    TerminalRootLayout {
        id: format!("terminal:{}", hasher.finalize().to_hex()),
        root_descriptor: rows.descriptor,
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
        )
    }
}

fn decode_typed_terminal_record(
    record: BorrowedRecord<'_>,
    kind: &MaintainedTerminalKind,
    tables: &TableSchemas,
    node_aliases: &BTreeMap<NodeUuid, NodeAlias>,
    decode_plan_cache: &mut VersionDecodePlanCache,
) -> Result<DecodedMaintainedEvent, super::Error> {
    match kind {
        MaintainedTerminalKind::AggregateAppRows(schema) => {
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
                // members and durable receipts do not churn merely because
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
            member.branch_or_prefix = branch_or_prefix;
            let member: ResultMemberEntry = match flat_join_digest {
                Some(digest) => member.with_row_digest(digest),
                None => member,
            }
            .into();
            let payload = ResultMemberPayloadEntry {
                member: member.clone(),
                descriptor: encode_record_descriptor(&record.descriptor())?,
                record: record.raw().to_vec(),
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
            let row = settled_result_value_storage_bytes(&row_value, &row_type)?;
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
            let replacement =
                settled_result_value_storage_bytes(&replacement_value, &replacement_type)?;
            let member = ResultMemberEntry::Synthetic {
                table,
                row,
                replacement: SyntheticReplacementToken::from_encoded_record(replacement),
            };
            let payload = ResultMemberPayloadEntry {
                member: member.clone(),
                descriptor: encode_record_descriptor(&record.descriptor())?,
                record: record.raw().to_vec(),
            };
            Ok(DecodedMaintainedEvent::AggregateResult {
                member,
                payload,
                synthetic: schema.synthetic.clone(),
                value_fields: schema
                    .value_fields
                    .iter()
                    .map(|field| field.name.clone())
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
            let index =
                descriptor
                    .field_index(&group.name)
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
    let row = settled_result_value_storage_bytes(&row_value, &row_type)?;
    let (replacement_value, replacement_type) = match schema.value_fields.first() {
        Some(output) => {
            let index =
                descriptor
                    .field_index(&output.name)
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
    let replacement = settled_result_value_storage_bytes(&replacement_value, &replacement_type)?;
    let member = ResultMemberEntry::Synthetic {
        table: "aggregate_result".to_owned(),
        row,
        replacement: SyntheticReplacementToken::from_encoded_record(replacement),
    };
    let payload = ResultMemberPayloadEntry {
        member: member.clone(),
        descriptor: encode_record_descriptor(&descriptor)?,
        record: record.raw().to_vec(),
    };
    Ok(DecodedMaintainedEvent::AggregateResult {
        member,
        payload,
        synthetic: schema.synthetic.clone(),
        value_fields: schema
            .value_fields
            .iter()
            .map(|field| field.name.clone())
            .collect(),
    })
}

/// Domain separation for the durable flat-join result revision.
///
/// `ResultMemberEntry::row_digest` is persisted as part of settled result and
/// program-fact state, so this must not inherit Rust/postcard layout. The
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
    let descriptor_bytes = encode_record_descriptor(&descriptor)?;
    // The public payload descriptor is the durable contract. An inner join may
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
    let mut cells = BTreeMap::new();
    if layer == VersionLayer::Content {
        for column in &table.columns {
            if let Some(value) = nullable_value(record.get_idx(plan.user_indices[&column.name])?)? {
                cells.insert(column.name.clone(), value);
            }
        }
    }
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
        created_by: AuthorSubject::from_canonical(record.get_str(plan.created_by_idx)?)
            .map_err(|_| groove::records::Error::NonCanonicalRecord)?,
        // Current-row provenance is public Unix milliseconds. Witness state
        // needs the corresponding history form only to identify/materialize
        // the authored version, whose provenance HLC always has counter zero.
        created_at: TxTime::from_physical_ms(record_u64_idx(record, plan.created_at_idx)?)
            .map_err(|_| {
                super::Error::InvalidStoredValue(
                    "maintained witness created_at_ms exceeds packed HLC range",
                )
            })?,
        updated_by: AuthorSubject::from_canonical(record.get_str(plan.updated_by_idx)?)
            .map_err(|_| groove::records::Error::NonCanonicalRecord)?,
        updated_at: TxTime::from_physical_ms(record_u64_idx(record, plan.updated_at_idx)?)
            .map_err(|_| {
                super::Error::InvalidStoredValue(
                    "maintained witness updated_at_ms exceeds packed HLC range",
                )
            })?,
        cells,
        authored_columns,
        deletion,
    };
    let values = if layer == VersionLayer::Content {
        history_values_from_parts(table, &parts)?
    } else {
        register_values_from_parts(&parts)?
    };
    let version = VersionRow {
        table: groove::Intern::new(parts.table),
        branch_key: parts.branch_key,
        record: owned_record_from_storage_values_with_descriptor(plan.descriptor, values)?,
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
        btree_map_bytes(self.by_identity.len())
            + self
                .by_identity
                .iter()
                .map(|(identity, version)| {
                    version_identity_bytes(identity) + weighted_version_bytes(version)
                })
                .sum::<usize>()
            + btree_map_bytes(self.by_tx.len())
            + self
                .by_tx
                .values()
                .map(|by_sort_key| {
                    btree_map_bytes(by_sort_key.len())
                        + by_sort_key
                            .iter()
                            .map(|(sort_key, identities)| {
                                version_sort_key_bytes(sort_key)
                                    + btree_set_bytes(identities.len())
                                    + identities.iter().map(version_identity_bytes).sum::<usize>()
                            })
                            .sum::<usize>()
                })
                .sum::<usize>()
    }

    fn apply_delta(
        &mut self,
        identity: VersionIdentity,
        row: VersionRow,
        weight: i64,
        node_aliases: &BTreeMap<NodeUuid, NodeAlias>,
    ) -> Result<(), super::Error> {
        let old = self
            .by_identity
            .get(&identity)
            .map(|version| version.weight)
            .unwrap_or(0);
        let tx_id = version_tx_id_from_aliases(&row, node_aliases).ok_or(
            super::Error::InvalidStoredValue("history tx node alias must exist"),
        )?;
        let sort_key = VersionSortKey::for_row(&row);
        let new = old + weight;

        if old <= 0 && new > 0 {
            self.by_tx
                .entry(tx_id)
                .or_default()
                .entry(sort_key.clone())
                .or_default()
                .insert(identity.clone());
        }
        if old > 0
            && new <= 0
            && let Some(existing) = self.by_identity.get(&identity)
        {
            remove_tx_identity(
                &mut self.by_tx,
                existing.tx_id,
                &existing.sort_key,
                &identity,
            );
        }

        if new > 0 {
            self.by_identity.insert(
                identity,
                WeightedVersion {
                    row,
                    tx_id,
                    sort_key,
                    weight: new,
                },
            );
        } else {
            self.by_identity.remove(&identity);
        }
        Ok(())
    }

    fn versions_by_tx(&self, tx_id: TxId) -> Vec<VersionRow> {
        let Some(by_sort_key) = self.by_tx.get(&tx_id) else {
            return Vec::new();
        };
        by_sort_key
            .values()
            .flat_map(|identities| {
                identities.iter().filter_map(|identity| {
                    self.by_identity
                        .get(identity)
                        .filter(|version| version.weight > 0)
                        .map(|version| version.row.clone())
                })
            })
            .collect()
    }
}

impl ReplacementIndex {
    fn footprint_bytes(&self) -> usize {
        replacement_map_bytes(&self.content_by_key) + replacement_map_bytes(&self.deletion_by_key)
    }

    fn apply_delta(
        &mut self,
        key: ReplacementKey,
        identity: VersionIdentity,
        row: VersionRow,
        weight: i64,
        node_aliases: &BTreeMap<NodeUuid, NodeAlias>,
    ) -> Result<(), super::Error> {
        let by_key = match key.layer {
            VersionLayer::Content => &mut self.content_by_key,
            VersionLayer::Deletion => &mut self.deletion_by_key,
        };
        let row_versions = by_key.entry(key.clone()).or_default();
        let old = row_versions
            .get(&identity)
            .map(|version| version.weight)
            .unwrap_or(0);
        let new = old + weight;
        if new > 0 {
            let tx_id = version_tx_id_from_aliases(&row, node_aliases).ok_or(
                super::Error::InvalidStoredValue("history tx node alias must exist"),
            )?;
            row_versions.insert(
                identity,
                WeightedVersion {
                    sort_key: VersionSortKey::for_row(&row),
                    row,
                    tx_id,
                    weight: new,
                },
            );
        } else {
            row_versions.remove(&identity);
        }
        if row_versions.is_empty() {
            by_key.remove(&key);
        }
        Ok(())
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
        self.content_by_key
            .values()
            .chain(self.deletion_by_key.values())
            .map(BTreeMap::len)
            .sum()
    }
}

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
    mem::size_of_val(identity)
        + intern_string_bytes(&identity.table)
        + vec_bytes(&identity.raw_record)
}

fn version_sort_key_bytes(sort_key: &VersionSortKey) -> usize {
    mem::size_of_val(sort_key)
        + intern_string_bytes(&sort_key.table)
        + vec_bytes(&sort_key.raw_record)
}

fn replacement_key_bytes(key: &ReplacementKey) -> usize {
    mem::size_of_val(key) + intern_string_bytes(&key.table)
}

fn weighted_version_bytes(version: &WeightedVersion) -> usize {
    mem::size_of_val(version)
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
            raw_record: row.record.raw().to_vec(),
        }
    }
}

impl VersionSortKey {
    fn for_row(row: &VersionRow) -> Self {
        Self {
            table: row.table,
            row_uuid: row.row_uuid(),
            layer: row.layer(),
            raw_record: row.record.raw().to_vec(),
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
            Self::Result(entry, _) => EventIdentity::Result(entry.clone()),
            Self::AggregateResult(member, ..) => EventIdentity::Result(member.clone()),
            Self::Version(source, identity, _) => {
                EventIdentity::Version(source.clone(), identity.clone())
            }
            Self::Replacement(source, key, identity, _) => {
                EventIdentity::Replacement(source.clone(), key.clone(), identity.clone())
            }
            Self::ProgramFact(fact) => EventIdentity::ProgramFact(fact.clone()),
            Self::StructuredAppRow(root, record) => {
                EventIdentity::StructuredAppRow(*root, record.raw().to_vec())
            }
        }
    }
}

fn remove_tx_identity(
    by_tx: &mut BTreeMap<TxId, BTreeMap<VersionSortKey, BTreeSet<VersionIdentity>>>,
    tx_id: TxId,
    sort_key: &VersionSortKey,
    identity: &VersionIdentity,
) {
    let Some(by_sort_key) = by_tx.get_mut(&tx_id) else {
        return;
    };
    if let Some(identities) = by_sort_key.get_mut(sort_key) {
        identities.remove(identity);
        if identities.is_empty() {
            by_sort_key.remove(sort_key);
        }
    }
    if by_sort_key.is_empty() {
        by_tx.remove(&tx_id);
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
    use std::collections::BTreeMap;

    use groove::ivm::RecordDelta;
    use groove::records::{Value, ValueType};
    use groove::schema::ColumnType;

    use super::*;
    use crate::ids::{NodeUuid, SchemaVersionAlias};
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

    // Internal receipt: `row_digest` is a durable settled-result identity, so
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
        let mut maintained = MaintainedSubscriptionView::default();
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
        };
        let layout = terminal_root_layout(&rows);
        assert_eq!(
            layout.public_fields,
            vec![
                TerminalRootPublicField {
                    name: "title".to_owned(),
                    descriptor_field_name: "user_title".to_owned(),
                    slot: 1,
                    carrier: TerminalRootCarrier::CurrentRow,
                },
                TerminalRootPublicField {
                    name: "__jazz_include_project".to_owned(),
                    descriptor_field_name: "user___jazz_include_project".to_owned(),
                    slot: 2,
                    carrier: TerminalRootCarrier::CurrentRow,
                },
                TerminalRootPublicField {
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
        }
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

        let rebound = rebind_terminal_operation_to_layout(&operation, &layout(target)).unwrap();
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

        let rebound = rebind_terminal_operation_to_layout(&operation, &layout(target)).unwrap();
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
            rebind_terminal_operation_to_layout(&operation, &layout(target)),
            Err(Error::InvalidStoredValue(
                "structured terminal operation descriptor disagrees with prepared root layout"
            ))
        ));
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
                created_by: AuthorSubject::SYSTEM,
                created_at: TxTime(time),
                updated_by: AuthorSubject::SYSTEM,
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
                created_by: AuthorSubject::SYSTEM,
                created_at: TxTime(time),
                updated_by: AuthorSubject::SYSTEM,
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

    fn replacement_content(row: VersionRow) -> DecodedMaintainedEvent {
        DecodedMaintainedEvent::ReplacementContent {
            source: test_source(),
            row,
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
        let mut maintained = MaintainedSubscriptionView::default();

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
        let mut maintained = MaintainedSubscriptionView::default();

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

    #[test]
    fn storage_backed_membership_publishes_without_bundle_witness() {
        // This must remain an internal receipt: public test routes cannot
        // synthesize the deliberately omitted Stream B terminal. The
        // storage-backed subset instead resolves the exact member `(table,
        // row, tx)` from node storage at materialization time.
        let aliases = aliases();
        let member = ResultMemberEntry::from(result(row(2), 20));
        let mut maintained = MaintainedSubscriptionView::default();
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
        let mut maintained = MaintainedSubscriptionView::default();

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
        let mut maintained = MaintainedSubscriptionView::default();

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
        let mut maintained = MaintainedSubscriptionView::default();
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

        let mut reopened = MaintainedSubscriptionView::default();
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
        let mut maintained = MaintainedSubscriptionView::default();

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
        let mut maintained = MaintainedSubscriptionView::default();

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

    #[test]
    fn versions_by_tx_contains_distinct_identities_sorted_and_prunes_retracted_one() {
        let aliases = aliases();
        let tx_id = tx(1, 10);
        let row_b = row(2);
        let row_a = row(1);
        let version_b = version(row_b, 10, "b");
        let version_a = version(row_a, 10, "a");
        let mut maintained = MaintainedSubscriptionView::default();

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
        let mut maintained = MaintainedSubscriptionView::default();

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
        let mut maintained = MaintainedSubscriptionView::default();

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
