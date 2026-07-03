use std::collections::{BTreeMap, BTreeSet};

use groove::ivm::{MultisinkDeltas, RecordDeltas};
use groove::records::{BorrowedRecord, RecordDescriptor, Value};

use super::codec::{
    VersionLayer, VersionRow, VersionRowParts, deletion_event_from_value, nullable_value,
    tx_ids_from_value, version_tx_id_from_aliases,
};
use super::query_engine::{
    AggregateResultSchema, OutputTerminalSchema, ProgramFactKey, ProgramFactSchema,
    ProgramFactTerminal, QueryProgram, ResultMembershipSchema, VersionWitnessSchema,
};
use crate::ids::{AuthorId, NodeAlias, NodeUuid, RowUuid};
use crate::protocol::{
    ProgramFactEntry, RealRowMemberEntry, ResultMemberEntry, ResultMemberPayloadEntry,
};
use crate::schema::TableSchema;
use crate::time::{GlobalSeq, TxTime};
use crate::tx::TxId;

type TableSchemas = BTreeMap<String, TableSchema>;

#[derive(Clone, Debug, Default)]
pub(crate) struct MaintainedSubscriptionView {
    result_weights: BTreeMap<ResultMemberEntry, i64>,
    result_payloads: BTreeMap<ResultMemberEntry, ResultMemberPayloadEntry>,
    versions: WeightedVersionIndex,
    replacements: ReplacementIndex,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct MaintainedSubscriptionViewFootprint {
    pub(crate) result_rows: usize,
    pub(crate) version_identities: usize,
    pub(crate) version_tx_entries: usize,
    pub(crate) replacement_entries: usize,
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
    pub(crate) adds: Vec<ResultMemberEntry>,
    pub(crate) removes: Vec<ResultMemberEntry>,
    pub(crate) program_fact_adds: Vec<ProgramFactEntry>,
    pub(crate) program_fact_removes: Vec<ProgramFactEntry>,
}

#[derive(Clone, Debug)]
pub(crate) enum DecodedMaintainedEvent {
    ResultCurrent(ResultMemberEntry),
    AggregateResult {
        member: ResultMemberEntry,
        payload: ResultMemberPayloadEntry,
        synthetic: super::query_engine::SyntheticResultMembershipSchema,
        value_fields: Vec<String>,
    },
    VersionContent(VersionRow),
    VersionDeletion(VersionRow),
    ReplacementContent(VersionRow),
    ReplacementDeletion(VersionRow),
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
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum EventIdentity {
    Result(ResultMemberEntry),
    Version(VersionIdentity),
    Replacement(ReplacementKey, VersionIdentity),
}

#[derive(Clone, Debug)]
enum NetEvent {
    Result(ResultMemberEntry),
    AggregateResult(
        ResultMemberEntry,
        ResultMemberPayloadEntry,
        super::query_engine::SyntheticResultMembershipSchema,
        Vec<String>,
    ),
    Version(VersionIdentity, VersionRow),
    Replacement(ReplacementKey, VersionIdentity, VersionRow),
}

impl MaintainedSubscriptionView {
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
        let decoded = deltas
            .iter()
            .map(|(record, weight)| {
                decode_typed_terminal_record(record, kind, tables, node_aliases)
                    .map(|event| (event, weight))
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.apply_decoded_deltas(decoded, node_aliases)
    }

    pub(crate) fn apply_multisink_deltas(
        &mut self,
        deltas: MultisinkDeltas,
        schemas: &MaintainedTerminalSchemas,
        tables: &TableSchemas,
        node_aliases: &BTreeMap<NodeUuid, NodeAlias>,
    ) -> Result<ResultTransitions, super::Error> {
        let mut transitions = ResultTransitions::default();
        for (sink, deltas) in deltas.sinks {
            let delta_transitions =
                self.apply_typed_deltas(&sink, &deltas, schemas, tables, node_aliases)?;
            transitions.adds.extend(delta_transitions.adds);
            transitions.removes.extend(delta_transitions.removes);
            transitions
                .program_fact_adds
                .extend(delta_transitions.program_fact_adds);
            transitions
                .program_fact_removes
                .extend(delta_transitions.program_fact_removes);
        }
        Ok(transitions)
    }

    pub(crate) fn apply_decoded_deltas(
        &mut self,
        rows: impl IntoIterator<Item = (DecodedMaintainedEvent, i64)>,
        node_aliases: &BTreeMap<NodeUuid, NodeAlias>,
    ) -> Result<ResultTransitions, super::Error> {
        let mut net = BTreeMap::<EventIdentity, (NetEvent, i64)>::new();
        for (event, weight) in rows {
            let net_event = match event {
                DecodedMaintainedEvent::ResultCurrent(entry) => NetEvent::Result(entry),
                DecodedMaintainedEvent::AggregateResult {
                    member,
                    payload,
                    synthetic,
                    value_fields,
                } => NetEvent::AggregateResult(member, payload, synthetic, value_fields),
                DecodedMaintainedEvent::VersionContent(row)
                | DecodedMaintainedEvent::VersionDeletion(row) => {
                    let identity = VersionIdentity::for_row(&row);
                    NetEvent::Version(identity, row)
                }
                DecodedMaintainedEvent::ReplacementContent(row) => {
                    let identity = VersionIdentity::for_row(&row);
                    let key = ReplacementKey::for_row(&row, VersionLayer::Content);
                    NetEvent::Replacement(key, identity, row)
                }
                DecodedMaintainedEvent::ReplacementDeletion(row) => {
                    let identity = VersionIdentity::for_row(&row);
                    let key = ReplacementKey::for_row(&row, VersionLayer::Deletion);
                    NetEvent::Replacement(key, identity, row)
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
            match event {
                NetEvent::Result(entry) => {
                    self.apply_result_delta(entry, weight, &mut transitions);
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
                NetEvent::Version(identity, row) => {
                    self.versions
                        .apply_delta(identity, row, weight, node_aliases)?;
                }
                NetEvent::Replacement(key, identity, row) => {
                    self.replacements
                        .apply_delta(key, identity, row, weight, node_aliases)?;
                }
            }
        }
        Ok(transitions)
    }

    pub(crate) fn versions_by_tx(&self, tx_id: TxId) -> Vec<VersionRow> {
        self.versions.versions_by_tx(tx_id)
    }

    pub(crate) fn replacement_for(
        &self,
        table: &str,
        row_uuid: RowUuid,
    ) -> (Option<VersionRow>, Option<VersionRow>) {
        self.replacements.replacement_for(table, row_uuid)
    }

    pub(crate) fn footprint(&self) -> MaintainedSubscriptionViewFootprint {
        MaintainedSubscriptionViewFootprint {
            result_rows: self
                .result_weights
                .values()
                .filter(|weight| **weight > 0)
                .count(),
            version_identities: self.versions.by_identity.len(),
            version_tx_entries: self
                .versions
                .by_tx
                .values()
                .flat_map(|by_sort_key| by_sort_key.values())
                .map(BTreeSet::len)
                .sum(),
            replacement_entries: self.replacements.entry_count(),
        }
    }

    pub(crate) fn payload_facts_for_members(
        &self,
        members: &[ResultMemberEntry],
    ) -> Vec<ProgramFactEntry> {
        members
            .iter()
            .filter_map(|member| self.result_payloads.get(member))
            .cloned()
            .map(ProgramFactEntry::ResultPayload)
            .collect()
    }

    fn apply_result_delta(
        &mut self,
        entry: ResultMemberEntry,
        weight: i64,
        transitions: &mut ResultTransitions,
    ) {
        let old = self.result_weights.get(&entry).copied().unwrap_or(0);
        let new = old + weight;
        if old <= 0 && new > 0 {
            transitions.adds.push(entry.clone());
        }
        if old > 0 && new <= 0 {
            transitions.removes.push(entry.clone());
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
        synthetic: &super::query_engine::SyntheticResultMembershipSchema,
        value_fields: &[String],
        weight: i64,
        transitions: &mut ResultTransitions,
    ) -> Result<(), super::Error> {
        let (old_member, old_payload) = self.aggregate_payload_for_stable_member(&member);
        let materialized = materialize_aggregate_payload_delta(
            old_payload.as_ref(),
            payload,
            synthetic,
            value_fields,
            weight,
        )?;
        if aggregate_payload_is_empty(&materialized, value_fields)? {
            if let Some(old_member) = old_member {
                transitions.removes.push(old_member.clone());
                self.result_weights.remove(&old_member);
                if let Some(existing) = self.result_payloads.remove(&old_member) {
                    transitions
                        .program_fact_removes
                        .push(ProgramFactEntry::ResultPayload(existing));
                }
            }
            return Ok(());
        }
        if let Some(old_member) = old_member
            && old_member != materialized.member
        {
            transitions.removes.push(old_member.clone());
            self.result_weights.remove(&old_member);
            if let Some(existing) = self.result_payloads.remove(&old_member) {
                transitions
                    .program_fact_removes
                    .push(ProgramFactEntry::ResultPayload(existing));
            }
        }
        let old = self
            .result_weights
            .get(&materialized.member)
            .copied()
            .unwrap_or(0);
        if old <= 0 {
            transitions.adds.push(materialized.member.clone());
        }
        transitions
            .program_fact_adds
            .push(ProgramFactEntry::ResultPayload(materialized.clone()));
        self.result_payloads
            .insert(materialized.member.clone(), materialized.clone());
        self.result_weights.insert(materialized.member, 1);
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

impl MaintainedTerminalSchemas {
    fn for_program(program: &QueryProgram) -> Self {
        let mut sinks = BTreeMap::new();
        for terminal in &program.lowered.terminals {
            let OutputTerminalSchema::Fact(fact) = &terminal.output else {
                continue;
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
}

fn decode_typed_terminal_record(
    record: BorrowedRecord<'_>,
    kind: &MaintainedTerminalKind,
    tables: &TableSchemas,
    node_aliases: &BTreeMap<NodeUuid, NodeAlias>,
) -> Result<DecodedMaintainedEvent, super::Error> {
    match kind {
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
                .map(|field| nullable_u64(record, field).map(|seq| seq.map(GlobalSeq)))
                .transpose()?
                .flatten();
            Ok(DecodedMaintainedEvent::ResultCurrent(
                RealRowMemberEntry::current_content((
                    table.name.clone().into(),
                    row_uuid,
                    TxId::new(tx_time, tx_node),
                ))
                .with_settle_position(settle_position)
                .into(),
            ))
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
            let row_value = record.get_idx(field_idx(record, &schema.synthetic.row_field)?)?;
            let row = postcard::to_allocvec(&row_value).map_err(|_| {
                super::Error::InvalidStoredValue("aggregate result row encoding failed")
            })?;
            let revision_value =
                record.get_idx(field_idx(record, &schema.synthetic.revision_field)?)?;
            let revision = postcard::to_allocvec(&revision_value).map_err(|_| {
                super::Error::InvalidStoredValue("aggregate result revision encoding failed")
            })?;
            let member = ResultMemberEntry::Synthetic {
                table,
                row,
                revision,
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
        MaintainedTerminalKind::VersionContent(schema) => {
            validate_witness_event_kind(record, "version_content")?;
            decode_typed_version_witness(record, schema, tables)
                .map(DecodedMaintainedEvent::VersionContent)
        }
        MaintainedTerminalKind::VersionDeletion(schema) => {
            validate_witness_event_kind(record, "version_deletion")?;
            decode_typed_version_witness(record, schema, tables)
                .map(DecodedMaintainedEvent::VersionDeletion)
        }
        MaintainedTerminalKind::ReplacementContent(schema) => {
            validate_witness_event_kind(record, "replacement_content")?;
            decode_typed_version_witness(record, schema, tables)
                .map(DecodedMaintainedEvent::ReplacementContent)
        }
        MaintainedTerminalKind::ReplacementDeletion(schema) => {
            validate_witness_event_kind(record, "replacement_deletion")?;
            decode_typed_version_witness(record, schema, tables)
                .map(DecodedMaintainedEvent::ReplacementDeletion)
        }
    }
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
    let mut cells = BTreeMap::new();
    for column in &table.columns {
        let field =
            schema
                .user_fields
                .get(&column.name)
                .ok_or(super::Error::InvalidStoredValue(
                    "maintained witness schema missing user field",
                ))?;
        if let Some(value) = nullable_value(record.get_idx(field_idx(record, field)?)?)? {
            cells.insert(column.name.clone(), value);
        }
    }
    let tx_time = TxTime(record_u64(record, &schema.identity.tx_time_field)?);
    VersionRow::from_parts_with_schema_version(
        table,
        VersionRowParts {
            table: table.name.clone(),
            row_uuid: RowUuid(record.get_uuid(field_idx(record, &schema.identity.row_field)?)?),
            tx_node_alias: NodeAlias(record_u64(record, &schema.identity.tx_node_field)?),
            schema_version_alias: crate::ids::SchemaVersionAlias(record_u64(
                record,
                &schema.identity.schema_field,
            )?),
            tx_time,
            parents: tx_ids_from_value(record.get_idx(field_idx(record, &schema.parents_field)?)?)?,
            created_by: AuthorId(record.get_uuid(field_idx(record, &schema.created_by_field)?)?),
            created_at: TxTime(record_u64(record, &schema.created_at_field)?),
            updated_by: AuthorId(record.get_uuid(field_idx(record, &schema.updated_by_field)?)?),
            updated_at: TxTime(record_u64(record, &schema.updated_at_field)?),
            cells,
            deletion,
        },
        None,
    )
}

fn tagged_deletion(value: Value) -> Result<Option<crate::tx::DeletionEvent>, super::Error> {
    match value {
        Value::Nullable(None) => Ok(None),
        Value::Nullable(Some(value)) => {
            let value = match *value {
                Value::U8(discriminant) => Value::Enum(discriminant),
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

impl WeightedVersionIndex {
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
            Self::Result(entry) => EventIdentity::Result(entry.clone()),
            Self::AggregateResult(member, ..) => EventIdentity::Result(member.clone()),
            Self::Version(identity, _) => EventIdentity::Version(identity.clone()),
            Self::Replacement(key, identity, _) => {
                EventIdentity::Replacement(key.clone(), identity.clone())
            }
        }
    }
}

fn materialize_aggregate_payload_delta(
    previous: Option<&ResultMemberPayloadEntry>,
    delta: ResultMemberPayloadEntry,
    synthetic: &super::query_engine::SyntheticResultMembershipSchema,
    value_fields: &[String],
    weight: i64,
) -> Result<ResultMemberPayloadEntry, super::Error> {
    let delta_descriptor = decode_payload_descriptor(&delta.descriptor)?;
    let delta_record = BorrowedRecord::new(&delta.record, &delta_descriptor);
    let mut values = if let Some(previous) = previous {
        let previous_descriptor = decode_payload_descriptor(&previous.descriptor)?;
        BorrowedRecord::new(&previous.record, &previous_descriptor)
            .to_values()
            .map_err(|_| super::Error::InvalidStoredValue("aggregate payload decode failed"))?
    } else {
        delta_record
            .to_values()
            .map_err(|_| super::Error::InvalidStoredValue("aggregate payload decode failed"))?
    };
    if previous.is_none() {
        for field in value_fields {
            let idx = field_idx(delta_record, field)?;
            values[idx] = zero_aggregate_value(values[idx].clone());
        }
    }
    for field in value_fields {
        let idx = field_idx(delta_record, field)?;
        let delta_value = delta_record.get_idx(idx)?;
        values[idx] = apply_aggregate_value_delta(values[idx].clone(), delta_value, weight)?;
    }
    if let Some(first_value_field) = value_fields.first() {
        let value_idx = field_idx(delta_record, first_value_field)?;
        let revision_idx = field_idx(delta_record, &synthetic.revision_field)?;
        values[revision_idx] = values[value_idx].clone();
    }
    let raw = delta_descriptor
        .create(&values)
        .map_err(|_| super::Error::InvalidStoredValue("aggregate payload encoding failed"))?;
    let revision_value = values[field_idx(delta_record, &synthetic.revision_field)?].clone();
    let revision = postcard::to_allocvec(&revision_value).map_err(|_| {
        super::Error::InvalidStoredValue("aggregate result revision encoding failed")
    })?;
    let member = match delta.member {
        ResultMemberEntry::Synthetic { table, row, .. } => ResultMemberEntry::Synthetic {
            table,
            row,
            revision,
        },
        _ => delta.member,
    };
    Ok(ResultMemberPayloadEntry {
        member,
        descriptor: delta.descriptor,
        record: raw,
    })
}

fn decode_payload_descriptor(bytes: &[u8]) -> Result<RecordDescriptor, super::Error> {
    let fields: Vec<(Option<String>, groove::records::ValueType)> = postcard::from_bytes(bytes)
        .map_err(|_| super::Error::InvalidStoredValue("aggregate descriptor decoding failed"))?;
    Ok(RecordDescriptor::new(fields.into_iter().map(
        |(name, value_type)| (name.unwrap_or_default(), value_type),
    )))
}

fn apply_aggregate_value_delta(
    current: Value,
    delta: Value,
    weight: i64,
) -> Result<Value, super::Error> {
    macro_rules! signed_int {
        ($value:expr, $variant:ident, $ty:ty) => {{
            let next = ($value as i128)
                .checked_add(
                    (weight as i128)
                        * (match delta {
                            Value::$variant(value) => value as i128,
                            _ => return Ok(delta),
                        }),
                )
                .ok_or(super::Error::InvalidStoredValue("aggregate value overflow"))?;
            Value::$variant(
                <$ty>::try_from(next)
                    .map_err(|_| super::Error::InvalidStoredValue("aggregate value overflow"))?,
            )
        }};
    }
    Ok(match current {
        Value::U8(value) => signed_int!(value, U8, u8),
        Value::U16(value) => signed_int!(value, U16, u16),
        Value::U32(value) => signed_int!(value, U32, u32),
        Value::U64(value) => signed_int!(value, U64, u64),
        Value::F64(value) => match delta {
            Value::F64(delta) => Value::F64(value + (weight as f64) * delta),
            _ => delta,
        },
        _ => delta,
    })
}

fn zero_aggregate_value(value: Value) -> Value {
    match value {
        Value::U8(_) => Value::U8(0),
        Value::U16(_) => Value::U16(0),
        Value::U32(_) => Value::U32(0),
        Value::U64(_) => Value::U64(0),
        Value::F64(_) => Value::F64(0.0),
        other => other,
    }
}

fn aggregate_payload_is_empty(
    payload: &ResultMemberPayloadEntry,
    value_fields: &[String],
) -> Result<bool, super::Error> {
    if value_fields.is_empty() {
        return Ok(false);
    }
    let descriptor = decode_payload_descriptor(&payload.descriptor)?;
    let record = BorrowedRecord::new(&payload.record, &descriptor);
    for field in value_fields {
        let value = record.get_idx(field_idx(record, field)?)?;
        match value {
            Value::U8(0) | Value::U16(0) | Value::U32(0) | Value::U64(0) => {}
            Value::F64(0.0) => {}
            _ => return Ok(false),
        }
    }
    Ok(true)
}

fn encode_record_descriptor(descriptor: &RecordDescriptor) -> Result<Vec<u8>, super::Error> {
    let fields = descriptor
        .fields()
        .iter()
        .map(|field| (field.name.clone(), field.value_type.clone()))
        .collect::<Vec<_>>();
    postcard::to_allocvec(&fields)
        .map_err(|_| super::Error::InvalidStoredValue("aggregate descriptor encoding failed"))
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

    use groove::records::Value;
    use groove::schema::ColumnType;

    use super::*;
    use crate::ids::{NodeUuid, SchemaVersionAlias};
    use crate::node::codec::{VersionRow, VersionRowParts};
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

    fn table() -> TableSchema {
        TableSchema::new("todos", [ColumnSchema::new("title", ColumnType::String)])
    }

    fn version(row_uuid: RowUuid, time: u64, title: &str) -> VersionRow {
        VersionRow::from_parts_with_schema_version(
            &table(),
            VersionRowParts {
                table: "todos".to_owned(),
                row_uuid,
                tx_node_alias: NodeAlias(10),
                schema_version_alias: SchemaVersionAlias(0),
                tx_time: TxTime(time),
                parents: Vec::new(),
                created_by: AuthorId::SYSTEM,
                created_at: TxTime(time),
                updated_by: AuthorId::SYSTEM,
                updated_at: TxTime(time),
                cells: BTreeMap::from([("title".to_owned(), Value::String(title.to_owned()))]),
                deletion: None,
            },
            None,
        )
        .unwrap()
    }

    fn deletion(row_uuid: RowUuid, time: u64) -> VersionRow {
        VersionRow::from_parts_with_schema_version(
            &table(),
            VersionRowParts {
                table: "todos".to_owned(),
                row_uuid,
                tx_node_alias: NodeAlias(10),
                schema_version_alias: SchemaVersionAlias(0),
                tx_time: TxTime(time),
                parents: Vec::new(),
                created_by: AuthorId::SYSTEM,
                created_at: TxTime(time),
                updated_by: AuthorId::SYSTEM,
                updated_at: TxTime(time),
                cells: BTreeMap::new(),
                deletion: Some(DeletionEvent::Deleted),
            },
            None,
        )
        .unwrap()
    }

    fn result(row_uuid: RowUuid, time: u64) -> ResultRowEntry {
        ("todos".to_owned().into(), row_uuid, tx(1, time))
    }

    #[test]
    fn result_single_enter_then_leave_emits_add_then_remove() {
        let aliases = aliases();
        let entry = result(row(1), 10);
        let member = ResultMemberEntry::from(entry);
        let mut maintained = MaintainedSubscriptionView::default();

        let first = maintained
            .apply_decoded_deltas(
                [(DecodedMaintainedEvent::ResultCurrent(member.clone()), 1)],
                &aliases,
            )
            .unwrap();
        assert_eq!(first.adds, vec![member.clone()]);
        assert!(first.removes.is_empty());

        let second = maintained
            .apply_decoded_deltas(
                [(DecodedMaintainedEvent::ResultCurrent(member.clone()), -1)],
                &aliases,
            )
            .unwrap();
        assert!(second.adds.is_empty());
        assert_eq!(second.removes, vec![member]);
        assert!(maintained.result_weights.is_empty());
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
                    (DecodedMaintainedEvent::ResultCurrent(member.clone()), 1),
                    (DecodedMaintainedEvent::ResultCurrent(member.clone()), 1),
                    (DecodedMaintainedEvent::ResultCurrent(member.clone()), -1),
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
            .apply_decoded_deltas(
                [(DecodedMaintainedEvent::ResultCurrent(member.clone()), 2)],
                &aliases,
            )
            .unwrap();
        assert_eq!(active.adds, vec![member.clone()]);
        assert!(active.removes.is_empty());

        let inactive = maintained
            .apply_decoded_deltas(
                [(DecodedMaintainedEvent::ResultCurrent(member.clone()), -2)],
                &aliases,
            )
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
                    (DecodedMaintainedEvent::VersionContent(version_b.clone()), 1),
                    (DecodedMaintainedEvent::VersionContent(version_a.clone()), 1),
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
            .apply_decoded_deltas(
                [(
                    DecodedMaintainedEvent::VersionContent(version_a.clone()),
                    -1,
                )],
                &aliases,
            )
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
            .apply_decoded_deltas(
                [(DecodedMaintainedEvent::ReplacementContent(old.clone()), 1)],
                &aliases,
            )
            .unwrap();
        assert_eq!(
            maintained.replacement_for("todos", row_uuid).0,
            Some(old.clone())
        );

        maintained
            .apply_decoded_deltas(
                [
                    (DecodedMaintainedEvent::ReplacementContent(old), -1),
                    (DecodedMaintainedEvent::ReplacementContent(new.clone()), 1),
                ],
                &aliases,
            )
            .unwrap();
        assert_eq!(
            maintained.replacement_for("todos", row_uuid),
            (Some(new), None)
        );

        maintained
            .apply_decoded_deltas(
                [(
                    DecodedMaintainedEvent::ReplacementDeletion(deletion.clone()),
                    1,
                )],
                &aliases,
            )
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
            .apply_decoded_deltas(
                [(DecodedMaintainedEvent::VersionDeletion(version.clone()), 1)],
                &aliases,
            )
            .unwrap();
        assert_eq!(maintained.versions_by_tx(tx_id), vec![version.clone()]);

        maintained
            .apply_decoded_deltas(
                [(DecodedMaintainedEvent::VersionDeletion(version), -1)],
                &aliases,
            )
            .unwrap();
        assert!(maintained.versions_by_tx(tx_id).is_empty());
        assert!(!maintained.versions.by_tx.contains_key(&tx_id));
    }
}
