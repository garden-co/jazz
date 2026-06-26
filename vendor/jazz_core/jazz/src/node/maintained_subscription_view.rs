use std::collections::{BTreeMap, BTreeSet};

use groove::ivm::RecordDeltas;
use groove::records::{BorrowedRecord, Value};

use super::codec::{
    VersionLayer, VersionRow, VersionRowParts, deletion_event_from_value, nullable_value,
    tx_ids_from_value, version_tx_id_from_aliases,
};
use super::query_eval::maintained_view_tagged_user_field;
use crate::ids::{NodeAlias, NodeUuid, RowUuid};
use crate::protocol::ResultRowEntry;
use crate::schema::TableSchema;
use crate::time::TxTime;
use crate::tx::TxId;

type TableSchemas = BTreeMap<String, TableSchema>;

#[derive(Clone, Debug, Default)]
pub(crate) struct MaintainedSubscriptionView {
    result_weights: BTreeMap<ResultRowEntry, i64>,
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
    pub(crate) adds: Vec<ResultRowEntry>,
    pub(crate) removes: Vec<ResultRowEntry>,
}

#[derive(Clone, Debug)]
pub(crate) enum DecodedMaintainedEvent {
    ResultCurrent(ResultRowEntry),
    VersionContent(VersionRow),
    VersionDeletion(VersionRow),
    ReplacementContent(VersionRow),
    ReplacementDeletion(VersionRow),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum EventIdentity {
    Result(ResultRowEntry),
    Version(VersionIdentity),
    Replacement(ReplacementKey, VersionIdentity),
}

#[derive(Clone, Debug)]
enum NetEvent {
    Result(ResultRowEntry),
    Version(VersionIdentity, VersionRow),
    Replacement(ReplacementKey, VersionIdentity, VersionRow),
}

impl MaintainedSubscriptionView {
    pub(crate) fn apply_tagged_deltas(
        &mut self,
        deltas: &RecordDeltas,
        tables: &TableSchemas,
        node_aliases: &BTreeMap<NodeUuid, NodeAlias>,
    ) -> Result<ResultTransitions, super::Error> {
        let decoded = deltas
            .iter()
            .map(|(record, weight)| {
                decode_tagged_terminal_record(record, tables, node_aliases)
                    .map(|event| (event, weight))
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.apply_decoded_deltas(decoded, node_aliases)
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

    #[cfg(test)]
    pub(super) fn active_result_entries(&self) -> BTreeSet<ResultRowEntry> {
        self.result_weights
            .iter()
            .filter(|(_, weight)| **weight > 0)
            .map(|(entry, _)| *entry)
            .collect()
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

    fn apply_result_delta(
        &mut self,
        entry: ResultRowEntry,
        weight: i64,
        transitions: &mut ResultTransitions,
    ) {
        let old = self.result_weights.get(&entry).copied().unwrap_or(0);
        let new = old + weight;
        if old <= 0 && new > 0 {
            transitions.adds.push(entry);
        }
        if old > 0 && new <= 0 {
            transitions.removes.push(entry);
        }
        if new == 0 {
            self.result_weights.remove(&entry);
        } else {
            self.result_weights.insert(entry, new);
        }
    }
}

fn decode_tagged_terminal_record(
    record: BorrowedRecord<'_>,
    tables: &TableSchemas,
    node_aliases: &BTreeMap<NodeUuid, NodeAlias>,
) -> Result<DecodedMaintainedEvent, super::Error> {
    let event_kind = match record.get_idx(field_idx(record, "event_kind")?)? {
        Value::String(value) => value,
        _ => {
            return Err(super::Error::InvalidStoredValue(
                "event_kind must be string",
            ));
        }
    };
    let table_name = match record.get_idx(field_idx(record, "table_name")?)? {
        Value::String(value) => value,
        _ => {
            return Err(super::Error::InvalidStoredValue(
                "table_name must be string",
            ));
        }
    };
    let table = tables
        .get(&table_name)
        .ok_or(super::Error::InvalidStoredValue(
            "maintained view tagged terminal table_name must exist",
        ))?;

    if event_kind == "result_current" {
        let row_uuid = RowUuid(record.get_uuid(field_idx(record, "row_uuid")?)?);
        let tx_time = TxTime(record_u64(record, "content_tx_time")?);
        let tx_node_alias = NodeAlias(record_u64(record, "content_tx_node_id")?);
        let tx_node = node_aliases
            .iter()
            .find_map(|(node, alias)| (*alias == tx_node_alias).then_some(*node))
            .ok_or(super::Error::InvalidStoredValue(
                "result tx node alias must exist",
            ))?;
        return Ok(DecodedMaintainedEvent::ResultCurrent((
            table.name.clone().into(),
            row_uuid,
            TxId::new(tx_time, tx_node),
        )));
    }

    let version = decode_tagged_terminal_version(record, table)?;
    match event_kind.as_str() {
        "version_content" => Ok(DecodedMaintainedEvent::VersionContent(version)),
        "version_deletion" => Ok(DecodedMaintainedEvent::VersionDeletion(version)),
        "replacement_content" => Ok(DecodedMaintainedEvent::ReplacementContent(version)),
        "replacement_deletion" => Ok(DecodedMaintainedEvent::ReplacementDeletion(version)),
        _ => Err(super::Error::InvalidStoredValue(
            "unknown maintained view tagged event kind",
        )),
    }
}

fn decode_tagged_terminal_version(
    record: BorrowedRecord<'_>,
    table: &TableSchema,
) -> Result<VersionRow, super::Error> {
    let deletion = tagged_deletion(record.get_idx(field_idx(record, "_deletion")?)?)?;
    let mut cells = BTreeMap::new();
    for column in &table.columns {
        let field = maintained_view_tagged_user_field(&table.name, &column.name);
        if let Some(value) = nullable_value(record.get_idx(field_idx(record, &field)?)?)? {
            cells.insert(column.name.clone(), value);
        }
    }
    VersionRow::from_parts_with_schema_version(
        table,
        VersionRowParts {
            table: table.name.clone(),
            row_uuid: RowUuid(record.get_uuid(field_idx(record, "row_uuid")?)?),
            tx_node_alias: NodeAlias(record_u64(record, "tx_node_id")?),
            schema_version_alias: crate::ids::SchemaVersionAlias(record_u64(
                record,
                "schema_version",
            )?),
            tx_time: TxTime(record_u64(record, "tx_time")?),
            parents: tx_ids_from_value(record.get_idx(field_idx(record, "parents")?)?)?,
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

fn field_idx(record: BorrowedRecord<'_>, field: &str) -> Result<usize, super::Error> {
    record
        .descriptor()
        .field_index(field)
        .ok_or(super::Error::InvalidStoredValue(
            "maintained view tagged terminal missing field",
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
            Self::Result(entry) => EventIdentity::Result(*entry),
            Self::Version(identity, _) => EventIdentity::Version(identity.clone()),
            Self::Replacement(key, identity, _) => {
                EventIdentity::Replacement(key.clone(), identity.clone())
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
    debug_assert!(
        versions
            .values()
            .filter(|version| version.weight > 0)
            .count()
            <= 1,
        "maintained view replacement stream produced multiple active winners"
    );
    versions
        .values()
        .find(|version| version.weight > 0)
        .map(|version| version.row.clone())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use groove::ivm::{RecordDelta, RecordDeltas};
    use groove::records::{RecordDescriptor, Value, ValueType};
    use groove::schema::ColumnType;

    use super::*;
    use crate::ids::{NodeUuid, SchemaVersionAlias};
    use crate::node::codec::{VersionRow, VersionRowParts};
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

    fn tables() -> BTreeMap<String, TableSchema> {
        let table = table();
        BTreeMap::from([(table.name.clone(), table)])
    }

    fn tagged_descriptor() -> RecordDescriptor {
        RecordDescriptor::new([
            ("event_kind", ValueType::String),
            ("table_name", ValueType::String),
            ("row_uuid", ValueType::Uuid),
            ("content_tx_time", ValueType::U64),
            ("content_tx_node_id", ValueType::U64),
            ("tx_time", ValueType::U64),
            ("tx_node_id", ValueType::U64),
            ("schema_version", ValueType::U64),
            (
                "parents",
                ValueType::Array(Box::new(ValueType::Tuple(vec![
                    ValueType::U64,
                    ValueType::Uuid,
                ]))),
            ),
            ("_deletion", ValueType::Nullable(Box::new(ValueType::U8))),
            (
                "user_title",
                ValueType::Nullable(Box::new(ValueType::String)),
            ),
        ])
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
    fn tagged_record_deltas_decode_and_apply_to_maintained_indexes() {
        let aliases = aliases();
        let descriptor = tagged_descriptor();
        let row_uuid = row(1);
        let raw = descriptor
            .create(&[
                Value::String("version_content".to_owned()),
                Value::String("todos".to_owned()),
                Value::Uuid(row_uuid.0),
                Value::U64(10),
                Value::U64(10),
                Value::U64(10),
                Value::U64(10),
                Value::U64(0),
                Value::Array(Vec::new()),
                Value::Nullable(None),
                Value::Nullable(Some(Box::new(Value::String("decoded".to_owned())))),
            ])
            .unwrap();
        let deltas = RecordDeltas {
            descriptor,
            deltas: vec![RecordDelta {
                record: raw,
                weight: 1,
            }],
        };
        let mut maintained = MaintainedSubscriptionView::default();

        let transitions = maintained
            .apply_tagged_deltas(&deltas, &tables(), &aliases)
            .unwrap();

        assert_eq!(transitions, ResultTransitions::default());
        assert_eq!(
            maintained.versions_by_tx(tx(1, 10)),
            vec![version(row_uuid, 10, "decoded")]
        );
    }

    #[test]
    fn result_single_enter_then_leave_emits_add_then_remove() {
        let aliases = aliases();
        let entry = result(row(1), 10);
        let mut maintained = MaintainedSubscriptionView::default();

        let first = maintained
            .apply_decoded_deltas(
                [(DecodedMaintainedEvent::ResultCurrent(entry), 1)],
                &aliases,
            )
            .unwrap();
        assert_eq!(first.adds, vec![entry]);
        assert!(first.removes.is_empty());

        let second = maintained
            .apply_decoded_deltas(
                [(DecodedMaintainedEvent::ResultCurrent(entry), -1)],
                &aliases,
            )
            .unwrap();
        assert!(second.adds.is_empty());
        assert_eq!(second.removes, vec![entry]);
        assert!(maintained.result_weights.is_empty());
    }

    #[test]
    fn result_non_consolidated_drain_nets_to_one_add() {
        let aliases = aliases();
        let entry = result(row(1), 10);
        let mut maintained = MaintainedSubscriptionView::default();

        let transitions = maintained
            .apply_decoded_deltas(
                [
                    (DecodedMaintainedEvent::ResultCurrent(entry), 1),
                    (DecodedMaintainedEvent::ResultCurrent(entry), 1),
                    (DecodedMaintainedEvent::ResultCurrent(entry), -1),
                ],
                &aliases,
            )
            .unwrap();

        assert_eq!(transitions.adds, vec![entry]);
        assert!(transitions.removes.is_empty());
        assert_eq!(maintained.result_weights.get(&entry), Some(&1));
    }

    #[test]
    fn result_weight_magnitude_greater_than_one_tracks_active_membership() {
        let aliases = aliases();
        let entry = result(row(1), 10);
        let mut maintained = MaintainedSubscriptionView::default();

        let active = maintained
            .apply_decoded_deltas(
                [(DecodedMaintainedEvent::ResultCurrent(entry), 2)],
                &aliases,
            )
            .unwrap();
        assert_eq!(active.adds, vec![entry]);
        assert!(active.removes.is_empty());

        let inactive = maintained
            .apply_decoded_deltas(
                [(DecodedMaintainedEvent::ResultCurrent(entry), -2)],
                &aliases,
            )
            .unwrap();
        assert!(inactive.adds.is_empty());
        assert_eq!(inactive.removes, vec![entry]);
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
