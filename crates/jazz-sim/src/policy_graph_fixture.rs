use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use jazz::groove::records::Value;
use serde_json::Value as JsonValue;

pub const MEMBER_SEED_ROWS_JSON: &str = "member-seed-rows.json";
pub const MEMBER_SEED_ROWS_COMPACT_JSON: &str = "member-seed-rows.compact.json";
pub const MEMBER_SEED_ROWS_COMPACT_ENCODING: &str = "policy-graph-member-seed-compact-v1";

#[derive(Clone, Debug, PartialEq)]
pub struct MemberSeedDump {
    pub identity: MemberSeedIdentity,
    pub manifest: MemberSeedManifest,
    pub rows: Vec<SeedRow>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MemberSeedIdentity {
    pub member_row: String,
    pub claims: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SeedRow {
    pub table: String,
    pub id: String,
    pub cells: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MemberSeedManifest {
    pub tables: Vec<ManifestTable>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ManifestTable {
    pub name: String,
    pub expected: usize,
}

pub fn member_seed_dump_from_path(path: &Path) -> MemberSeedDump {
    let bytes = fs::read(path).expect("read policy graph member seed row dump");
    let value: JsonValue =
        serde_json::from_slice(&bytes).expect("decode policy graph perf member seed row dump");
    member_seed_dump_from_json(value)
}

pub fn member_seed_dump_from_json(value: JsonValue) -> MemberSeedDump {
    match value.get("encoding").and_then(JsonValue::as_str) {
        Some(MEMBER_SEED_ROWS_COMPACT_ENCODING) => expand_compact_member_seed_dump(&value),
        Some(other) => panic!("unsupported policy graph member seed row encoding {other:?}"),
        None => decode_legacy_member_seed_dump(&value),
    }
}

impl MemberSeedManifest {
    pub fn expected_counts(&self) -> BTreeMap<String, usize> {
        self.tables
            .iter()
            .map(|table| (table.name.clone(), table.expected))
            .collect()
    }
}

fn decode_legacy_member_seed_dump(value: &JsonValue) -> MemberSeedDump {
    let (member_row, claims, tables) = decode_manifest(value);
    let rows = value
        .get("rows")
        .and_then(JsonValue::as_array)
        .expect("member seed row dump rows")
        .iter()
        .map(|row| SeedRow {
            table: row
                .get("table")
                .and_then(JsonValue::as_str)
                .expect("seed row table")
                .to_owned(),
            id: row
                .get("id")
                .and_then(JsonValue::as_str)
                .expect("seed row id")
                .to_owned(),
            cells: row
                .get("cells")
                .and_then(JsonValue::as_object)
                .expect("seed row cells")
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        })
        .collect::<Vec<_>>();
    MemberSeedDump {
        identity: MemberSeedIdentity { member_row, claims },
        manifest: MemberSeedManifest { tables },
        rows,
    }
}

fn expand_compact_member_seed_dump(value: &JsonValue) -> MemberSeedDump {
    let (member_row, claims, tables) = decode_manifest(value);
    let rows = value
        .get("tables")
        .and_then(JsonValue::as_array)
        .expect("compact member seed row dump tables")
        .iter()
        .flat_map(expand_compact_table)
        .collect::<Vec<_>>();
    MemberSeedDump {
        identity: MemberSeedIdentity { member_row, claims },
        manifest: MemberSeedManifest { tables },
        rows,
    }
}

fn decode_manifest(value: &JsonValue) -> (String, BTreeMap<String, Value>, Vec<ManifestTable>) {
    let identity = value
        .get("identity")
        .expect("member seed row dump identity");
    let member_row = identity
        .get("member_row")
        .and_then(JsonValue::as_str)
        .expect("member seed row dump member_row")
        .to_owned();
    let claims = identity
        .get("claims")
        .and_then(JsonValue::as_object)
        .expect("member seed row dump claims")
        .iter()
        .map(|(key, value)| (key.clone(), json_to_claim_value(value, &member_row)))
        .collect::<BTreeMap<_, _>>();
    let tables = value
        .get("subscriptions")
        .and_then(JsonValue::as_array)
        .expect("member seed row dump subscriptions")
        .iter()
        .map(|table| {
            let name = table
                .get("name")
                .and_then(JsonValue::as_str)
                .expect("manifest table name")
                .to_owned();
            let expected = table
                .get("expected")
                .and_then(JsonValue::as_u64)
                .expect("manifest table expected") as usize;
            ManifestTable { name, expected }
        })
        .collect::<Vec<_>>();
    assert_eq!(
        tables.len(),
        39,
        "member seed row dump must cover all 39 subscriptions"
    );
    (member_row, claims, tables)
}

fn expand_compact_table(table: &JsonValue) -> Vec<SeedRow> {
    let name = table
        .get("name")
        .and_then(JsonValue::as_str)
        .expect("compact table name")
        .to_owned();
    let columns = table
        .get("columns")
        .and_then(JsonValue::as_array)
        .expect("compact table columns")
        .iter()
        .map(|column| {
            column
                .as_str()
                .expect("compact table column string")
                .to_owned()
        })
        .collect::<Vec<_>>();
    let values = table
        .get("values")
        .and_then(JsonValue::as_array)
        .expect("compact table values");
    assert_eq!(
        values.len(),
        columns.len(),
        "compact table {name} column/value dictionary length mismatch"
    );
    let row_values = table
        .get("rows")
        .and_then(JsonValue::as_array)
        .expect("compact table rows");
    let ids = table.get("ids").and_then(JsonValue::as_array);
    let id_prefix = table
        .get("id_prefix")
        .and_then(JsonValue::as_str)
        .map(str::to_owned);
    let id_start = table.get("id_start").and_then(JsonValue::as_u64);
    if let Some(ids) = ids {
        assert_eq!(
            ids.len(),
            row_values.len(),
            "compact table {name} id/row length mismatch"
        );
    } else {
        id_prefix
            .as_ref()
            .expect("compact table id_prefix without ids");
        id_start.expect("compact table id_start without ids");
    }

    row_values
        .iter()
        .enumerate()
        .map(|(row_idx, row)| {
            let row = row
                .as_array()
                .unwrap_or_else(|| panic!("compact table {name} row must be an array"));
            assert_eq!(
                row.len(),
                columns.len(),
                "compact table {name} row width mismatch at {row_idx}"
            );
            let id = match ids {
                Some(ids) => ids[row_idx]
                    .as_str()
                    .expect("compact explicit row id")
                    .to_owned(),
                None => format!(
                    "{}{:012}",
                    id_prefix.as_ref().expect("compact id_prefix"),
                    id_start.expect("compact id_start") + row_idx as u64
                ),
            };
            let cells = columns
                .iter()
                .enumerate()
                .filter_map(|(column_idx, column)| {
                    let encoded = &row[column_idx];
                    if encoded.is_null() {
                        return None;
                    }
                    let value_idx = encoded
                        .as_u64()
                        .unwrap_or_else(|| panic!("compact table {name} value index")) as usize;
                    let value = values[column_idx]
                        .as_array()
                        .unwrap_or_else(|| {
                            panic!("compact table {name} column {column} value dictionary")
                        })
                        .get(value_idx)
                        .unwrap_or_else(|| {
                            panic!(
                                "compact table {name} column {column} value index {value_idx} out of bounds"
                            )
                        })
                        .clone();
                    Some((column.clone(), value))
                })
                .collect::<BTreeMap<_, _>>();
            SeedRow {
                table: name.clone(),
                id,
                cells,
            }
        })
        .collect()
}

fn json_to_claim_value(value: &JsonValue, member_row: &str) -> Value {
    match value {
        JsonValue::Bool(value) => Value::Bool(*value),
        JsonValue::String(value) if value == member_row => {
            Value::Uuid(uuid::Uuid::parse_str(value).expect("claim member uuid"))
        }
        JsonValue::String(value) => Value::String(value.clone()),
        other => panic!("unsupported identity claim value {other:?}"),
    }
}
