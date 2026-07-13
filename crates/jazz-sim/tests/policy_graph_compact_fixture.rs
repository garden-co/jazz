use std::collections::BTreeSet;
use std::path::PathBuf;

use jazz_sim::policy_graph_fixture::{
    MEMBER_SEED_ROWS_COMPACT_ENCODING, MEMBER_SEED_ROWS_COMPACT_JSON, member_seed_dump_from_path,
};
use serde_json::Value as JsonValue;

fn public_fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../packages/jazz-tools/src/testing/fixtures/policy-graph-perf")
}

#[test]
fn public_policy_graph_compact_fixture_expands_to_expected_shape() {
    let path = public_fixture_dir().join(MEMBER_SEED_ROWS_COMPACT_JSON);
    let raw = std::fs::read(&path).expect("read compact policy graph fixture");
    let value: JsonValue =
        serde_json::from_slice(&raw).expect("parse compact policy graph fixture");
    assert_eq!(
        value.get("encoding").and_then(JsonValue::as_str),
        Some(MEMBER_SEED_ROWS_COMPACT_ENCODING)
    );
    assert_eq!(
        value.get("expanded_row_count").and_then(JsonValue::as_u64),
        Some(21_154)
    );

    let dump = member_seed_dump_from_path(&path);
    assert_eq!(dump.rows.len(), 21_154);
    assert_eq!(dump.manifest.tables.len(), 39);
    assert_eq!(
        dump.manifest
            .tables
            .iter()
            .map(|table| table.expected)
            .sum::<usize>(),
        20_726
    );

    let row_tables = dump
        .rows
        .iter()
        .map(|row| row.table.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(row_tables.len(), 53);
    assert_eq!(
        dump.rows.iter().filter(|row| row.table == "t67").count(),
        19_894
    );
}
