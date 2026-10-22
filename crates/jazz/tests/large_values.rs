//! Chunked large values on SQLite: a `string`, `json` or `bytes` value above
//! the inline threshold is stored as a descriptor over a content-defined
//! chunk tree, and still reads, filters, orders and indexes as its logical
//! value (Groove SPEC ch. 9, `INV-LARGE-1`).

mod common;

use std::collections::BTreeMap;
use std::path::PathBuf;

use jazz::db::{Db, DbConfig, DbIdentity, FileOptions, InsertOptions, ReadOpts, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, contains, eq, lit};
use jazz::records::Value;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

/// Above the 64 KiB inline threshold, and several leaves long.
const LARGE: usize = 300 * 1024;

fn config(indexed: bool) -> DbConfig {
    let mut docs = TableSchemaBuilder::new("docs")
        .column("body", ColumnType::Text)
        .column("blob", ColumnType::Bytea)
        .policies(allow_all_policies());
    if indexed {
        docs = docs.index_only(["body"]);
    }
    DbConfig::new(
        compile_schema(&SchemaBuilder::new().table(docs).build()),
        DbIdentity {
            node: NodeUuid::from_bytes([3; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
}

fn db() -> Db {
    block_on(Db::open(config(false))).unwrap()
}

fn row(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}

/// Deterministic text without repeats, so the chunker cuts it into many
/// content-defined leaves.
fn text(seed: u64, len: usize) -> String {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (b'a' + (state % 26) as u8) as char
        })
        .collect()
}

fn insert(db: &Db, n: u8, body: &str, blob: &[u8]) {
    block_on(db.insert(
        "docs",
        BTreeMap::from([
            ("body".to_owned(), Value::String(body.to_owned())),
            ("blob".to_owned(), Value::Bytes(blob.to_vec())),
        ]),
        InsertOptions {
            row_id: Some(row(n)),
            ..Default::default()
        },
    ))
    .unwrap();
}

fn cell(row: &jazz::db::CurrentRow, name: &str) -> Value {
    row.cells()
        .find(|(n, _)| *n == name)
        .and_then(|(_, value)| value.cloned())
        .unwrap()
}

fn ids(db: &Db, query: Query) -> Vec<RowUuid> {
    let prepared = db.prepare_query(&query).unwrap();
    block_on(db.all(&prepared, ReadOpts::default()))
        .unwrap()
        .iter()
        .map(|row| row.row_uuid())
        .collect()
}

#[test]
fn large_values_are_chunked_and_read_back_whole() {
    let db = db();
    let body = text(1, LARGE);
    let blob: Vec<u8> = text(2, LARGE)
        .into_bytes()
        .into_iter()
        .map(|b| b ^ 0x80)
        .collect();
    insert(&db, 1, &body, &blob);
    insert(&db, 2, "small", b"\x00\xff");

    let (nodes, roots) = db.large_value_nodes_for_test().unwrap();
    assert!(nodes > 4, "both values span several nodes: {nodes}");
    assert_eq!(roots, 2, "one chunked root per large cell");

    let prepared = db
        .prepare_query(&Query::from("docs").order_by("id", OrderDirection::Asc))
        .unwrap();
    let rows = block_on(db.all(&prepared, ReadOpts::default())).unwrap();
    assert_eq!(cell(&rows[0], "body"), Value::String(body));
    assert_eq!(cell(&rows[0], "blob"), Value::Bytes(blob));
    assert_eq!(cell(&rows[1], "body"), Value::String("small".to_owned()));
    assert_eq!(cell(&rows[1], "blob"), Value::Bytes(b"\x00\xff".to_vec()));
}

#[test]
fn equality_compares_logical_values_across_arms() {
    let db = db();
    let body = text(3, LARGE);
    insert(&db, 1, &body, b"");
    insert(&db, 2, &text(4, LARGE), b"");
    insert(&db, 3, "short", b"");
    // The literal is bound inline; the stored cell is chunked.
    let found = ids(
        &db,
        Query::from("docs").filter(eq(col("body"), lit(Value::String(body.clone())))),
    );
    assert_eq!(found, vec![row(1)]);
    // Same prefix, one byte longer: not equal.
    let longer = format!("{body}x");
    let found = ids(
        &db,
        Query::from("docs").filter(eq(col("body"), lit(Value::String(longer)))),
    );
    assert!(found.is_empty());
}

fn ordered(db: &Db) -> Vec<RowUuid> {
    ids(
        db,
        Query::from("docs").order_by("body", OrderDirection::Asc),
    )
}

#[test]
fn ordering_and_indexes_follow_logical_bytes() {
    for indexed in [false, true] {
        let db = block_on(Db::open(config(indexed))).unwrap();
        let base = text(5, LARGE);
        // Values that share long prefixes, differ deep inside a chunk, or
        // only in length, mixed with inline values.
        let mut deep = base.clone().into_bytes();
        deep[LARGE - 10] = b'A';
        let values = [
            base.clone(),
            format!("{base}z"),
            String::from_utf8(deep).unwrap(),
            base[..LARGE / 2].to_owned(),
            "zzz".to_owned(),
            base[..10].to_owned(),
            text(6, LARGE),
        ];
        for (n, value) in values.iter().enumerate() {
            insert(&db, n as u8 + 1, value, b"");
        }
        let mut expected: Vec<(String, RowUuid)> = values
            .iter()
            .enumerate()
            .map(|(n, value)| (value.clone(), row(n as u8 + 1)))
            .collect();
        expected.sort();
        assert_eq!(
            ordered(&db),
            expected.iter().map(|(_, id)| *id).collect::<Vec<_>>(),
            "indexed: {indexed}"
        );
        // A range seek on the column reads through the same collation.
        let found = ids(
            &db,
            Query::from("docs")
                .filter(jazz::query::gt(
                    col("body"),
                    lit(Value::String(base.clone())),
                ))
                .order_by("body", OrderDirection::Asc),
        );
        let greater: Vec<RowUuid> = expected
            .iter()
            .filter(|(value, _)| *value > base)
            .map(|(_, id)| *id)
            .collect();
        assert_eq!(found, greater, "indexed: {indexed}");
    }
}

#[test]
fn contains_scans_chunked_values_across_boundaries() {
    let db = db();
    let body = text(7, LARGE);
    let blob = text(8, LARGE).into_bytes();
    insert(&db, 1, &body, &blob);
    insert(&db, 2, "nothing here", b"none");
    for at in [0, 1, 64 * 1024 - 3, 100_000, LARGE - 20] {
        let needle = body[at..at + 20].to_owned();
        assert_eq!(
            ids(
                &db,
                Query::from("docs").filter(contains(col("body"), lit(Value::String(needle))))
            ),
            vec![row(1)],
            "string needle at {at}"
        );
        let needle = blob[at..at + 20].to_vec();
        assert_eq!(
            ids(
                &db,
                Query::from("docs").filter(contains(col("blob"), lit(Value::Bytes(needle))))
            ),
            vec![row(1)],
            "bytes needle at {at}"
        );
    }
    let absent = ids(
        &db,
        Query::from("docs").filter(contains(col("body"), lit(Value::String("0".repeat(5))))),
    );
    assert!(absent.is_empty());
}

#[test]
fn history_versions_keep_their_own_roots() {
    let db = db();
    insert(&db, 1, &text(9, LARGE), b"");
    block_on(db.update(
        "docs",
        row(1),
        BTreeMap::from([("body".to_owned(), Value::String(text(10, LARGE)))]),
        Default::default(),
    ))
    .unwrap();
    let (_, roots) = db.large_value_nodes_for_test().unwrap();
    assert_eq!(roots, 2, "the superseded version still references its root");
    block_on(db.tick()).unwrap();
    let (_, roots) = db.large_value_nodes_for_test().unwrap();
    assert_eq!(roots, 2, "nothing referenced is reclaimed");
    // Shrinking a large value to an inline one releases only the current
    // cell's reference.
    block_on(db.update(
        "docs",
        row(1),
        BTreeMap::from([("body".to_owned(), Value::String("now small".to_owned()))]),
        Default::default(),
    ))
    .unwrap();
    let (_, roots) = db.large_value_nodes_for_test().unwrap();
    assert_eq!(roots, 2, "both large versions are history now");
    let prepared = db.prepare_query(&Query::from("docs")).unwrap();
    let rows = block_on(db.all(&prepared, ReadOpts::default())).unwrap();
    assert_eq!(
        cell(&rows[0], "body"),
        Value::String("now small".to_owned())
    );
}

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("jazz-large-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("store.sqlite")
}

#[test]
fn a_file_store_reads_chunks_back_after_reopening() {
    let path = scratch("reopen");
    let values: Vec<String> = (0..4).map(|n| text(20 + n, LARGE)).collect();
    {
        let db = block_on(Db::open_at(config(true), &path, FileOptions::default())).unwrap();
        for (n, value) in values.iter().enumerate() {
            insert(&db, n as u8 + 1, value, b"");
        }
    }
    let db = block_on(Db::open_at(config(true), &path, FileOptions::default())).unwrap();
    let mut expected: Vec<(String, RowUuid)> = values
        .iter()
        .enumerate()
        .map(|(n, value)| (value.clone(), row(n as u8 + 1)))
        .collect();
    expected.sort();
    assert_eq!(
        ordered(&db),
        expected.iter().map(|(_, id)| *id).collect::<Vec<_>>()
    );
    let found = ids(
        &db,
        Query::from("docs").filter(eq(col("body"), lit(Value::String(values[2].clone())))),
    );
    assert_eq!(found, vec![row(3)]);
}

#[test]
fn large_columns_never_reach_sqlite_text_functions() {
    let db = db();
    for query in [
        Query::from("docs").filter(contains(col("body"), lit(Value::String("x".into())))),
        Query::from("docs").filter(contains(col("blob"), lit(Value::Bytes(vec![1])))),
        Query::from("docs").filter(eq(col("body"), lit(Value::String("x".into())))),
        Query::from("docs").order_by("body", OrderDirection::Desc),
    ] {
        let prepared = db.prepare_query(&query).unwrap();
        let (sql, _) = db.explain_query_plan(&prepared, None).unwrap();
        let lower = sql.to_lowercase();
        for builtin in [
            "instr(", "length(", "substr(", "like ", "glob ", "lower(", "upper(", "json_", "hex(",
            "trim(", "replace(",
        ] {
            assert!(!lower.contains(builtin), "{builtin} in {sql}");
        }
    }
}
