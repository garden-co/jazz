//! Chunked large values: a `string`, `json` or `bytes` value above
//! the inline threshold is stored as a descriptor over a content-defined
//! chunk tree, and still reads, filters, orders and indexes as its logical
//! value (`INV-CONTENT-1`).

use crate::common;

use std::collections::BTreeMap;
use std::path::PathBuf;

use jazz::db::{
    Db, DbConfig, DbIdentity, FileOptions, InsertOptions, Journal, JsonSetEdit, LargeValueUpdate,
    LargeValueUpdatePage, LargeValueUpdateSplice, ReadOpts, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, contains, eq, lit};
use jazz::records::Value;
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};

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
    let notes = TableSchemaBuilder::new("notes")
        .column("doc", ColumnType::Json { schema: None })
        .policies(allow_all_policies());
    DbConfig::new(
        compile_schema(&SchemaBuilder::new().table(docs).table(notes).build()),
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
        // A bounded page cuts inside values that share their whole sort
        // prefix, in both directions.
        for (direction, mut want) in [
            (OrderDirection::Asc, expected.clone()),
            (
                OrderDirection::Desc,
                expected.iter().rev().cloned().collect(),
            ),
        ] {
            want.truncate(3);
            let found = ids(
                &db,
                Query::from("docs").order_by("body", direction).limit(3),
            );
            assert_eq!(
                found,
                want.iter().map(|(_, id)| *id).collect::<Vec<_>>(),
                "indexed: {indexed}, {direction:?}"
            );
        }
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

/// Every journal mode, including the ones browser VFSes need (no WAL):
/// the large-value reader connection sees exactly the committed chunks.
#[test]
fn a_file_store_reads_chunks_back_after_reopening() {
    for journal in [Journal::Wal, Journal::Rollback, Journal::Memory] {
        let path = scratch(&format!("reopen-{journal:?}"));
        let options = FileOptions {
            journal,
            ..FileOptions::default()
        };
        let values: Vec<String> = (0..4).map(|n| text(20 + n, LARGE)).collect();
        {
            let db = block_on(Db::open_at(config(true), &path, options)).unwrap();
            for (n, value) in values.iter().enumerate() {
                insert(&db, n as u8 + 1, value, b"");
            }
            // Read back within the same session, through the reader.
            assert_eq!(ordered(&db).len(), values.len(), "{journal:?}");
        }
        let db = block_on(Db::open_at(config(true), &path, options)).unwrap();
        let mut expected: Vec<(String, RowUuid)> = values
            .iter()
            .enumerate()
            .map(|(n, value)| (value.clone(), row(n as u8 + 1)))
            .collect();
        expected.sort();
        assert_eq!(
            ordered(&db),
            expected.iter().map(|(_, id)| *id).collect::<Vec<_>>(),
            "{journal:?}"
        );
        let found = ids(
            &db,
            Query::from("docs").filter(eq(col("body"), lit(Value::String(values[2].clone())))),
        );
        assert_eq!(found, vec![row(3)], "{journal:?}");
    }
}

/// The file needs nothing but SQLite: a plain connection, with no Jazz
/// collation or functions registered, opens the schema, checks every index
/// (expression indexes over sort keys included), prepares the statements
/// large-value operators compile to, and fires the refcount triggers.
#[test]
fn a_plain_sqlite_connection_runs_the_schema_queries_and_triggers() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("vanilla.sqlite");
    let statements = {
        let db = block_on(Db::open_at(config(true), &path, FileOptions::default())).unwrap();
        insert(&db, 1, &text(40, LARGE), &vec![7; LARGE]);
        insert(&db, 2, "short", b"tiny");
        let mut statements = Vec::new();
        for query in [
            Query::from("docs").filter(contains(col("body"), lit(Value::String("x".into())))),
            Query::from("docs").filter(contains(col("blob"), lit(Value::Bytes(vec![1])))),
            Query::from("docs").filter(eq(col("body"), lit(Value::String("x".into())))),
            Query::from("docs").filter(eq(col("body"), lit(Value::String(text(41, LARGE))))),
            Query::from("docs").order_by("body", OrderDirection::Desc),
        ] {
            let prepared = db.prepare_query(&query).unwrap();
            statements.push(db.explain_query_plan(&prepared, None).unwrap().0);
        }
        block_on(db.close()).unwrap();
        statements
    };
    let plain = rusqlite::Connection::open(&path).unwrap();
    let schema: Vec<String> = plain
        .prepare("SELECT sql FROM sqlite_schema WHERE sql IS NOT NULL")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    for sql in &schema {
        let lower = sql.to_lowercase();
        assert!(
            !lower.contains("collate") && !lower.contains("jz_"),
            "{sql}"
        );
    }
    let check: String = plain
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .unwrap();
    assert_eq!(check, "ok");
    for sql in &statements {
        plain.prepare(sql).unwrap_or_else(|e| panic!("{e}: {sql}"));
    }
    let durable = |plain: &rusqlite::Connection| -> i64 {
        plain
            .query_row(
                "SELECT COALESCE(SUM(refs), 0) FROM _jazz_lv_root",
                [],
                |r| r.get(0),
            )
            .unwrap()
    };
    let before = durable(&plain);
    assert!(before > 0);
    plain.execute("DELETE FROM docs", []).unwrap();
    assert!(
        durable(&plain) < before,
        "the delete triggers released the roots"
    );
}

fn body(db: &Db, n: u8) -> String {
    let prepared = db
        .prepare_query(&Query::from("docs").filter(eq(col("id"), lit(Value::Uuid(row(n).0)))))
        .unwrap();
    let rows = block_on(db.all(&prepared, ReadOpts::default())).unwrap();
    match cell(&rows[0], "body") {
        Value::String(text) => text,
        other => panic!("{other:?}"),
    }
}

#[test]
fn range_reads_slice_chunked_and_inline_cells() {
    let db = db();
    let body = text(30, LARGE);
    let blob: Vec<u8> = (0..LARGE).map(|n| (n * 7 % 251) as u8).collect();
    insert(&db, 1, &body, &blob);
    insert(&db, 2, "short", b"tiny");
    for range in [0..10, 70_000..70_100, (LARGE - 5) as u64..LARGE as u64] {
        let (from, to) = (range.start as usize, range.end as usize);
        let got = block_on(db.read_value_range("docs", row(1), "blob", range.clone())).unwrap();
        assert_eq!(got, blob[from..to]);
        let got = block_on(db.read_value_range("docs", row(1), "body", range.clone())).unwrap();
        assert_eq!(got, body.as_bytes()[from..to]);
    }
    assert_eq!(
        block_on(db.read_value_range("docs", row(2), "blob", 1..3)).unwrap(),
        b"in"
    );
    assert!(block_on(db.read_value_range("docs", row(1), "blob", 0..LARGE as u64 + 1)).is_err());

    // UTF-16 ranges count code units: each emoji is two.
    let emoji = format!("{}{}", "\u{1F600}".repeat(40_000), text(31, 1000));
    block_on(db.update(
        "docs",
        row(2),
        BTreeMap::from([("body".to_owned(), Value::String(emoji.clone()))]),
        Default::default(),
    ))
    .unwrap();
    let units: Vec<u16> = emoji.encode_utf16().collect();
    let got = block_on(db.read_text_utf16_range("docs", row(2), "body", 79_990..80_010)).unwrap();
    assert_eq!(got, String::from_utf16(&units[79_990..80_010]).unwrap());
    assert!(
        block_on(db.read_text_utf16_range("docs", row(2), "body", 1..4)).is_err(),
        "a boundary inside a surrogate pair fails"
    );
}

#[test]
fn appends_extend_the_edit_tail_until_consolidation() {
    let db = db();
    let mut expected = text(32, LARGE);
    insert(&db, 1, &expected, b"");
    block_on(db.tick()).unwrap();
    let (nodes, _) = db.large_value_nodes_for_test().unwrap();
    for n in 0..8 {
        let suffix = format!("<{n}>");
        block_on(db.append_value("docs", row(1), "body", suffix.clone().into_bytes())).unwrap();
        expected.push_str(&suffix);
    }
    let (after, _) = db.large_value_nodes_for_test().unwrap();
    assert_eq!(after, nodes, "a tail append writes no chunks");
    assert_eq!(body(&db, 1), expected);
    // The appended value compares and contains as its logical bytes.
    assert_eq!(
        ids(
            &db,
            Query::from("docs").filter(eq(col("body"), lit(Value::String(expected.clone()))))
        ),
        vec![row(1)]
    );
    assert_eq!(
        ids(
            &db,
            Query::from("docs").filter(contains(col("body"), lit(Value::String("<6><7>".into()))))
        ),
        vec![row(1)]
    );
    // Past an epoch's edit bytes the value is consolidated into new nodes.
    for n in 8..(8 + jazz::large_values::epoch::EPOCH_EDIT_BYTES / 4) {
        let suffix = format!("[{n}]");
        block_on(db.append_value("docs", row(1), "body", suffix.clone().into_bytes())).unwrap();
        expected.push_str(&suffix);
    }
    let (after, _) = db.large_value_nodes_for_test().unwrap();
    assert!(
        after > nodes,
        "consolidation wrote new nodes: {nodes} -> {after}"
    );
    assert_eq!(body(&db, 1), expected);
}

#[test]
fn many_versions_with_small_edits_share_one_epoch() {
    let db = db();
    let mut expected = text(33, LARGE);
    insert(&db, 1, &expected, b"");
    let (nodes, _) = db.large_value_nodes_for_test().unwrap();
    // Each append is its own commit, so its own version in history.
    for n in 0..300 {
        let suffix = if n % 2 == 0 { "a" } else { "é" };
        block_on(db.append_value("docs", row(1), "body", suffix.as_bytes().to_vec())).unwrap();
        expected.push_str(suffix);
    }
    let (after, _) = db.large_value_nodes_for_test().unwrap();
    assert_eq!(after, nodes, "no version wrote a node");
    let (epochs, edit_bytes) = db.large_value_epochs_for_test().unwrap();
    assert_eq!(epochs, 1, "every version names a prefix of one epoch");
    // A one-character append costs two or three bytes of edits.
    assert!(edit_bytes <= 300 * 3, "{edit_bytes} edit bytes");
    assert_eq!(body(&db, 1), expected);
    block_on(db.tick()).unwrap();
    assert_eq!(db.large_value_epochs_for_test().unwrap().0, 1);
}

#[test]
fn an_inline_value_grown_past_the_threshold_is_chunked() {
    let db = db();
    let start = text(33, 60 * 1024);
    insert(&db, 1, &start, b"");
    assert_eq!(db.large_value_nodes_for_test().unwrap().1, 0);
    let more = text(34, 10 * 1024);
    block_on(db.append_value("docs", row(1), "body", more.clone().into_bytes())).unwrap();
    assert_eq!(db.large_value_nodes_for_test().unwrap().1, 1);
    assert_eq!(body(&db, 1), format!("{start}{more}"));
}

#[test]
fn splices_edit_chunked_values_in_place() {
    let db = db();
    let mut expected = text(35, LARGE).into_bytes();
    insert(&db, 1, std::str::from_utf8(&expected).unwrap(), b"");
    for n in 0..100_u64 {
        let at = (n * 2_903) % (expected.len() as u64 - 10);
        let insert = format!("({n})").into_bytes();
        block_on(db.splice_value(
            "docs",
            row(1),
            "body",
            LargeValueUpdateSplice {
                at,
                delete: 3,
                insert: insert.clone(),
            },
            Default::default(),
        ))
        .unwrap();
        expected.splice(at as usize..at as usize + 3, insert);
    }
    assert_eq!(body(&db, 1).into_bytes(), expected);
    assert!(
        block_on(db.splice_value(
            "docs",
            row(1),
            "body",
            LargeValueUpdateSplice {
                at: 0,
                delete: 1,
                insert: vec![0xFF],
            },
            Default::default(),
        ))
        .is_err(),
        "a splice that breaks UTF-8 is rejected"
    );
    assert_eq!(
        body(&db, 1).into_bytes(),
        expected,
        "and leaves the row unchanged"
    );
}

#[test]
fn partial_updates_splice_utf16_pages_and_set_json_members() {
    let db = db();
    let text16 = format!("{}{}", "é\u{1F600}".repeat(30_000), "tail");
    insert(&db, 1, &text16, b"");
    let units: Vec<u16> = text16.encode_utf16().collect();
    block_on(db.update_with_large_value_mutations(
        "docs",
        row(1),
        BTreeMap::new(),
        vec![LargeValueUpdate::Splice {
            column: "body".into(),
            within: LargeValueUpdatePage::TextUtf16 { from: 30, to: 60 },
            splices: vec![
                LargeValueUpdateSplice {
                    at: 0,
                    delete: 3,
                    insert: b"AB".to_vec(),
                },
                LargeValueUpdateSplice {
                    at: 2,
                    delete: 1,
                    insert: "\u{1F680}".as_bytes().to_vec(),
                },
            ],
        }],
        Default::default(),
    ))
    .unwrap();
    let mut edited: Vec<u16> = units[..30].to_vec();
    edited.extend("AB".encode_utf16());
    edited.extend("\u{1F680}".encode_utf16());
    // "AB" replaced units 30..33; the second splice then replaced the
    // unit after "AB", which was unit 33 of the original.
    edited.extend(&units[34..]);
    assert_eq!(body(&db, 1), String::from_utf16(&edited).unwrap());

    let items: Vec<serde_json::Value> = (0..4000)
        .map(|n| serde_json::json!({ "n": n, "label": text(n, 20) }))
        .collect();
    let doc = serde_json::json!({ "items": items, "title": "draft" }).to_string();
    assert!(doc.len() > 64 * 1024);
    block_on(db.insert(
        "notes",
        BTreeMap::from([("doc".to_owned(), Value::String(doc))]),
        InsertOptions {
            row_id: Some(row(9)),
            ..Default::default()
        },
    ))
    .unwrap();
    block_on(db.update_with_large_value_mutations(
        "notes",
        row(9),
        BTreeMap::new(),
        vec![LargeValueUpdate::JsonSet {
            column: "doc".into(),
            edits: vec![
                JsonSetEdit {
                    at: "/title".into(),
                    value: serde_json::json!("final"),
                },
                JsonSetEdit {
                    at: "/items/2/n".into(),
                    value: serde_json::json!(-2),
                },
            ],
        }],
        Default::default(),
    ))
    .unwrap();
    let read =
        |pointer: &str| block_on(db.read_json_pointer("notes", row(9), "doc", pointer)).unwrap();
    assert_eq!(read("/title"), Some(serde_json::json!("final")));
    assert_eq!(read("/items/2/n"), Some(serde_json::json!(-2)));
    assert_eq!(read("/items/3999/n"), Some(serde_json::json!(3999)));
    assert_eq!(read("/missing"), None);
    assert!(
        block_on(db.update_with_large_value_mutations(
            "notes",
            row(9),
            BTreeMap::new(),
            vec![LargeValueUpdate::JsonSet {
                column: "doc".into(),
                edits: vec![JsonSetEdit {
                    at: "/nope/deeper".into(),
                    value: serde_json::json!(1),
                }],
            }],
            Default::default(),
        ))
        .is_err(),
        "missing parents are never created"
    );
}

#[test]
fn streaming_inserts_chunk_as_they_read() {
    let db = db();
    let blob: Vec<u8> = (0..LARGE * 3).map(|n| (n * 13 % 253) as u8).collect();
    block_on(db.insert_streaming_value_with_id(
        "docs",
        row(1),
        BTreeMap::from([("body".to_owned(), Value::String("streamed".into()))]),
        "blob",
        std::io::Cursor::new(blob.clone()),
    ))
    .unwrap();
    let (nodes, roots) = db.large_value_nodes_for_test().unwrap();
    assert!(nodes > 4);
    assert_eq!(roots, 1);
    assert_eq!(
        block_on(db.read_value_range("docs", row(1), "blob", 500_000..500_100)).unwrap(),
        blob[500_000..500_100]
    );

    // A failed insert spends its receipt; the streamed tree is reclaimed.
    assert!(
        block_on(db.insert_streaming_value_with_id(
            "docs",
            row(2),
            BTreeMap::from([("nonexistent".to_owned(), Value::String("x".into()))]),
            "blob",
            std::io::Cursor::new(blob.clone()),
        ))
        .is_err()
    );
    for _ in 0..8 {
        block_on(db.tick()).unwrap();
    }
    assert_eq!(db.large_value_nodes_for_test().unwrap(), (nodes, roots));
}

#[test]
fn a_file_store_streams_edits_and_reads_back_after_reopening() {
    let path = scratch("stream-edit");
    let blob: Vec<u8> = (0..LARGE * 2).map(|n| (n * 31 % 241) as u8).collect();
    let mut expected = text(40, LARGE);
    {
        let db = block_on(Db::open_at(config(false), &path, FileOptions::default())).unwrap();
        block_on(db.insert_streaming_value_with_id(
            "docs",
            row(1),
            BTreeMap::from([("body".to_owned(), Value::String(expected.clone()))]),
            "blob",
            std::io::Cursor::new(blob.clone()),
        ))
        .unwrap();
        for n in 0..70 {
            let suffix = format!("+{n}");
            block_on(db.append_value("docs", row(1), "body", suffix.clone().into_bytes())).unwrap();
            expected.push_str(&suffix);
        }
        assert_eq!(
            block_on(db.read_value_range("docs", row(1), "blob", 300_000..300_050)).unwrap(),
            blob[300_000..300_050]
        );
    }
    let db = block_on(Db::open_at(config(false), &path, FileOptions::default())).unwrap();
    assert_eq!(body(&db, 1), expected);
    assert_eq!(
        block_on(db.read_value_range("docs", row(1), "blob", 0..blob.len() as u64)).unwrap(),
        blob
    );
}

/// Partial updates staged in an open transaction read the row with the
/// transaction's earlier writes applied, are visible to its own reads, and
/// land in one commit; an abandoned transaction leaves the row unchanged.
#[test]
fn partial_updates_inside_a_transaction_build_on_its_earlier_writes() {
    let db = db();
    let original = text(7, LARGE);
    insert(&db, 1, &original, b"");
    let appended = |n: usize| LargeValueUpdate::Splice {
        column: "body".into(),
        within: LargeValueUpdatePage::TextUtf8 {
            from: (LARGE + n) as u64,
            to: (LARGE + n) as u64,
        },
        splices: vec![LargeValueUpdateSplice {
            at: 0,
            delete: 0,
            insert: vec![b'0' + (n % 10) as u8],
        }],
    };
    let expected: String = original.clone()
        + &(0..64)
            .map(|n| char::from(b'0' + (n % 10) as u8))
            .collect::<String>();

    // Abandoned: nothing lands.
    let batch = OpenTransactionId::new();
    block_on(db.begin_mergeable(batch)).unwrap();
    block_on(db.tx_ref(batch).update_with_large_value_mutations(
        "docs",
        row(1),
        BTreeMap::new(),
        vec![appended(0)],
        Default::default(),
    ))
    .unwrap();
    db.abandon_transaction_handle(batch).unwrap();
    assert_eq!(body(&db, 1), original);

    // Each edit builds on the previous one; enough of them that the edit
    // tail fills and consolidates inside the transaction.
    let batch = OpenTransactionId::new();
    block_on(db.begin_mergeable(batch)).unwrap();
    let tx = db.tx_ref(batch);
    for n in 0..64 {
        block_on(tx.update_with_large_value_mutations(
            "docs",
            row(1),
            BTreeMap::new(),
            vec![appended(n)],
            Default::default(),
        ))
        .unwrap();
    }
    let point = db
        .prepare_query(&Query::from("docs").filter(eq(col("id"), lit(Value::Uuid(row(1).0)))))
        .unwrap();
    let seen = block_on(tx.all_prepared(&point)).unwrap();
    assert_eq!(cell(&seen[0], "body"), Value::String(expected.clone()));
    assert_eq!(body(&db, 1), original, "nothing lands before the commit");
    block_on(db.commit_mergeable_handle(batch)).unwrap();
    assert_eq!(body(&db, 1), expected);

    // A JSON member set inside a transaction.
    let doc = serde_json::json!({
        "items": (0..4000).map(|n| serde_json::json!({ "n": n, "label": text(n, 20) })).collect::<Vec<_>>(),
        "title": "draft",
    })
    .to_string();
    assert!(doc.len() > 64 * 1024);
    block_on(db.insert(
        "notes",
        BTreeMap::from([("doc".to_owned(), Value::String(doc))]),
        InsertOptions {
            row_id: Some(row(9)),
            ..Default::default()
        },
    ))
    .unwrap();
    let batch = OpenTransactionId::new();
    block_on(db.begin_mergeable(batch)).unwrap();
    for title in ["second", "final"] {
        block_on(db.tx_ref(batch).update_with_large_value_mutations(
            "notes",
            row(9),
            BTreeMap::new(),
            vec![LargeValueUpdate::JsonSet {
                column: "doc".into(),
                edits: vec![JsonSetEdit {
                    at: "/title".into(),
                    value: serde_json::json!(title),
                }],
            }],
            Default::default(),
        ))
        .unwrap();
    }
    block_on(db.commit_mergeable_handle(batch)).unwrap();
    let read =
        |pointer: &str| block_on(db.read_json_pointer("notes", row(9), "doc", pointer)).unwrap();
    assert_eq!(read("/title"), Some(serde_json::json!("final")));
    assert_eq!(read("/items/3999/n"), Some(serde_json::json!(3999)));
}
