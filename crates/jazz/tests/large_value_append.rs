//! Local appends to large text and bytes cells (#3471, case 2).
//!
//! An append derived from an already published descriptor must produce the
//! exact logical value, keep rejecting invalid input, and validate only what
//! it added before its staging receipt is issued. The scale check uses
//! Groove's test-only `finalize_validation_bytes_for_test` counter: it is the
//! one internal hook here, because "how much of the value was re-read" has no
//! public observable other than elapsed time, which cannot prove scaling.

use std::collections::BTreeMap;

mod common;

use common::{allow_all_policies, compile_schema};
use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions};
use jazz::groove::large_values::{
    INLINE_VALUE_MAX_BYTES, LEAF_MAX_BYTES, MAX_EDIT_COUNT, finalize_validation_bytes_for_test,
};
use jazz::groove::records::Value;
use jazz::groove::storage::TestStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

fn open_db(node: u8) -> Db<TestStorage> {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("docs")
                    .column("body", ColumnType::Text)
                    .column("blob", ColumnType::Bytea)
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    jazz::block_on(Db::open(DbConfig {
        schema,
        storage: TestStorage::new(&refs),
        identity: DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::for_test_bytes([0xa1; 16]),
        },
        id_source: None,
    }))
    .expect("open db")
}

/// Deterministic, non-repeating printable text so content-defined chunking
/// produces an ordinary multi-leaf tree rather than one deduplicated leaf.
fn text(len: usize, seed: u64) -> String {
    let mut state = seed | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            char::from(b'a' + (state % 26) as u8)
        })
        .collect()
}

fn bytes(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

fn insert(db: &Db<TestStorage>, row: RowUuid, body: String, blob: Vec<u8>) {
    jazz::block_on(db.insert(
        "docs",
        BTreeMap::from([
            ("body".to_owned(), Value::String(body)),
            ("blob".to_owned(), Value::Bytes(blob)),
        ]),
        InsertOptions {
            row_id: Some(row),
            ..Default::default()
        },
    ))
    .expect("insert large row");
}

fn read_all(db: &Db<TestStorage>, row: RowUuid, column: &str, len: usize) -> Vec<u8> {
    jazz::block_on(db.read_value_range("docs", row, column, 0..len as u64)).expect("read value")
}

fn append(db: &Db<TestStorage>, row: RowUuid, column: &str, suffix: &[u8]) {
    jazz::block_on(db.append_value("docs", row, column, suffix.to_vec())).expect("append");
}

#[test]
fn string_appends_across_consolidation_keep_exact_multibyte_text() {
    let db = open_db(0x31);
    let row = RowUuid::from_bytes([0x31; 16]);
    let mut expected = format!("{}🙂", text(INLINE_VALUE_MAX_BYTES * 8, 7));
    insert(&db, row, expected.clone(), bytes(16, 1));

    // Well past one consolidation (every MAX_EDIT_COUNT appends), mixing
    // one-, two-, three- and four-byte code points at append boundaries.
    for step in 0..(MAX_EDIT_COUNT * 2 + 7) {
        let suffix = match step % 4 {
            0 => format!("<{step}>"),
            1 => "é".to_owned(),
            2 => "€漢".to_owned(),
            _ => "🙂x".to_owned(),
        };
        append(&db, row, "body", suffix.as_bytes());
        expected.push_str(&suffix);
    }

    let actual = read_all(&db, row, "body", expected.len());
    assert_eq!(actual.len(), expected.len());
    assert!(
        actual == expected.as_bytes(),
        "appended text diverged from the expected value"
    );
    // A range spanning the last consolidation and the current tail.
    let end = expected.len() as u64;
    assert_eq!(
        jazz::block_on(db.read_value_range("docs", row, "body", end - 200..end)).unwrap(),
        expected.as_bytes()[expected.len() - 200..]
    );
}

#[test]
fn bytes_appends_across_consolidation_keep_exact_bytes() {
    let db = open_db(0x32);
    let row = RowUuid::from_bytes([0x32; 16]);
    let mut expected = bytes(INLINE_VALUE_MAX_BYTES * 8, 3);
    insert(&db, row, "short".to_owned(), expected.clone());

    for step in 0..(MAX_EDIT_COUNT * 2 + 3) {
        // Arbitrary bytes, including ones that are never valid UTF-8.
        let suffix = [0xff, step as u8, 0x00, 0xc3];
        append(&db, row, "blob", &suffix);
        expected.extend_from_slice(&suffix);
    }

    let actual = read_all(&db, row, "blob", expected.len());
    assert!(actual == expected, "appended bytes diverged");
}

#[test]
fn splices_across_consolidation_keep_exact_text() {
    // Splices share the derived-validation path with appends; unlike them
    // they can drop or reshape whole base subtrees before consolidation.
    let db = open_db(0x36);
    let row = RowUuid::from_bytes([0x36; 16]);
    let mut expected = text(INLINE_VALUE_MAX_BYTES * 4, 5).into_bytes();
    insert(
        &db,
        row,
        String::from_utf8(expected.clone()).unwrap(),
        bytes(16, 1),
    );

    for step in 0..(MAX_EDIT_COUNT + 6) {
        let len = expected.len();
        let (offset, delete) = match step % 3 {
            // A large deletion spanning several leaves.
            0 => (len / 5, (len / 7).min(100_000)),
            1 => (len / 2 + step, 3),
            _ => (len - 10, 10),
        };
        let insert = format!("[{step}é]");
        jazz::block_on(db.splice_value(
            "docs",
            row,
            "body",
            offset as u64,
            delete as u64,
            insert.clone().into_bytes(),
        ))
        .expect("splice");
        expected.splice(offset..offset + delete, insert.into_bytes());
    }

    let actual = read_all(&db, row, "body", expected.len());
    assert!(actual == expected, "spliced text diverged");
}

#[test]
fn string_append_still_rejects_invalid_utf8_and_split_code_points() {
    let db = open_db(0x33);
    let row = RowUuid::from_bytes([0x33; 16]);
    let base = text(INLINE_VALUE_MAX_BYTES * 4, 11);
    insert(&db, row, base.clone(), bytes(16, 1));
    append(&db, row, "body", "ok".as_bytes());
    let before = format!("{base}ok");

    // An append is one self-contained UTF-8 string: a code point cannot be
    // split across appends, and arbitrary non-UTF-8 bytes are rejected.
    let smile = "🙂".as_bytes();
    for invalid in [&smile[..2], &smile[2..], &[0xff][..], &[b'a', 0xc3][..]] {
        let Err(error) = jazz::block_on(db.append_value("docs", row, "body", invalid.to_vec()))
        else {
            panic!("invalid UTF-8 append {invalid:?} was accepted");
        };
        assert!(
            error.to_string().to_lowercase().contains("utf"),
            "unexpected rejection: {error}"
        );
        assert_eq!(read_all(&db, row, "body", before.len()), before.as_bytes());
    }

    append(&db, row, "body", smile);
    assert_eq!(
        read_all(&db, row, "body", before.len() + smile.len()),
        format!("{before}🙂").as_bytes()
    );
}

/// Returns (validation bytes for the fresh insert, max validation bytes of
/// any single append over `appends` appends).
fn validation_bytes(node: u8, size: usize, appends: usize) -> (u64, u64) {
    let db = open_db(node);
    let row = RowUuid::from_bytes([node; 16]);
    let before = finalize_validation_bytes_for_test();
    insert(&db, row, text(size, u64::from(node)), bytes(16, 1));
    let insert_bytes = finalize_validation_bytes_for_test() - before;
    let mut max_append = 0;
    for step in 0..appends {
        let before = finalize_validation_bytes_for_test();
        append(&db, row, "body", format!("{step:0>24}").as_bytes());
        max_append = max_append.max(finalize_validation_bytes_for_test() - before);
    }
    (insert_bytes, max_append)
}

#[test]
fn local_append_validation_does_not_scale_with_value_size() {
    // Cover the first consolidation, which stages new nodes and retains the
    // untouched remainder of the base tree.
    let appends = MAX_EDIT_COUNT + 2;
    let (small_insert, small_append) = validation_bytes(0x34, 128 * 1024, appends);
    let (large_insert, large_append) = validation_bytes(0x35, 1024 * 1024, appends);

    // Control: a fresh value is still fully re-read before its receipt, so
    // the counter observes this thread's finalization work.
    assert!(small_insert >= 128 * 1024, "{small_insert}");
    assert!(large_insert >= 1024 * 1024, "{large_insert}");

    // A derived append re-reads only its newly staged nodes and the base
    // branches/ranges needed to prove reuse and replay its tail. That is
    // bounded by a few leaves whatever the value's size; leaf boundaries are
    // content-defined, so the two sizes may differ by about one leaf.
    assert!(
        large_append <= small_append + LEAF_MAX_BYTES as u64
            && large_append <= 2 * LEAF_MAX_BYTES as u64,
        "append validation bytes: 128 KiB value {small_append}, 1 MiB value {large_append}"
    );
}
