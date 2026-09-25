//! Large-value reads must cost what they return, not the size of the value
//! (#3471).
//!
//! Two public paths used to rebuild every large value of every row they
//! touched, then discard it:
//!
//! - `read_value_range` authorized the cell with a one-shot query of the
//!   whole row, although only the row's identity was inspected;
//! - a projected listing rebuilt the large columns it projected away.
//!
//! Elapsed time cannot establish scale independence robustly, so these tests
//! observe Groove's test-only count of complete large-value rebuilds
//! (`full_materializations_for_test`). That counter is the one internal hook
//! here: the public API returns the same rows either way, and the defect is
//! precisely the work those rows do not show. Every other assertion is on
//! public results.

use std::collections::BTreeMap;

mod common;

use common::{allow_all_policies, compile_schema, read_and_allow_all_writes};
use jazz::block_on;
use jazz::db::{
    Db, DbConfig, DbIdentity, ErrorCode, InsertOptions, LocalUpdates, Propagation, ReadOpts,
};
use jazz::groove::large_values::{LEAF_MAX_BYTES, full_materializations_for_test};
use jazz::groove::records::Value;
use jazz::groove::storage::TestStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query};
use jazz::schema::JazzSchema;
use jazz::tools::{
    ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder, Value as PublicValue,
};
use jazz::tx::DurabilityTier;

const FILES: &str = "files";

fn row(seed: u8) -> RowUuid {
    RowUuid::from_bytes([seed; 16])
}

fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([0xa1; 16])
}

fn files_schema(policies: TablePolicies) -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new(FILES)
                    .column("name", ColumnType::Text)
                    .column("notes", ColumnType::Text)
                    .column("contents", ColumnType::Bytea)
                    .policies(policies),
            )
            .build(),
    )
}

fn open_db(schema: JazzSchema, seed: u8, author: AuthorSubject) -> Db<TestStorage> {
    let families = schema.column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    block_on(Db::open(DbConfig::new(
        schema,
        TestStorage::new(&refs),
        DbIdentity {
            node: NodeUuid::from_bytes([seed; 16]),
            author,
        },
    )))
    .expect("open db")
}

fn table(db_schema: &JazzSchema) -> jazz::schema::TableSchema {
    db_schema
        .tables()
        .iter()
        .find(|table| table.name == FILES)
        .expect("files table")
        .clone()
}

/// Deterministic bytes spanning several chunk leaves.
fn contents(seed: u8, leaves: usize) -> Vec<u8> {
    (0..LEAF_MAX_BYTES * leaves + 17)
        .map(|index| (index % 251) as u8 ^ seed)
        .collect()
}

fn large_notes(prefix: &str) -> String {
    format!("{prefix}{}", "n".repeat(LEAF_MAX_BYTES * 2))
}

fn insert_file(db: &Db<TestStorage>, id: RowUuid, name: &str, notes: &str, bytes: &[u8]) {
    let write = block_on(db.insert(
        FILES,
        BTreeMap::from([
            ("name".to_owned(), Value::String(name.to_owned())),
            ("notes".to_owned(), Value::String(notes.to_owned())),
            ("contents".to_owned(), Value::Bytes(bytes.to_vec())),
        ]),
        InsertOptions {
            row_id: Some(id),
            ..Default::default()
        },
    ))
    .expect("insert file");
    block_on(write.wait(DurabilityTier::Local)).expect("local durability");
}

fn opts() -> ReadOpts {
    ReadOpts {
        tier: DurabilityTier::Local,
        local_updates: LocalUpdates::Immediate,
        propagation: Propagation::LocalOnly,
        include_deleted: false,
        ..ReadOpts::default()
    }
}

/// Runs `read` and returns its result with the number of complete
/// large-value rebuilds it caused on this thread.
fn counting<T>(read: impl FnOnce() -> T) -> (T, u64) {
    let before = full_materializations_for_test();
    let result = read();
    (result, full_materializations_for_test() - before)
}

/// `read_value_range` returns the requested window and never rebuilds the
/// whole value, at any value size: authorization inspects only row identity.
///
/// alice ──insert 2-leaf and 12-leaf files──► db
/// alice ──read_value_range(middle 64 bytes)──► db ──0 full rebuilds──► bytes
#[test]
fn range_reads_do_not_rebuild_the_whole_value_at_any_size() {
    let schema = files_schema(allow_all_policies());
    let db = open_db(schema, 0x31, alice());
    for (seed, leaves) in [(0x32, 2), (0x33, 12)] {
        let bytes = contents(seed, leaves);
        insert_file(&db, row(seed), "clip.bin", "", &bytes);
        let middle = (bytes.len() / 2) as u64;

        let (window, rebuilds) = counting(|| {
            block_on(db.read_value_range(FILES, row(seed), "contents", middle..middle + 64))
                .expect("range read")
        });

        assert_eq!(window, bytes[middle as usize..middle as usize + 64]);
        assert_eq!(
            rebuilds, 0,
            "a {leaves}-leaf range read must not rebuild the whole value"
        );
    }
}

/// A listing that projects a large column away never rebuilds it; selecting
/// the column, or reading the whole row, still returns the complete value.
///
/// alice ──insert 3 files with large notes and contents──► db
/// alice ──read select(name)──────────────► 3 names, 0 full rebuilds
/// alice ──read select(name, contents)────► 3 complete contents
/// alice ──read *─────────────────────────► complete notes and contents
#[test]
fn projected_listing_leaves_excluded_large_columns_unread() {
    let schema = files_schema(allow_all_policies());
    let table = table(&schema);
    let db = open_db(schema, 0x41, alice());
    let files = [
        (row(0x42), "a.bin", large_notes("a"), contents(0x42, 3)),
        (row(0x43), "b.bin", large_notes("b"), contents(0x43, 3)),
        (row(0x44), "c.bin", large_notes("c"), contents(0x44, 3)),
    ];
    for (id, name, notes, bytes) in &files {
        insert_file(&db, *id, name, notes, bytes);
    }

    let listing = db
        .prepare_query(&Query::from(FILES).select(["name"]))
        .expect("prepare listing");
    let (rows, rebuilds) = counting(|| db.read(&listing).expect("listing"));
    assert_eq!(rows.len(), files.len());
    assert_eq!(
        rebuilds, 0,
        "a listing must not rebuild the large columns it projects away"
    );
    for row in &rows {
        assert_eq!(row.cell(&table, "contents"), None);
        assert_eq!(row.cell(&table, "notes"), None);
    }

    let with_contents = db
        .prepare_query(&Query::from(FILES).select(["name", "contents"]))
        .expect("prepare contents listing");
    let rows = db.read(&with_contents).expect("contents listing");
    for (id, name, _, bytes) in &files {
        let row = rows
            .iter()
            .find(|row| row.row_uuid() == *id)
            .expect("listed");
        assert_eq!(
            row.cell(&table, "name"),
            Some(Value::String((*name).to_owned()))
        );
        assert_eq!(
            row.cell(&table, "contents"),
            Some(Value::Bytes(bytes.clone()))
        );
        assert_eq!(row.cell(&table, "notes"), None);
    }

    let whole = db
        .prepare_query(&Query::from(FILES))
        .expect("prepare whole rows");
    let rows = db.read(&whole).expect("whole rows");
    for (id, _, notes, bytes) in &files {
        let row = rows
            .iter()
            .find(|row| row.row_uuid() == *id)
            .expect("listed");
        assert_eq!(
            row.cell(&table, "notes"),
            Some(Value::String(notes.clone()))
        );
        assert_eq!(
            row.cell(&table, "contents"),
            Some(Value::Bytes(bytes.clone()))
        );
    }
}

/// Ordering by a selected large column still orders by its logical value
/// while another large column is projected away.
///
/// alice ──insert notes c…, a…, b… (each larger than one leaf)──► db
/// alice ──read select(name, notes) order_by(notes)──► a, b, c
#[test]
fn projected_listing_orders_by_a_large_column() {
    let schema = files_schema(allow_all_policies());
    let table = table(&schema);
    let db = open_db(schema, 0x51, alice());
    insert_file(
        &db,
        row(0x52),
        "third",
        &large_notes("c"),
        &contents(0x52, 2),
    );
    insert_file(
        &db,
        row(0x53),
        "first",
        &large_notes("a"),
        &contents(0x53, 2),
    );
    insert_file(
        &db,
        row(0x54),
        "second",
        &large_notes("b"),
        &contents(0x54, 2),
    );

    let listing = db
        .prepare_query(
            &Query::from(FILES)
                .select(["name", "notes"])
                .order_by("notes", OrderDirection::Asc),
        )
        .expect("prepare ordered listing");
    let rows = db.read(&listing).expect("ordered listing");
    let names = rows
        .iter()
        .map(|row| row.cell(&table, "name"))
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        ["first", "second", "third"].map(|name| Some(Value::String(name.to_owned())))
    );
    assert_eq!(
        rows[0].cell(&table, "notes"),
        Some(Value::String(large_notes("a")))
    );
    assert_eq!(rows[0].cell(&table, "contents"), None);
}

/// A read policy whose predicate reads a large column still decides
/// visibility for range reads and projected reads, although neither
/// rebuilds the value for its caller.
///
/// bob (reader) ──read_value_range(visible)──► bytes
/// bob ──read_value_range(hidden)────────────✗ NotObserved
/// server-side read as bob, select(name)─────► visible row only, no notes
/// server-side read as bob, whole row────────► visible row with full notes
#[test]
fn large_column_read_policy_still_gates_range_and_projected_reads() {
    let allowed = large_notes("shared/");
    let policy = read_and_allow_all_writes(PolicyExpr::eq_literal(
        "notes",
        PublicValue::Text(allowed.clone()),
    ));
    let schema = files_schema(policy);
    let table = table(&schema);
    let bob = AuthorSubject::for_test_bytes([0xb0; 16]);
    let db = open_db(schema, 0x61, bob);
    let visible = row(0x62);
    let hidden = row(0x63);
    let visible_bytes = contents(0x62, 2);
    insert_file(&db, visible, "visible", &allowed, &visible_bytes);
    // Differs from the allowed value only in its final byte.
    let near_miss = format!("{}x", &allowed[..allowed.len() - 1]);
    insert_file(&db, hidden, "hidden", &near_miss, &contents(0x63, 2));

    assert_eq!(
        block_on(db.read_value_range(FILES, visible, "contents", 5..13)).expect("visible range"),
        visible_bytes[5..13]
    );
    let denied = block_on(db.read_value_range(FILES, hidden, "contents", 5..13))
        .expect_err("hidden range read is denied");
    assert_eq!(denied.code, ErrorCode::NotObserved);

    let listing = db
        .prepare_query(&Query::from(FILES).select(["name"]))
        .expect("prepare listing");
    let rows = block_on(db.all_for_identity(&listing, opts(), bob)).expect("listing as bob");
    assert_eq!(
        rows.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
        vec![visible]
    );
    assert_eq!(rows[0].cell(&table, "notes"), None);

    let whole = db
        .prepare_query(&Query::from(FILES))
        .expect("prepare whole rows");
    let rows = block_on(db.all_for_identity(&whole, opts(), bob)).expect("rows as bob");
    assert_eq!(
        rows.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
        vec![visible]
    );
    assert_eq!(rows[0].cell(&table, "notes"), Some(Value::String(allowed)));
    assert_eq!(
        rows[0].cell(&table, "contents"),
        Some(Value::Bytes(visible_bytes))
    );
}
