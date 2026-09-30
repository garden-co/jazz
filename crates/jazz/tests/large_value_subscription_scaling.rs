//! A retained subscription must not rebuild the large columns its projection
//! drops, neither for its first result nor for any later update (#3830).
//!
//! One-shot reads already keep such columns physical (#3471). A retained
//! subscription still rebuilt every large value of every row it published,
//! then dropped the columns its `select` excludes. On a replica that holds
//! only the row, rebuilding means fetching every chunk first. A folder listing
//! that selected only file names therefore downloaded each file completely,
//! and published none of its rows until the last chunk had arrived.
//!
//! As in `large_value_read_scaling`, these tests observe Groove's test-only
//! count of complete large-value rebuilds (`full_materializations_for_test`):
//! the public rows are the same either way, and the defect is precisely the
//! work those rows do not show. Each count is compared with the same write
//! made while no subscription is open, so a write's own work never counts
//! against the subscription. Every other assertion is on public results.

use std::collections::BTreeMap;

mod common;

use common::{allow_all_policies, compile_schema};
use jazz::block_on;
use jazz::db::{
    Db, DbConfig, DbIdentity, DeleteOptions, InsertOptions, LocalUpdates, Propagation, ReadOpts,
    SubscriptionEvent, SubscriptionStream, UpdateOptions,
};
use jazz::groove::large_values::{LEAF_MAX_BYTES, full_materializations_for_test};
use jazz::groove::records::Value;
use jazz::groove::storage::TestStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::node::CurrentRow;
use jazz::query::Query;
use jazz::schema::{JazzSchema, TableSchema};
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

const FILES: &str = "files";

fn row(seed: u8) -> RowUuid {
    RowUuid::from_bytes([seed; 16])
}

fn files_schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new(FILES)
                    .column("name", ColumnType::Text)
                    .column("notes", ColumnType::Text)
                    .column("contents", ColumnType::Bytea)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn open_db(schema: JazzSchema, seed: u8) -> Db {
    let families = schema.column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    block_on(Db::open(DbConfig::new(
        schema,
        TestStorage::new(&refs),
        DbIdentity {
            node: NodeUuid::from_bytes([seed; 16]),
            author: AuthorSubject::for_test_bytes([0xa1; 16]),
        },
    )))
    .expect("open db")
}

fn table(schema: &JazzSchema) -> TableSchema {
    schema
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

fn insert_file(db: &Db, id: RowUuid, name: &str, bytes: &[u8]) {
    let write = block_on(db.insert(
        FILES,
        BTreeMap::from([
            ("name".to_owned(), Value::String(name.to_owned())),
            ("notes".to_owned(), Value::String(large_notes(name))),
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

fn rename_file(db: &Db, id: RowUuid, name: &str) {
    let write = block_on(db.update(
        FILES,
        id,
        BTreeMap::from([("name".to_owned(), Value::String(name.to_owned()))]),
        UpdateOptions::default(),
    ))
    .expect("rename file");
    block_on(write.wait(DurabilityTier::Local)).expect("local durability");
}

fn replace_contents(db: &Db, id: RowUuid, bytes: &[u8]) {
    let write = block_on(db.update(
        FILES,
        id,
        BTreeMap::from([("contents".to_owned(), Value::Bytes(bytes.to_vec()))]),
        UpdateOptions::default(),
    ))
    .expect("replace contents");
    block_on(write.wait(DurabilityTier::Local)).expect("local durability");
}

fn delete_file(db: &Db, id: RowUuid) {
    let write = block_on(db.delete(FILES, id, DeleteOptions::default())).expect("delete file");
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

/// Runs `work` and returns its result with the number of complete
/// large-value rebuilds it caused on this thread.
fn counting<T>(work: impl FnOnce() -> T) -> (T, u64) {
    let before = full_materializations_for_test();
    let result = work();
    (result, full_materializations_for_test() - before)
}

fn subscribe(db: &Db, query: Query) -> SubscriptionStream {
    let prepared = db.prepare_query(&query).expect("prepare subscription");
    block_on(db.subscribe(&prepared, opts())).expect("subscribe")
}

struct Delta {
    reset: bool,
    added: Vec<CurrentRow>,
    updated: Vec<CurrentRow>,
    removed: Vec<RowUuid>,
}

fn next_delta(subscription: &mut SubscriptionStream) -> Delta {
    match block_on(subscription.next_event()) {
        Some(SubscriptionEvent::Delta {
            reset,
            added,
            updated,
            removed,
            ..
        }) => Delta {
            reset,
            added: added.into_iter().map(|output| output.row).collect(),
            updated: updated.into_iter().map(|output| output.row).collect(),
            removed: removed
                .into_iter()
                .map(|removed| removed.row_uuid)
                .collect(),
        },
        other => panic!("expected a subscription delta, got {other:?}"),
    }
}

fn name(table: &TableSchema, row: &CurrentRow) -> Option<Value> {
    row.cell(table, "name")
}

/// A listing subscription that projects the large columns away never
/// rebuilds them: not for its first result, not when a file is added,
/// renamed or deleted. Its rows carry the selected column only.
///
/// alice ──insert a.bin, b.bin (large notes and contents)──► db
/// alice ──subscribe select(name)──► {a.bin, b.bin}, 0 rebuilds
/// alice ──insert c.bin───────────► +c.bin, as many rebuilds as with no subscription
/// alice ──rename b.bin → b2.bin──► ~b2.bin, as many rebuilds as with no subscription
/// alice ──delete a.bin───────────► −a.bin
#[test]
fn listing_subscription_never_rebuilds_excluded_large_columns() {
    let schema = files_schema();
    let table = table(&schema);
    let db = open_db(schema, 0x81);

    // Baselines: the same writes while no subscription is open.
    let ((), insert_baseline) =
        counting(|| insert_file(&db, row(0x82), "baseline.bin", &contents(0x82, 3)));
    let ((), rename_baseline) = counting(|| rename_file(&db, row(0x82), "baseline-2.bin"));
    delete_file(&db, row(0x82));

    insert_file(&db, row(0x83), "a.bin", &contents(0x83, 3));
    insert_file(&db, row(0x84), "b.bin", &contents(0x84, 3));

    let ((mut listing, opening), rebuilds) = counting(|| {
        let mut listing = subscribe(&db, Query::from(FILES).select(["name"]));
        let opening = next_delta(&mut listing);
        (listing, opening)
    });
    assert!(opening.reset);
    assert_eq!(
        rebuilds, 0,
        "a listing's first result must not rebuild the large columns it projects away"
    );
    let mut names = opening
        .added
        .iter()
        .map(|published| match name(&table, published) {
            Some(Value::String(listed)) => listed,
            other => panic!("expected a listed name, got {other:?}"),
        })
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, ["a.bin", "b.bin"]);
    for published in &opening.added {
        assert_eq!(published.cell(&table, "contents"), None);
        assert_eq!(published.cell(&table, "notes"), None);
    }

    let (added, rebuilds) = counting(|| {
        insert_file(&db, row(0x85), "c.bin", &contents(0x85, 3));
        next_delta(&mut listing)
    });
    assert_eq!(
        rebuilds, insert_baseline,
        "adding a file must not rebuild its contents for a listing"
    );
    assert_eq!(
        added
            .added
            .iter()
            .map(CurrentRow::row_uuid)
            .collect::<Vec<_>>(),
        vec![row(0x85)]
    );
    assert_eq!(
        name(&table, &added.added[0]),
        Some(Value::String("c.bin".to_owned()))
    );
    assert_eq!(added.added[0].cell(&table, "contents"), None);

    let (renamed, rebuilds) = counting(|| {
        rename_file(&db, row(0x84), "b2.bin");
        next_delta(&mut listing)
    });
    assert_eq!(
        rebuilds, rename_baseline,
        "renaming a file must not rebuild its contents for a listing"
    );
    let renamed_rows = renamed
        .updated
        .iter()
        .chain(&renamed.added)
        .filter(|published| published.row_uuid() == row(0x84))
        .collect::<Vec<_>>();
    assert_eq!(renamed_rows.len(), 1, "the renamed row is republished once");
    assert_eq!(
        name(&table, renamed_rows[0]),
        Some(Value::String("b2.bin".to_owned()))
    );

    // The retraction names the physical record the listing published.
    delete_file(&db, row(0x83));
    let deleted = next_delta(&mut listing);
    assert_eq!(deleted.removed, vec![row(0x83)]);
    assert!(deleted.added.is_empty());
}

/// A listing and a subscription that selects the contents may share graph
/// nodes. Each keeps its own representation: the listing never sees the
/// contents, while the other subscription still receives them in full, for
/// its first result and after the contents change.
///
/// alice ──subscribe select(name)──────────► listing
/// alice ──subscribe select(name, contents)─► viewer
/// alice ──insert d.bin────────────────────► listing +d.bin, viewer +d.bin with contents
/// alice ──replace d.bin's contents────────► viewer ~d.bin with the new contents
/// alice ──read *──────────────────────────► complete notes and contents
#[test]
fn listing_and_content_subscriptions_keep_their_own_representation() {
    let schema = files_schema();
    let table = table(&schema);
    let db = open_db(schema, 0x91);
    let first = contents(0x92, 3);
    insert_file(&db, row(0x92), "first.bin", &first);

    let mut listing = subscribe(&db, Query::from(FILES).select(["name"]));
    let mut viewer = subscribe(&db, Query::from(FILES).select(["name", "contents"]));
    let listed = next_delta(&mut listing);
    let viewed = next_delta(&mut viewer);
    assert!(listed.reset && viewed.reset);
    assert_eq!(listed.added.len(), 1);
    assert_eq!(listed.added[0].cell(&table, "contents"), None);
    assert_eq!(viewed.added.len(), 1);
    assert_eq!(
        viewed.added[0].cell(&table, "contents"),
        Some(Value::Bytes(first))
    );

    let added = contents(0x93, 4);
    insert_file(&db, row(0x93), "d.bin", &added);
    let listed = next_delta(&mut listing);
    let viewed = next_delta(&mut viewer);
    assert_eq!(
        listed
            .added
            .iter()
            .map(CurrentRow::row_uuid)
            .collect::<Vec<_>>(),
        vec![row(0x93)]
    );
    assert_eq!(listed.added[0].cell(&table, "contents"), None);
    assert_eq!(
        viewed
            .added
            .iter()
            .map(CurrentRow::row_uuid)
            .collect::<Vec<_>>(),
        vec![row(0x93)]
    );
    assert_eq!(
        viewed.added[0].cell(&table, "contents"),
        Some(Value::Bytes(added))
    );

    let replaced = contents(0x94, 5);
    replace_contents(&db, row(0x93), &replaced);
    let viewed = next_delta(&mut viewer);
    let changed = viewed
        .updated
        .iter()
        .chain(&viewed.added)
        .find(|published| published.row_uuid() == row(0x93))
        .expect("the viewer republishes the changed file");
    assert_eq!(
        changed.cell(&table, "contents"),
        Some(Value::Bytes(replaced.clone()))
    );

    let whole = db
        .prepare_query(&Query::from(FILES))
        .expect("prepare whole rows");
    let rows = db.read(&whole).expect("whole rows");
    let changed = rows
        .iter()
        .find(|published| published.row_uuid() == row(0x93))
        .expect("listed");
    assert_eq!(
        changed.cell(&table, "notes"),
        Some(Value::String(large_notes("d.bin")))
    );
    assert_eq!(
        changed.cell(&table, "contents"),
        Some(Value::Bytes(replaced))
    );
}
