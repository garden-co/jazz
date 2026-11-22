//! Building a specialised index over rows already stored (SPEC 20.3): a
//! build ends, resumes after a restart, never serves a search half done,
//! and neither another index's state nor its failure holds it up.

use std::collections::BTreeMap;
use std::path::Path;

use jazz::db::{Db, DbConfig, DbIdentity, FileOptions, InsertOptions, ReadOpts, Reason, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{IndexOp, MatchMode, Query};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::storage::Storage;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use crate::common::{allow_all_policies, compile_schema};

/// What one tick builds (`BUILD_BATCH`).
const BATCH: u32 = 512;

/// `notes(title)` with a text index per name in `indexes`, over the
/// title.
fn schema(indexes: &[&str]) -> JazzSchema {
    let mut table = TableSchemaBuilder::new("notes")
        .column("title", ColumnType::Text)
        .policies(allow_all_policies());
    for index in indexes {
        table = table.text_index(*index, [("title", 1)], "jazz-text/1:en", false);
    }
    compile_schema(&SchemaBuilder::new().table(table).build())
}

fn config(schema: JazzSchema) -> DbConfig {
    jazz::use_test_kinds(false);
    DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([1; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
}

fn open(path: &Path, schema: JazzSchema) -> Db {
    block_on(Db::open_at(config(schema), path, FileOptions::default())).unwrap()
}

/// A store at `path` with `rows` notes titled "garden", reopened under
/// `indexes`: none of them is built yet.
fn deployed(path: &Path, rows: u32, indexes: &[&str], authority: bool) -> Db {
    let db = open(path, schema(&[]));
    for n in 0..rows {
        insert(&db, n);
    }
    drop(db);
    let db = open(path, schema(indexes));
    if authority {
        db.set_authority();
    }
    db
}

fn insert(db: &Db, n: u32) {
    let mut id = [0u8; 16];
    id[..4].copy_from_slice(&n.to_be_bytes());
    block_on(db.insert(
        "notes",
        BTreeMap::from([("title".to_owned(), Value::String("garden".into()))]),
        InsertOptions {
            row_id: Some(RowUuid::from_bytes(id)),
            ..Default::default()
        },
    ))
    .unwrap();
}

fn found(db: &Db, index: &str) -> Result<usize, jazz::db::Error> {
    let prepared = db.prepare_query(&Query::from("notes").search(
        index,
        IndexOp::Match {
            text: "garden".into(),
            mode: MatchMode::Any,
        },
    ))?;
    Ok(block_on(db.all(&prepared, ReadOpts::default()))?.len())
}

fn tick(db: &Db, times: u32) {
    for _ in 0..times {
        block_on(db.tick()).unwrap();
    }
}

/// Run `sql` on the store file beside the engine's connection.
fn tamper(path: &Path, sql: &str) {
    Storage::open(path).unwrap().execute_batch(sql).unwrap();
}

/// A build covers the rows stored when it started, and only those: rows
/// written while it runs reach the index through its queue, so writes as
/// fast as it builds do not keep it from ending.
#[test]
fn a_build_ends_while_rows_keep_arriving_as_fast_as_it_builds() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("jazz.sqlite");
    let mut rows = BATCH * 2 + 10;
    let db = deployed(&path, rows, &["search"], true);
    // Three batches cover what was stored at the start.
    for _ in 0..3 {
        for _ in 0..BATCH {
            insert(&db, rows);
            rows += 1;
        }
        tick(&db, 1);
    }
    assert_eq!(found(&db, "search").expect("built"), rows as usize);
}

/// The same bound holds across a restart in mid-build, which resumes where
/// the build was.
#[test]
fn a_build_resumed_after_a_restart_ends_where_it_started_out_to() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("jazz.sqlite");
    let mut rows = BATCH * 2 + 10;
    let db = deployed(&path, rows, &["search"], true);
    tick(&db, 1);
    drop(db);
    let db = open(&path, schema(&["search"]));
    db.set_authority();
    for _ in 0..BATCH * 2 {
        insert(&db, rows);
        rows += 1;
    }
    // One batch was done: two are left, whatever arrived since.
    tick(&db, 2);
    assert_eq!(found(&db, "search").expect("built"), rows as usize);
}

/// A row that takes a lower `rid` than the build has reached, or reuses
/// the one the build stops at, while it runs, is indexed all the same.
#[test]
fn a_reused_or_lower_rid_during_a_build_is_indexed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("jazz.sqlite");
    let rows = BATCH * 2 + 10;
    let db = deployed(&path, rows, &["search"], true);
    tick(&db, 1);
    // Two rows are replaced by copies of themselves: the last under its
    // own rid again, the one before it under a rid below every other.
    tamper(
        &path,
        "CREATE TEMP TABLE moved AS SELECT * FROM notes ORDER BY rid DESC LIMIT 2; \
         DELETE FROM notes WHERE rid IN (SELECT rid FROM moved); \
         UPDATE moved SET rid = -7 WHERE rid = (SELECT min(rid) FROM moved); \
         INSERT INTO notes SELECT * FROM moved;",
    );
    tick(&db, 4);
    assert_eq!(found(&db, "search").expect("built"), rows as usize);
}

/// A search never reads an index that is not built: a store that cannot
/// build it says so (`index_building`), and never answers from part of it.
#[test]
fn a_search_never_reads_an_index_that_is_not_built() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("jazz.sqlite");
    let db = deployed(&path, 20, &["search"], false);
    // Its state is gone, so no batch can be taken for it.
    tamper(&path, "DELETE FROM _jazz_ix WHERE name = 'search';");
    let error = found(&db, "search").expect_err("not built");
    assert_eq!(error.reason, Some(Reason::IndexBuilding));
}

/// A recorded index this store does not maintain is passed over: the
/// indexes after it still build, by ticks on an authority and by the first
/// search on a client.
#[test]
fn an_unmaintained_index_does_not_hold_up_the_others_builds() {
    for authority in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("jazz.sqlite");
        let db = deployed(&path, 20, &["search"], authority);
        // A row sorting before every maintained index's.
        tamper(
            &path,
            "CREATE TEMP TABLE stray AS SELECT * FROM _jazz_ix; \
             UPDATE stray SET tbl = '!gone', name = 'gone'; \
             INSERT INTO _jazz_ix SELECT * FROM stray;",
        );
        if authority {
            tick(&db, 2);
        }
        assert_eq!(found(&db, "search").expect("built"), 20, "{authority}");
    }
}

/// A batch that fails is reported and retried, and stops neither the rest
/// of the tick nor another index's build.
#[test]
fn a_failed_batch_stops_neither_the_tick_nor_the_other_builds() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("jazz.sqlite");
    let db = deployed(&path, 20, &["broken", "search"], true);
    // `broken` builds first and cannot write.
    tamper(&path, "DROP TABLE _jazz_ix_notes__broken_post;");
    for _ in 0..3 {
        block_on(db.tick()).expect("the tick goes on past a failed batch");
    }
    assert_eq!(found(&db, "search").expect("built"), 20);
    let failed = db.index_build_errors();
    assert_eq!(failed.len(), 1, "{failed:?}");
    assert_eq!(failed[0].1, "broken", "{failed:?}");
}
