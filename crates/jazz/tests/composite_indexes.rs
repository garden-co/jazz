//! Declared composite current indexes, exercised through the public schema
//! builder and the `Db` read/write API, in memory and over durable
//! file-backed storage.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use jazz::db::{Db, DbConfig, DbIdentity, FileOptions, InsertOptions, ReadOpts, UpdateOptions};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol::{CurrentWriteSchema, MigrationLens, SchemaVersion, TableLens};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, Schema, SchemaBuilder, SchemaHash, TableSchemaBuilder};

use crate::common;

use common::allow_all_policies;

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

fn row(byte: u8) -> RowUuid {
    RowUuid::from_bytes([byte; 16])
}

fn tasks_table() -> TableSchemaBuilder {
    TableSchemaBuilder::new("tasks")
        .column("owner", ColumnType::Text)
        .column("rank", ColumnType::Integer)
        .column("title", ColumnType::Text)
        .policies(allow_all_policies())
}

fn public_schema() -> Schema {
    SchemaBuilder::new()
        .table(tasks_table().composite_index(["owner", "rank"]))
        .build()
}

/// The store file inside a test's scratch directory.
fn store_path(directory: &std::path::Path) -> std::path::PathBuf {
    directory.join("jazz.sqlite")
}

/// A durable file-backed store in `directory`, opened with `schema`.
fn open_file_db(directory: &std::path::Path, schema: &JazzSchema) -> Db {
    block_on(Db::open_at(
        DbConfig::new(
            schema.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([0xc1; 16]),
                author: AuthorSubject::SYSTEM,
            },
        ),
        &store_path(directory),
        FileOptions::default(),
    ))
    .unwrap()
}

fn insert_task(db: &Db, id: RowUuid, owner: &str, rank: i32, title: &str) {
    block_on(db.insert(
        "tasks",
        BTreeMap::from([
            ("owner".to_owned(), Value::String(owner.to_owned())),
            ("rank".to_owned(), Value::I32(rank)),
            ("title".to_owned(), Value::String(title.to_owned())),
        ]),
        InsertOptions {
            row_id: Some(id),
            ..Default::default()
        },
    ))
    .unwrap();
}

/// Titles of `owner`'s top two tasks by descending rank: the equality-prefix,
/// ordered-page shape a `(owner, rank)` composite index is declared for.
fn top_two_titles(db: &Db, owner: &str) -> Vec<String> {
    let query = db
        .prepare_query(
            &Query::from("tasks")
                .filter(eq(col("owner"), lit(Value::String(owner.to_owned()))))
                .order_by("rank", OrderDirection::Desc)
                .limit(2),
        )
        .unwrap();
    block_on(db.all(&query, ReadOpts::default()))
        .unwrap()
        .into_iter()
        .map(|row| match row.cell("title") {
            Some(Value::String(title)) => title,
            other => panic!("expected a title, got {other:?}"),
        })
        .collect()
}

/// A table that declares a `(owner, rank)` composite index answers alice's
/// "top two by rank" page correctly, keeps answering it after rows move
/// between owners and ranks, and still does after a restart that recovers the
/// composite-index schema and its index from the store file.
///
/// ```text
/// alice ──insert 3 tasks, bob 1──► db ──top2(alice)──► [ship, plan]
/// alice ──bob's task → alice rank 9; ship → rank 0──► top2(alice) = [review, plan]
/// db ──close──► reopen same directory ──top2(alice)──► [review, plan]
/// ```
#[test]
fn composite_index_schema_serves_ordered_pages_across_updates_and_reopen() {
    let schema = JazzSchema::new(&public_schema()).expect("composite schema compiles");
    let directory = tempfile::tempdir().unwrap();
    {
        let db = open_file_db(directory.path(), &schema);
        insert_task(&db, row(1), "alice", 3, "ship");
        insert_task(&db, row(2), "alice", 1, "draft");
        insert_task(&db, row(3), "alice", 2, "plan");
        insert_task(&db, row(4), "bob", 5, "review");
        assert_eq!(top_two_titles(&db, "alice"), ["ship", "plan"]);
        assert_eq!(top_two_titles(&db, "bob"), ["review"]);

        // Both indexed columns change: the row must leave bob's prefix and
        // enter alice's at its new position; `ship` must move down in place.
        block_on(db.update(
            "tasks",
            row(4),
            BTreeMap::from([
                ("owner".to_owned(), Value::String("alice".to_owned())),
                ("rank".to_owned(), Value::I32(9)),
            ]),
            UpdateOptions::default(),
        ))
        .unwrap();
        block_on(db.update(
            "tasks",
            row(1),
            BTreeMap::from([("rank".to_owned(), Value::I32(0))]),
            UpdateOptions::default(),
        ))
        .unwrap();
        assert_eq!(top_two_titles(&db, "alice"), ["review", "plan"]);
        assert!(top_two_titles(&db, "bob").is_empty());
    }

    let reopened = open_file_db(directory.path(), &schema);
    assert_eq!(top_two_titles(&reopened, "alice"), ["review", "plan"]);
    assert!(top_two_titles(&reopened, "bob").is_empty());
}

/// Composite indexes are part of schema identity, but only as a set: the
/// column order inside one index matters, the order in which a developer
/// declares distinct indexes does not, and schemas that differ only in
/// declaration order compare equal. Declarations the runtime could not
/// maintain as one ordered index are rejected when the schema is compiled.
#[test]
fn composite_index_declarations_are_canonical_and_validated() {
    let declared = |indexes: &[[&str; 2]]| {
        let mut table = tasks_table();
        for columns in indexes {
            table = table.composite_index(*columns);
        }
        SchemaBuilder::new().table(table).build()
    };
    let forward = declared(&[["owner", "rank"], ["rank", "owner"]]);
    let reversed = declared(&[["rank", "owner"], ["owner", "rank"]]);
    assert_eq!(forward, reversed);
    assert_eq!(
        SchemaHash::compute(&forward),
        SchemaHash::compute(&reversed)
    );
    assert_ne!(
        SchemaHash::compute(&declared(&[["owner", "rank"]])),
        SchemaHash::compute(&declared(&[["rank", "owner"]])),
        "column order inside an index is identity"
    );
    assert_ne!(
        SchemaHash::compute(&declared(&[["owner", "rank"]])),
        SchemaHash::compute(&declared(&[])),
        "declaring an index is identity"
    );

    let rejected = |table: TableSchemaBuilder| {
        JazzSchema::new(&SchemaBuilder::new().table(table).build()).is_err()
    };
    assert!(
        rejected(tasks_table().composite_index(["owner"])),
        "one column"
    );
    assert!(
        rejected(tasks_table().composite_index(["owner", "missing"])),
        "undeclared column"
    );
    assert!(
        rejected(tasks_table().composite_index(["owner", "owner"])),
        "repeated column"
    );
    assert!(
        rejected(
            tasks_table()
                .composite_index(["owner", "rank"])
                .composite_index(["owner", "rank"])
        ),
        "duplicate index"
    );
    assert!(
        rejected(
            tasks_table()
                .column("blob", ColumnType::Bytea)
                .composite_index(["owner", "blob"])
        ),
        "unordered column type"
    );
}

/// Name and definition of every index SQLite holds for `tasks` in the
/// store file, in name order.
fn stored_task_indexes(directory: &std::path::Path) -> Vec<(String, String)> {
    let conn = rusqlite::Connection::open(store_path(directory)).unwrap();
    let mut statement = conn
        .prepare(
            "SELECT name, sql FROM sqlite_master \
             WHERE type = 'index' AND tbl_name = 'tasks' AND sql IS NOT NULL ORDER BY name",
        )
        .unwrap();
    statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

/// Durable-encoding receipt for the composite index. Its name is its stored
/// key list, so reopening a store under any schema version that declares
/// the same index re-creates nothing; a silent respelling of the name or of
/// its key columns would build a second copy of every entry beside the
/// first. The index keys the declared columns in declared order (a text
/// column by its key expression) and ends in the row id.
#[test]
fn composite_index_definition_in_the_store_file_is_pinned() {
    let schema = JazzSchema::new(&public_schema()).expect("composite schema compiles");
    let directory = tempfile::tempdir().unwrap();
    {
        let db = open_file_db(directory.path(), &schema);
        insert_task(&db, row(0x0a), "alice", 7, "ship");
    }
    let composite = stored_task_indexes(directory.path())
        .into_iter()
        .filter(|(name, _)| name.contains(','))
        .collect::<Vec<_>>();
    assert_eq!(
        composite,
        [(
            "tasks__ix(owner,rank)".to_owned(),
            concat!(
                "CREATE INDEX \"tasks__ix(owner,rank)\" ON \"tasks\"(",
                "\"owner\", \"rank\", id)"
            )
            .to_owned()
        )]
    );
    let before = stored_task_indexes(directory.path());
    {
        let db = open_file_db(directory.path(), &schema);
        assert_eq!(top_two_titles(&db, "alice"), ["ship"]);
    }
    assert_eq!(
        stored_task_indexes(directory.path()),
        before,
        "reopening creates no index twice"
    );
}

/// Leading bytes of the row ids of `tasks` in `(owner, rank)` order, read
/// from the store file by a scan SQLite must serve from the composite index
/// (`INDEXED BY` fails the statement when the index cannot serve it). The
/// test row uuids are `[n; 16]`.
fn composite_index_row_order(directory: &std::path::Path) -> Vec<u8> {
    let conn = rusqlite::Connection::open(store_path(directory)).unwrap();
    let mut statement = conn
        .prepare(
            "SELECT id FROM tasks INDEXED BY \"tasks__ix(owner,rank)\" \
             WHERE owner IS NOT NULL ORDER BY owner, rank, id",
        )
        .unwrap();
    statement
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .unwrap()
        .map(|id| id.unwrap()[0])
        .collect()
}

/// Asserts that `owner`'s page is served from the composite index, not by
/// scanning and sorting the table.
fn assert_page_uses_composite_index(db: &Db, owner: &str) {
    let page = db
        .prepare_query(
            &Query::from("tasks")
                .filter(eq(col("owner"), lit(Value::String(owner.to_owned()))))
                .order_by("rank", OrderDirection::Desc)
                .limit(2),
        )
        .unwrap();
    let (_, plan) = db.explain_query_plan(&page, None).unwrap();
    assert!(plan.contains("tasks__ix(owner,rank)"), "{plan}");
    assert!(!plan.contains("TEMP B-TREE FOR ORDER BY"), "{plan}");
}

/// Publish `composite` as the identity-lens descendant of `plain` and make it
/// the write schema: the catalogue migration path for a schema change to a
/// live store.
fn publish_composite_descendant(db: &Db, plain: &JazzSchema, composite: &JazzSchema) {
    let target = SchemaVersion::new(composite.clone());
    let lens = MigrationLens::new(
        plain.version_id(),
        target.id,
        vec![TableLens {
            source_table: "tasks".into(),
            target_table: "tasks".into(),
            ops: vec![],
        }],
    )
    .unwrap();
    let publication = db
        .author_schema_lineage_publication(target.clone(), lens)
        .unwrap();
    block_on(db.publish_schema_with_lens(publication)).unwrap();
    block_on(db.activate_catalogue_schema_for_test(CurrentWriteSchema {
        revision: 1,
        schema: target.id,
    }))
    .unwrap();
}

/// Adding a composite index to an existing app is a schema change: the
/// composite schema is a new schema version. A store written under the
/// plain schema takes the index whether the composite schema arrives by
/// reopening the file with it or by publishing it to the live store through
/// an identity lens. Either way the index is backfilled with the rows written
/// before it existed and maintained for rows written after, across a
/// reopen, in `(owner, rank)` order.
///
/// ```text
/// open(plain) ──insert r1 alice/2, r2 bob/1──► close
/// open(plain + composite) ──insert r3 alice/1──► close
/// open(plain + composite) ──insert r4 bob/0──► close
///     index order = [r3 alice/1, r1 alice/2, r4 bob/0, r2 bob/1]
///
/// open(plain) ──insert r1, r2──► publish composite via identity lens
///     ──insert r3──► close ──► open(plain + composite) ──insert r4──► same order
/// ```
#[test]
fn adding_composite_index_to_existing_store_maintains_and_backfills_it() {
    let plain = JazzSchema::new(&SchemaBuilder::new().table(tasks_table()).build())
        .expect("plain schema compiles");
    let composite = JazzSchema::new(&public_schema()).expect("composite schema compiles");
    assert_ne!(
        plain.version_id(),
        composite.version_id(),
        "declaring a composite index is a new schema version"
    );

    // The composite schema reaches the file by reopening it.
    let reopened = tempfile::tempdir().unwrap();
    {
        let db = open_file_db(reopened.path(), &plain);
        insert_task(&db, row(1), "alice", 2, "plan");
        insert_task(&db, row(2), "bob", 1, "review");
    }
    assert!(
        stored_task_indexes(reopened.path())
            .iter()
            .all(|(name, _)| !name.contains(',')),
        "the plain schema declares no composite index"
    );
    {
        let db = open_file_db(reopened.path(), &composite);
        insert_task(&db, row(3), "alice", 1, "draft");
        assert_page_uses_composite_index(&db, "alice");
        assert_eq!(top_two_titles(&db, "alice"), ["plan", "draft"]);
    }
    {
        let db = open_file_db(reopened.path(), &composite);
        insert_task(&db, row(4), "bob", 0, "triage");
        assert_eq!(top_two_titles(&db, "bob"), ["review", "triage"]);
    }
    assert_eq!(composite_index_row_order(reopened.path()), [3, 1, 4, 2]);

    // The composite schema reaches the live store as a published descendant.
    let published = tempfile::tempdir().unwrap();
    {
        let db = open_file_db(published.path(), &plain);
        insert_task(&db, row(1), "alice", 2, "plan");
        insert_task(&db, row(2), "bob", 1, "review");
        publish_composite_descendant(&db, &plain, &composite);
        insert_task(&db, row(3), "alice", 1, "draft");
        assert_page_uses_composite_index(&db, "alice");
        assert_eq!(top_two_titles(&db, "alice"), ["plan", "draft"]);
    }
    {
        let db = open_file_db(published.path(), &composite);
        insert_task(&db, row(4), "bob", 0, "triage");
        assert_eq!(top_two_titles(&db, "bob"), ["review", "triage"]);
    }
    assert_eq!(composite_index_row_order(published.path()), [3, 1, 4, 2]);
}

/// Durable-encoding receipt for schema version ids. The id is a UUIDv5 over
/// the schema's canonical bytes and addresses every catalogue record, so it
/// must never drift for an existing schema: a schema without composite
/// indexes keeps its frozen `jazz-schema-v1-large-value-kinds` id (this value
/// predates composite indexes), and a schema that declares one is addressed
/// under the distinct `jazz-schema-v2-composite-indexes` domain, with its
/// indexes in canonical order so declaration order is not identity.
#[test]
fn schema_version_ids_are_pinned_for_plain_and_composite_schemas() {
    let plain = JazzSchema::new(&SchemaBuilder::new().table(tasks_table()).build())
        .expect("plain schema compiles");
    assert_eq!(
        plain.version_id().0.to_string(),
        "ea0daf2b-1053-5c20-87ac-ac6cec6e0a05"
    );
    let composite = JazzSchema::new(&public_schema()).expect("composite schema compiles");
    assert_eq!(
        composite.version_id().0.to_string(),
        "f68ae7b9-71bd-5411-9bd8-40f52d154e79"
    );
    let two = |first: [&str; 2], second: [&str; 2]| {
        JazzSchema::new(
            &SchemaBuilder::new()
                .table(tasks_table().composite_index(first).composite_index(second))
                .build(),
        )
        .expect("composite schema compiles")
        .version_id()
    };
    assert_eq!(
        two(["owner", "rank"], ["rank", "owner"]).0.to_string(),
        "e07667f6-7076-597e-ad37-f1829ff8bd96"
    );
    assert_eq!(
        two(["owner", "rank"], ["rank", "owner"]),
        two(["rank", "owner"], ["owner", "rank"])
    );
}

/// The public `composite_indexes` field is a plain `Vec`, so a schema can be
/// built without the builder's canonical sort. Such a hand-built, unsorted
/// table is the same schema as the sorted one everywhere schemas are
/// compared, hashed, or encoded: equality, `SchemaHash`, the serialized public
/// schema, and the compiled schema's version id all agree.
#[test]
fn hand_built_unsorted_composite_indexes_are_canonical() {
    let sorted = SchemaBuilder::new()
        .table(
            tasks_table()
                .composite_index(["owner", "rank"])
                .composite_index(["rank", "owner"]),
        )
        .build();
    let mut unsorted = sorted.clone();
    for (_, table) in &mut unsorted {
        table.composite_indexes.reverse();
        assert_eq!(table.composite_indexes[0][0].as_str(), "rank");
    }

    assert_eq!(unsorted, sorted);
    assert_eq!(SchemaHash::compute(&unsorted), SchemaHash::compute(&sorted));
    assert_eq!(
        serde_json::to_string(&unsorted).unwrap(),
        serde_json::to_string(&sorted).unwrap()
    );
    assert_eq!(
        JazzSchema::new(&unsorted).unwrap().version_id(),
        JazzSchema::new(&sorted).unwrap().version_id()
    );

    let mut different = sorted.clone();
    for (_, table) in &mut different {
        table.composite_indexes.pop();
    }
    assert_ne!(different, sorted, "the set of indexes is still identity");
}

/// A composite index's logical name cannot collide with a single-column
/// index, whatever the column is called. `composite_5_owner_4_rank` is the
/// column name that would have spelled the `(owner, rank)` composite index's
/// former `by_app_composite_5_owner_4_rank` name as `by_app_<column>`.
///
/// The logical global-current table is a public lowering descriptor, not a
/// query result, so this inspects its index names directly.
#[test]
fn composite_index_logical_names_cannot_collide_with_single_column_indexes() {
    let schema = JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                tasks_table()
                    .column("composite_5_owner_4_rank", ColumnType::Text)
                    .index_only(["composite_5_owner_4_rank"])
                    .composite_index(["owner", "rank"]),
            )
            .build(),
    )
    .expect("schema compiles");
    let global_current = &schema.tables()[0].global_current_storage_table();
    let names = global_current
        .indices
        .iter()
        .map(|index| index.name.as_str())
        .collect::<Vec<_>>();
    assert!(
        names.contains(&"by_app_composite_5_owner_4_rank"),
        "{names:?}"
    );
    assert!(names.contains(&"by_composite_5_owner_4_rank"), "{names:?}");
    let distinct = names.iter().collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        distinct.len(),
        names.len(),
        "duplicate index name: {names:?}"
    );
}

/// A settled row a history-complete authority seeds (the server
/// bootstrap/import path) lands in the same table, and is served by the
/// same composite index, as a local write: alice's seeded row and her local
/// row share one `(owner, rank)` page.
#[test]
fn settled_rows_are_served_by_the_composite_index() {
    let schema = JazzSchema::new(&public_schema()).expect("composite schema compiles");
    let db = block_on(Db::open_history_complete(DbConfig::new(
        schema.clone(),
        DbIdentity {
            node: NodeUuid::from_bytes([0xc1; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    db.seed_settled_mergeable_for_bootstrap(
        "tasks",
        row(0x0a),
        AuthorSubject::SYSTEM,
        BTreeMap::from([
            ("owner".to_owned(), Value::String("alice".to_owned())),
            ("rank".to_owned(), Value::I32(7)),
            ("title".to_owned(), Value::String("ship".to_owned())),
        ]),
    )
    .unwrap();
    insert_task(&db, row(0x0b), "alice", 3, "plan");
    insert_task(&db, row(0x0c), "bob", 9, "review");
    assert_page_uses_composite_index(&db, "alice");
    assert_eq!(top_two_titles(&db, "alice"), ["ship", "plan"]);
    assert_eq!(top_two_titles(&db, "bob"), ["review"]);
}

/// A composite index may end in a provenance timestamp: a chat's newest
/// page by `$createdAt` (or `$updatedAt`, read as its create stamp until
/// the first update) is served from the index, without sorting the chat.
#[test]
fn composite_indexes_can_order_by_provenance_timestamps() {
    for stamp in ["$createdAt", "$updatedAt"] {
        let schema = JazzSchema::new(
            &SchemaBuilder::new()
                .table(tasks_table().composite_index(["owner", stamp]))
                .build(),
        )
        .unwrap();
        let db = block_on(Db::open(DbConfig::new(
            schema,
            DbIdentity {
                node: NodeUuid::from_bytes([1; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )))
        .unwrap();
        for n in 0..20u8 {
            block_on(db.insert(
                "tasks",
                BTreeMap::from([
                    ("owner".to_owned(), Value::String("ada".to_owned())),
                    ("rank".to_owned(), Value::I32(i32::from(n))),
                    ("title".to_owned(), Value::String(format!("t{n}"))),
                ]),
                InsertOptions {
                    row_id: Some(row(n + 1)),
                    ..Default::default()
                },
            ))
            .unwrap();
        }
        let page = db
            .prepare_query(
                &Query::from("tasks")
                    .filter(eq(col("owner"), lit(Value::String("ada".to_owned()))))
                    .order_by(stamp, OrderDirection::Desc)
                    .limit(3),
            )
            .unwrap();
        let (_, plan) = db.explain_query_plan(&page, None).unwrap();
        assert!(plan.contains("tasks__ix"), "{stamp}: {plan}");
        assert!(
            !plan.contains("TEMP B-TREE FOR ORDER BY"),
            "{stamp}: {plan}"
        );
        assert_eq!(
            block_on(db.all(&page, ReadOpts::default())).unwrap().len(),
            3
        );
    }
}

/// A single-column index that a composite one leads is not created (the
/// composite serves its reads, and writes keep one entry fewer), and a read
/// that names row ids seeks them rather than scanning a declared index that
/// also covers its other filter.
#[test]
fn composite_indexes_subsume_their_prefixes_and_id_reads_seek_by_id() {
    let schema = JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                tasks_table()
                    .index_only(["owner"])
                    .composite_index(["owner", "rank"]),
            )
            .build(),
    )
    .unwrap();
    let db = block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([1; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    for n in 0..20u8 {
        block_on(db.insert(
            "tasks",
            BTreeMap::from([
                ("owner".to_owned(), Value::String("ada".to_owned())),
                ("rank".to_owned(), Value::I32(i32::from(n))),
                ("title".to_owned(), Value::String(format!("t{n}"))),
            ]),
            InsertOptions {
                row_id: Some(row(n + 1)),
                ..Default::default()
            },
        ))
        .unwrap();
    }
    let ada =
        || Query::from("tasks").filter(eq(col("owner"), lit(Value::String("ada".to_owned()))));
    let (_, plan) = db
        .explain_query_plan(&db.prepare_query(&ada()).unwrap(), None)
        .unwrap();
    assert!(plan.contains("tasks__ix(owner,rank)"), "{plan}");

    let point = db
        .prepare_query(&ada().filter(eq(col("id"), lit(Value::Uuid(row(5).0)))))
        .unwrap();
    let (_, plan) = db.explain_query_plan(&point, None).unwrap();
    assert!(plan.contains("(id=? AND _b=?)"), "{plan}");
    let rows = block_on(db.all(&point, ReadOpts::default())).unwrap();
    assert_eq!(rows.len(), 1);
}

/// #3399: reopening an existing store under a schema that adds a column to
/// `index_only` builds that column's index over the rows already stored.
/// The index set is not part of the schema version: a deployment may
/// refresh it without a new version.
#[test]
fn adding_an_index_only_column_to_an_existing_store_backfills_its_index() {
    let narrow = JazzSchema::new(
        &SchemaBuilder::new()
            .table(tasks_table().index_only(["owner"]))
            .build(),
    )
    .expect("narrow schema compiles");
    let wider = JazzSchema::new(
        &SchemaBuilder::new()
            .table(tasks_table().index_only(["owner", "title"]))
            .build(),
    )
    .expect("wider schema compiles");
    assert_eq!(narrow.version_id(), wider.version_id());

    let directory = tempfile::tempdir().unwrap();
    {
        let db = open_file_db(directory.path(), &narrow);
        insert_task(&db, row(1), "alice", 2, "plan");
        insert_task(&db, row(2), "bob", 1, "review");
    }
    // The value index on a column (not its large-value marker index).
    let index_on = |column: &str| {
        stored_task_indexes(directory.path())
            .into_iter()
            .find(|(name, _)| *name == format!("tasks__ix({column})"))
    };
    assert!(index_on("owner").is_some());
    assert_eq!(
        index_on("title"),
        None,
        "the narrow schema indexes no title"
    );
    {
        let db = open_file_db(directory.path(), &wider);
        let titles = |title: &str| {
            let query = db
                .prepare_query(
                    &Query::from("tasks")
                        .filter(eq(col("title"), lit(Value::String(title.into())))),
                )
                .unwrap();
            block_on(db.all(&query, ReadOpts::default())).unwrap().len()
        };
        assert_eq!((titles("plan"), titles("review")), (1, 1));
    }
    let (name, _) = index_on("title").expect("reopening builds the title index");
    let conn = rusqlite::Connection::open(store_path(directory.path())).unwrap();
    let entries: i64 = conn
        .query_row(
            &format!("SELECT count(*) FROM tasks INDEXED BY \"{name}\""),
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(entries, 2, "the index holds the rows stored before it");
}
