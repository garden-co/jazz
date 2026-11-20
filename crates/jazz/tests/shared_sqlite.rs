//! Jazz owns the tables recorded in its schema catalogue, not the whole file.
use crate::common::{allow_all_policies, compile_schema};
use jazz::{
    db::{Db, DbConfig, DbIdentity, ErrorCode, FileOptions, ReadOpts, block_on},
    ids::{AuthorSubject, NodeUuid},
    query::Query,
    records::Value,
    schema::JazzSchema,
    tools::{ColumnType, SchemaBuilder, TableSchemaBuilder},
};
use std::{collections::BTreeMap, path::Path};

fn schema(tables: &[&str]) -> JazzSchema {
    let mut builder = SchemaBuilder::new();
    for name in tables {
        builder = builder.table(
            TableSchemaBuilder::new(name)
                .column("body", ColumnType::Text)
                .policies(allow_all_policies()),
        );
    }
    compile_schema(&builder.build())
}
fn config(schema: JazzSchema) -> DbConfig {
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
fn insert(db: &Db, table: &str, body: &str) {
    block_on(db.insert(
        table,
        BTreeMap::from([("body".into(), Value::String(body.into()))]),
        Default::default(),
    ))
    .unwrap();
}
fn bodies(db: &Db, table: &str) -> Vec<String> {
    let query = db.prepare_query(&Query::from(table)).unwrap();
    block_on(db.all(&query, ReadOpts::default()))
        .unwrap()
        .into_iter()
        .map(|row| {
            let Some(Value::String(body)) = row.cell("body") else {
                panic!("body must be text");
            };
            body
        })
        .collect()
}

/// Alice shares SQLite with another application. Jazz initializes, opens a
/// later schema, and reopens under an older one without changing Bob's tables.
/// Raw SQL represents the other application's storage, not Jazz's data path.
#[test]
fn unrelated_tables_survive_initialization_and_historical_schema_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("shared.sqlite");
    let other = rusqlite::Connection::open(&path).unwrap();
    other.execute_batch("CREATE TABLE foreign_data (value TEXT); INSERT INTO foreign_data VALUES ('bob'); CREATE TABLE cache__h (v BLOB); INSERT INTO cache__h VALUES (X'00FF'); CREATE TABLE meta (key TEXT PRIMARY KEY, value BLOB NOT NULL) STRICT; INSERT INTO meta VALUES ('format', X'0102');").unwrap();
    let before: Vec<(String, Option<String>)> = other
        .prepare("SELECT name, sql FROM sqlite_master ORDER BY name")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let old = schema(&["notes"]);
    let new = schema(&["notes", "tasks"]);
    {
        let db = open(&path, old.clone());
        insert(&db, "notes", "alice");
    }
    {
        let db = open(&path, new.clone());
        insert(&db, "tasks", "later");
    }
    {
        let db = open(&path, old);
        assert_eq!(bodies(&db, "notes"), ["alice"]);
        let later = block_on(db.register_schema_view(new)).unwrap();
        assert_eq!(bodies(&later, "tasks"), ["later"]);
    }
    for (name, ddl) in before {
        assert_eq!(
            other
                .query_row(
                    "SELECT sql FROM sqlite_master WHERE name = ?1",
                    [name],
                    |r| r.get::<_, Option<String>>(0)
                )
                .unwrap(),
            ddl
        );
    }
    assert_eq!(
        other
            .query_row("SELECT value FROM foreign_data", [], |r| r
                .get::<_, String>(0))
            .unwrap(),
        "bob"
    );
    assert_eq!(
        other
            .query_row("SELECT v FROM cache__h", [], |r| r.get::<_, Vec<u8>>(0))
            .unwrap(),
        [0, 255]
    );
    assert_eq!(
        other
            .query_row("SELECT value FROM meta WHERE key = 'format'", [], |r| r
                .get::<_, Vec<u8>>(0))
            .unwrap(),
        [1, 2]
    );
}

/// Alice cannot adopt Bob's table, history-name table, clock-index name.
/// Raw SQL constructs objects owned by Bob; every collision is reported through
/// Jazz's public open API and Bob's existing value remains untouched.
#[test]
fn new_table_collisions_are_rejected_without_adopting_foreign_objects() {
    for object in ["notes", "NOTES", "notes__h", "notes__by_clock"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("collision.sqlite");
        let other = rusqlite::Connection::open(&path).unwrap();
        other
            .execute_batch(&format!(
                "CREATE TABLE \"{object}\" (value TEXT); INSERT INTO \"{object}\" VALUES ('bob')"
            ))
            .unwrap();
        let error = block_on(Db::open_at(
            config(schema(&["notes"])),
            &path,
            FileOptions::default(),
        ))
        .err()
        .expect("must reject collision");
        assert_eq!(error.code, ErrorCode::Schema);
        assert!(error.message.contains(object));
        assert_eq!(
            other
                .query_row(&format!("SELECT value FROM \"{object}\""), [], |r| r
                    .get::<_, String>(0))
                .unwrap(),
            "bob"
        );
    }
}

/// Alice adds a schema version whose new table conflicts with Bob's table.
/// The failed admission leaves Alice's existing rows and Bob's rows intact.
#[test]
fn later_schema_collisions_leave_both_applications_intact() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("admission.sqlite");
    let db = open(&path, schema(&["notes"]));
    insert(&db, "notes", "alice");
    let other = rusqlite::Connection::open(&path).unwrap();
    other
        .execute_batch("CREATE TABLE tasks (value TEXT); INSERT INTO tasks VALUES ('bob')")
        .unwrap();
    let error = block_on(Db::open_at(
        config(schema(&["notes", "tasks"])),
        &path,
        FileOptions::default(),
    ))
    .err()
    .expect("must reject collision");
    assert_eq!(error.code, ErrorCode::Schema);
    assert_eq!(bodies(&db, "notes"), ["alice"]);
    assert_eq!(
        other
            .query_row("SELECT value FROM tasks", [], |r| r.get::<_, String>(0))
            .unwrap(),
        "bob"
    );
    drop(db);
    assert_eq!(bodies(&open(&path, schema(&["notes"])), "notes"), ["alice"]);
}

/// Alice's catalogue-owned tables remain visible regardless of provider-like
/// names or a history-like suffix; Bob's zero-column table also survives reopen.
#[test]
fn ownership_does_not_depend_on_prefix_suffix_or_user_columns() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("names.sqlite");
    let mut builder =
        SchemaBuilder::new().table(TableSchemaBuilder::new("empty").policies(allow_all_policies()));
    for name in ["_rivet_notes", "_cf_notes", "archive__h"] {
        builder = builder.table(
            TableSchemaBuilder::new(name)
                .column("body", ColumnType::Text)
                .policies(allow_all_policies()),
        );
    }
    let schema = compile_schema(&builder.build());
    {
        let db = open(&path, schema.clone());
        for name in ["_rivet_notes", "_cf_notes", "archive__h"] {
            insert(&db, name, "alice");
        }
        block_on(db.insert("empty", BTreeMap::new(), Default::default())).unwrap();
    }
    let db = open(&path, schema);
    for name in ["_rivet_notes", "_cf_notes", "archive__h"] {
        assert_eq!(bodies(&db, name), ["alice"]);
    }
    let query = db.prepare_query(&Query::from("empty")).unwrap();
    assert_eq!(
        block_on(db.all(&query, ReadOpts::default())).unwrap().len(),
        1
    );
}

/// Alice's missing owned table is corruption, not a fresh database. Direct SQL
/// deliberately damages the fixture; reopening must not recreate an empty table.
#[test]
fn a_missing_catalogued_table_is_not_recreated() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("missing.sqlite");
    {
        let db = open(&path, schema(&["notes"]));
        insert(&db, "notes", "alice");
    }
    let other = rusqlite::Connection::open(&path).unwrap();
    other.execute_batch("DROP TABLE notes").unwrap();
    let error = block_on(Db::open_at(
        config(schema(&["notes"])),
        &path,
        FileOptions::default(),
    ))
    .err()
    .expect("must refuse missing table");
    assert_eq!(error.code, ErrorCode::Storage);
    assert_eq!(
        other
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE name = 'notes'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
}

/// Alice opens Jazz on managed SQL beside Bob's tables. The storage-emulation
/// backend exercises the same callback scopes used by a Durable Object.
/// Raw SQL models Bob's data, which is deliberately outside Jazz's row API.
#[test]
fn managed_storage_preserves_unrelated_tables() {
    use jazz::host::HostTables;
    let storage = jazz::storage::Storage::managed_emulation_in_memory().unwrap();
    storage.execute_batch("CREATE TABLE foreign_data (v TEXT); INSERT INTO foreign_data VALUES ('bob'); CREATE TABLE _cf_metadata (v TEXT); INSERT INTO _cf_metadata VALUES ('host')").unwrap();
    let db = block_on(Db::open_storage(config(schema(&["notes"])), storage)).unwrap();
    insert(&db, "notes", "alice");
    assert_eq!(bodies(&db, "notes"), ["alice"]);
    db.host_sql(&mut |storage| {
        assert_eq!(
            storage
                .query_row("SELECT v FROM foreign_data", [], |r| r.get::<_, String>(0))
                .unwrap(),
            "bob"
        );
        assert_eq!(
            storage
                .query_row("SELECT v FROM _cf_metadata", [], |r| r.get::<_, String>(0))
                .unwrap(),
            "host"
        );
        Ok(())
    })
    .unwrap();
}

/// Alice must not initialize over Bob's legacy Groove store, even when its
/// metadata DDL uses different capitalization. Direct SQL constructs the old
/// encoding; refusal must leave its bytes unchanged.
#[test]
fn legacy_groove_markers_are_refused_without_modification() {
    for ddl in [
        "CREATE TABLE meta (key TEXT PRIMARY KEY, value BLOB NOT NULL) STRICT",
        "CREATE TABLE META (KEY TEXT PRIMARY KEY, VALUE BLOB)",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.sqlite");
        {
            let other = rusqlite::Connection::open(&path).unwrap();
            other.execute_batch(ddl).unwrap();
            other
                .execute(
                    "INSERT INTO meta VALUES ('format', ?1)",
                    [b"jazz-groove-ordered-kv".as_slice()],
                )
                .unwrap();
        }
        let before = std::fs::read(&path).unwrap();
        let error = block_on(Db::open_at(
            config(schema(&["notes"])),
            &path,
            FileOptions::default(),
        ))
        .err()
        .expect("legacy store must be refused");
        assert_eq!(error.code, ErrorCode::UnsupportedStorageCodecs);
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }
}

/// Alice's first schema must not reuse Bob's refcount-trigger names. Direct
/// SQL creates Bob's objects; a rejected open leaves his trigger and rows intact.
#[test]
fn foreign_refcount_trigger_names_refuse_initialization() {
    for target in ["notes", "notes__h"] {
        for operation in ["ins", "del", "upd"] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("trigger.sqlite");
            let other = rusqlite::Connection::open(&path).unwrap();
            let trigger = format!("{target}__lv_{operation}").to_uppercase();
            other.execute_batch(&format!("CREATE TABLE other (value TEXT); INSERT INTO other VALUES ('bob'); CREATE TRIGGER \"{trigger}\" AFTER INSERT ON other BEGIN SELECT 1; END;")).unwrap();
            let error = block_on(Db::open_at(
                config(schema(&["notes"])),
                &path,
                FileOptions::default(),
            ))
            .err()
            .expect("must refuse a foreign trigger");
            assert!(
                error
                    .message
                    .to_lowercase()
                    .contains(&trigger.to_lowercase()),
                "{error}"
            );
            assert_eq!(
                other
                    .query_row(
                        "SELECT tbl_name FROM sqlite_master WHERE name = ?1",
                        [&trigger],
                        |row| row.get::<_, String>(0)
                    )
                    .unwrap(),
                "other"
            );
            assert_eq!(
                other
                    .query_row("SELECT value FROM other", [], |row| row.get::<_, String>(0))
                    .unwrap(),
                "bob"
            );
            assert_eq!(
                other
                    .query_row(
                        "SELECT count(*) FROM sqlite_master WHERE name = 'notes'",
                        [],
                        |row| row.get::<_, i64>(0)
                    )
                    .unwrap(),
                0
            );
        }
    }
}

/// Alice adds a large-capable column to an existing schema; Bob already owns
/// its derived trigger name. The failed admission must leave Alice's rows and
/// old schema usable, with neither current nor history columns partly added.
#[test]
fn foreign_refcount_trigger_names_refuse_later_column_sets() {
    let later = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("body", ColumnType::Text)
                    .column_with_default(
                        "extra",
                        ColumnType::Text,
                        jazz::tools::Value::Text(String::new()),
                    )
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    // Obtain the derived suffix from a successful admission: new column
    // lineages include a schema-derived suffix, not just their logical name.
    let reference = tempfile::tempdir().unwrap();
    let reference_path = reference.path().join("reference.sqlite");
    let reference_db = open(&reference_path, schema(&["notes"]));
    block_on(
        reference_db.apply_catalogue(jazz::catalogue::CatalogueChange {
            admit: vec![(later.clone(), vec![])],
            ..Default::default()
        }),
    )
    .unwrap();
    let reference_sql = rusqlite::Connection::open(reference_path).unwrap();
    let trigger: String = reference_sql
        .query_row(
            "SELECT name FROM sqlite_master WHERE type = 'trigger' AND name GLOB 'notes__lv_ins_*'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let suffix = trigger.strip_prefix("notes__lv_ins_").unwrap();
    for target in ["notes", "notes__h"] {
        for operation in ["ins", "del", "upd"] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("later-trigger.sqlite");
            let db = open(&path, schema(&["notes"]));
            insert(&db, "notes", "alice");
            let other = rusqlite::Connection::open(&path).unwrap();
            let trigger = format!("{target}__lv_{operation}_{suffix}");
            other.execute_batch(&format!("CREATE TABLE other (value TEXT); INSERT INTO other VALUES ('bob'); CREATE TRIGGER \"{trigger}\" AFTER INSERT ON other BEGIN SELECT 1; END;")).unwrap();
            let error = block_on(db.apply_catalogue(jazz::catalogue::CatalogueChange {
                admit: vec![(later.clone(), vec![])],
                ..Default::default()
            }))
            .expect_err("must refuse a foreign per-set trigger");
            assert!(error.message.contains(&trigger), "{error}");
            assert_eq!(bodies(&db, "notes"), ["alice"]);
            assert_eq!(
                other
                    .query_row(
                        "SELECT tbl_name FROM sqlite_master WHERE name = ?1",
                        [&trigger],
                        |row| row.get::<_, String>(0)
                    )
                    .unwrap(),
                "other"
            );
            for table in ["notes", "notes__h"] {
                assert_eq!(
                    other
                        .query_row(
                            "SELECT count(*) FROM pragma_table_info(?1) WHERE name GLOB 'extra~*'",
                            [table],
                            |row| row.get::<_, i64>(0)
                        )
                        .unwrap(),
                    0
                );
            }
            drop(db);
            assert_eq!(bodies(&open(&path, schema(&["notes"])), "notes"), ["alice"]);
        }
    }
}

/// Bob's index cannot satisfy Alice's index DDL, either at first open or when
/// she adds an index later. Once Bob removes the collision, Alice's own index
/// is created and reopening remains idempotent.
#[test]
fn foreign_index_names_refuse_initial_and_later_schemas() {
    let notes = || {
        TableSchemaBuilder::new("notes")
            .column("body", ColumnType::Text)
            .policies(allow_all_policies())
    };
    let indexed = compile_schema(
        &SchemaBuilder::new()
            .table(notes().index_only(["body"]))
            .build(),
    );
    let unindexed = compile_schema(
        &SchemaBuilder::new()
            .table(notes().index_only([] as [&str; 0]))
            .build(),
    );
    let reference = tempfile::tempdir().unwrap();
    let reference_path = reference.path().join("reference.sqlite");
    drop(open(&reference_path, indexed.clone()));
    let reference_sql = rusqlite::Connection::open(reference_path).unwrap();
    let index: String = reference_sql
        .query_row(
            "SELECT name FROM sqlite_master WHERE type = 'index' AND name GLOB 'notes__ix(*)'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let foreign = index.to_uppercase();
    for existing in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index.sqlite");
        if existing {
            let db = open(&path, unindexed.clone());
            insert(&db, "notes", "alice");
        }
        let other = rusqlite::Connection::open(&path).unwrap();
        other.execute_batch(&format!("CREATE TABLE other (value TEXT); INSERT INTO other VALUES ('bob'); CREATE INDEX \"{foreign}\" ON other(value);")).unwrap();
        let error = block_on(Db::open_at(
            config(indexed.clone()),
            &path,
            FileOptions::default(),
        ))
        .err()
        .expect("must refuse a foreign index");
        assert_eq!(error.code, ErrorCode::Schema);
        assert!(error.message.contains(&foreign));
        assert_eq!(
            other
                .query_row(
                    "SELECT tbl_name FROM sqlite_master WHERE name = ?1",
                    [&foreign],
                    |row| row.get::<_, String>(0)
                )
                .unwrap(),
            "other"
        );
        assert_eq!(
            other
                .query_row("SELECT value FROM other", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "bob"
        );
        other
            .execute_batch(&format!("DROP INDEX \"{foreign}\""))
            .unwrap();
        drop(open(&path, indexed.clone()));
        let db = open(&path, indexed.clone());
        assert_eq!(
            other
                .query_row(
                    "SELECT tbl_name FROM sqlite_master WHERE name = ?1",
                    [&index],
                    |row| row.get::<_, String>(0)
                )
                .unwrap(),
            "notes"
        );
        if existing {
            assert_eq!(bodies(&db, "notes"), ["alice"]);
        }
    }
}

/// Alice's rejected open must leave Bob's SQLite schema exactly as it was,
/// including no Jazz format stamp or metadata. Both early table collisions and
/// late trigger collisions roll back; Alice can then retry with a valid schema.
#[test]
fn rejected_initialization_leaves_no_jazz_objects() {
    use jazz::storage::{Storage, managed_emulation::EmulatedObjectSql};
    for managed in [false, true] {
        for trigger in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("atomic.sqlite");
            let backend = EmulatedObjectSql::open_in_memory().unwrap();
            let observer = if managed {
                Storage::from_backend(backend.clone())
            } else {
                Storage::open(&path).unwrap()
            };
            let connect = || {
                if managed {
                    Storage::from_backend(backend.clone())
                } else {
                    Storage::open(&path).unwrap()
                }
            };
            observer.execute_batch(if trigger {
                "CREATE TABLE other (value TEXT); INSERT INTO other VALUES ('bob'); CREATE TRIGGER notes__lv_ins AFTER INSERT ON other BEGIN SELECT 1; END;"
            } else {
                "CREATE TABLE notes (value TEXT); INSERT INTO notes VALUES ('bob');"
            }).unwrap();
            let objects = || {
                observer
                    .prepare(
                        "SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name",
                    )
                    .unwrap()
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, Option<String>>(3)?,
                        ))
                    })
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap()
            };
            let before = objects();
            let result = if managed {
                block_on(Db::open_storage(config(schema(&["notes"])), connect()))
            } else {
                block_on(Db::open_at(
                    config(schema(&["notes"])),
                    &path,
                    FileOptions::default(),
                ))
            };
            assert!(result.is_err());
            drop(result);
            assert_eq!(objects(), before);
            let table = if trigger { "other" } else { "notes" };
            assert_eq!(
                observer
                    .query_row(&format!("SELECT value FROM {table}"), [], |row| row
                        .get::<_, String>(0))
                    .unwrap(),
                "bob"
            );
            let db = block_on(Db::open_storage(config(schema(&["messages"])), connect())).unwrap();
            insert(&db, "messages", "alice");
            drop(db);
            let reopened =
                block_on(Db::open_storage(config(schema(&["messages"])), connect())).unwrap();
            assert_eq!(bodies(&reopened, "messages"), ["alice"]);
        }
    }
}

/// Alice opens a host store without writing, then a failed host write rolls
/// back its format stamp too. A successful host write stamps the store and
/// survives a later rejected database initialization on the same store.
#[test]
fn host_store_stamps_only_with_a_committed_write() {
    use jazz::host::{HostTable, HostTables, StoreFile};
    use jazz::storage::{Storage, managed_emulation::EmulatedObjectSql};
    for managed in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("host.sqlite");
        let backend = EmulatedObjectSql::open_in_memory().unwrap();
        let observer = if managed {
            Storage::from_backend(backend.clone())
        } else {
            Storage::open(&path).unwrap()
        };
        let connect = || {
            if managed {
                Storage::from_backend(backend.clone())
            } else {
                Storage::open(&path).unwrap()
            }
        };
        let file = StoreFile::from_storage(connect()).unwrap();
        let count = || {
            observer
                .query_row(
                    "SELECT count(*) FROM sqlite_master WHERE name GLOB '_jazz_*'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap()
        };
        assert_eq!(count(), 0);
        assert!(file.catalogue().unwrap().schemas.is_empty());
        assert!(
            file.host_sql(&mut |conn| {
                conn.execute_batch("CREATE TABLE _jazz_host_failed (v TEXT)")
                    .unwrap();
                Err(jazz::db::Error::new(ErrorCode::Storage, "injected failure"))
            })
            .is_err()
        );
        assert_eq!(count(), 0);
        let table = HostTable::new("settings");
        file.host_put_if_absent(table, b"key", b"alice").unwrap();
        observer
            .execute_batch("CREATE TABLE notes (v TEXT)")
            .unwrap();
        assert!(block_on(Db::open_file(config(schema(&["notes"])), file)).is_err());
        let file = StoreFile::from_storage(connect()).unwrap();
        assert_eq!(
            file.host_get(table, b"key").unwrap(),
            Some(b"alice".to_vec())
        );
        assert_eq!(count(), 2, "only the committed stamp and host table remain");
    }
}

/// Alice's first deployment collides with Bob's table after initializing its
/// genesis. Failure rolls back all Jazz initialization; Bob's data survives,
/// and Alice can retry with a different genesis. Raw SQL observes ownership
/// boundaries that the Jazz row API intentionally cannot expose.
#[test]
fn first_deployment_rolls_back_initialization_and_can_retry() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("initial.sqlite");
    let other = rusqlite::Connection::open(&path).unwrap();
    other
        .execute_batch("CREATE TABLE tasks (value TEXT); INSERT INTO tasks VALUES ('bob')")
        .unwrap();
    let file = jazz::host::StoreFile::open(&path, FileOptions::default()).unwrap();
    let before: Vec<(String, Option<String>)> = other
        .prepare("SELECT name, sql FROM sqlite_master ORDER BY name")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let result = Db::open_file_with_catalogue(
        config(schema(&["notes"])),
        file,
        jazz::catalogue::CatalogueChange {
            admit: vec![(schema(&["notes", "tasks"]), vec![])],
            ..Default::default()
        },
    );
    assert!(result.is_err());
    drop(result);
    let after: Vec<(String, Option<String>)> = other
        .prepare("SELECT name, sql FROM sqlite_master ORDER BY name")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        after, before,
        "failed deployment must leave no initialized tables"
    );
    assert_eq!(
        other
            .query_row("SELECT value FROM tasks", [], |r| r.get::<_, String>(0))
            .unwrap(),
        "bob"
    );
    let db = Db::open_file_with_catalogue(
        config(schema(&["messages"])),
        jazz::host::StoreFile::open(&path, FileOptions::default()).unwrap(),
        Default::default(),
    )
    .unwrap();
    insert(&db, "messages", "alice");
    drop(db);
    assert_eq!(
        bodies(&open(&path, schema(&["messages"])), "messages"),
        ["alice"]
    );
}
