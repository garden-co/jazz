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

/// Alice shares SQLite with another application. Jazz initializes, admits a
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
        block_on(db.activate_schema(new.clone())).unwrap();
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
    let error = block_on(db.activate_schema(schema(&["notes", "tasks"]))).unwrap_err();
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
    let result = block_on(Db::open_file_with_catalogue(
        config(schema(&["notes"])),
        file,
        jazz::catalogue::CatalogueChange {
            admit: vec![(schema(&["notes", "tasks"]), vec![])],
            ..Default::default()
        },
    ));
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
    let db = block_on(Db::open_file_with_catalogue(
        config(schema(&["messages"])),
        jazz::host::StoreFile::open(&path, FileOptions::default()).unwrap(),
        Default::default(),
    ))
    .unwrap_or_else(|(error, _)| panic!("{error}"));
    insert(&db, "messages", "alice");
    drop(db);
    assert_eq!(
        bodies(&open(&path, schema(&["messages"])), "messages"),
        ["alice"]
    );
}
