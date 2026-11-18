//! Installing a newer schema when a SQLite store opens: the stored tables
//! grow (new tables, columns, indexes) and nothing already stored changes.
//! Changes SQLite cannot make in place fail with "not supported yet", and
//! the store keeps opening under its old schema. A running store never
//! switches schema: a deployment installs its schema at startup.

use crate::common;

use std::collections::BTreeMap;
use std::path::PathBuf;

use jazz::db::{Db, DbConfig, DbIdentity, FileOptions, InsertOptions, ReadOpts, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

fn items(extra: impl FnOnce(TableSchemaBuilder) -> TableSchemaBuilder) -> TableSchemaBuilder {
    extra(TableSchemaBuilder::new("items").column("label", ColumnType::Text))
        .policies(allow_all_policies())
}

fn v1() -> JazzSchema {
    compile_schema(&SchemaBuilder::new().table(items(|t| t)).build())
}

/// v1 plus a nullable column, a defaulted column, an index and a new table.
fn v2() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(items(|t| {
                t.nullable_column("note", ColumnType::Text)
                    .column_with_default("rank", ColumnType::BigInt, jazz::tools::Value::BigInt(7))
                    .index_only(["rank"])
            }))
            .table(
                TableSchemaBuilder::new("tags")
                    .column("name", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
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

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("jazz-activate-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("store.sqlite")
}

fn row(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}

fn insert_v1(db: &Db, n: u8) {
    block_on(db.insert(
        "items",
        BTreeMap::from([("label".to_owned(), Value::String(format!("i{n}")))]),
        InsertOptions {
            row_id: Some(row(n)),
            ..Default::default()
        },
    ))
    .unwrap();
}

fn cell(db: &Db, table: &str, column: &str) -> Vec<Option<Value>> {
    let prepared = db.prepare_query(&Query::from(table)).unwrap();
    block_on(db.all(&prepared, ReadOpts::default()))
        .unwrap()
        .iter()
        .map(|row| {
            row.cells()
                .find(|(name, _)| *name == column)
                .and_then(|(_, value)| value.cloned())
        })
        .collect()
}

fn indexes(path: &PathBuf) -> Vec<String> {
    let conn = rusqlite::Connection::open(path).unwrap();
    let mut statement = conn
        .prepare("SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'items' ORDER BY name")
        .unwrap();
    statement
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

#[test]
fn opening_under_a_newer_schema_grows_the_store() {
    let path = scratch("grow");
    {
        let db = block_on(Db::open_at(config(v1()), &path, FileOptions::default())).unwrap();
        insert_v1(&db, 1);
    }
    let db = block_on(Db::open_at(config(v2()), &path, FileOptions::default())).unwrap();
    assert_eq!(
        cell(&db, "items", "label"),
        vec![Some(Value::String("i1".into()))]
    );
    assert_eq!(
        cell(&db, "items", "rank"),
        vec![Some(Value::I64(7))],
        "old rows read the default"
    );
    assert_eq!(
        cell(&db, "items", "note"),
        vec![Some(Value::Nullable(None))]
    );
    block_on(db.insert(
        "tags",
        BTreeMap::from([("name".to_owned(), Value::String("t".into()))]),
        InsertOptions::default(),
    ))
    .unwrap();
    assert_eq!(
        cell(&db, "tags", "name"),
        vec![Some(Value::String("t".into()))]
    );
    drop(db);
    // Opening under the same schema again changes nothing.
    let db = block_on(Db::open_at(config(v2()), &path, FileOptions::default())).unwrap();
    assert_eq!(cell(&db, "items", "label").len(), 1);
    assert_eq!(cell(&db, "tags", "name").len(), 1);
}

#[test]
fn reopening_with_a_newer_schema_extends_the_file_once() {
    let path = scratch("reopen");
    {
        let db = block_on(Db::open_at(config(v1()), &path, FileOptions::default())).unwrap();
        insert_v1(&db, 1);
    }
    {
        let db = block_on(Db::open_at(config(v2()), &path, FileOptions::default())).unwrap();
        assert_eq!(cell(&db, "items", "rank"), vec![Some(Value::I64(7))]);
        block_on(db.insert(
            "tags",
            BTreeMap::from([("name".to_owned(), Value::String("t".into()))]),
            InsertOptions::default(),
        ))
        .unwrap();
    }
    let after_first = indexes(&path);
    // Reopening with the old schema keeps the newer columns and tables; the
    // newer schema then sees everything it wrote.
    {
        let db = block_on(Db::open_at(config(v1()), &path, FileOptions::default())).unwrap();
        insert_v1(&db, 2);
        assert_eq!(cell(&db, "items", "label").len(), 2);
    }
    let db = block_on(Db::open_at(config(v2()), &path, FileOptions::default())).unwrap();
    assert_eq!(
        cell(&db, "tags", "name"),
        vec![Some(Value::String("t".into()))]
    );
    assert_eq!(
        cell(&db, "items", "rank"),
        vec![Some(Value::I64(7)), Some(Value::I64(7))]
    );
    assert_eq!(indexes(&path), after_first, "no index is created twice");
}

#[test]
fn changes_sqlite_cannot_make_in_place_are_not_supported_yet() {
    let path = scratch("unsupported");
    {
        let db = block_on(Db::open_at(config(v1()), &path, FileOptions::default())).unwrap();
        insert_v1(&db, 1);
    }
    let open = |schema| block_on(Db::open_at(config(schema), &path, FileOptions::default()));
    let retyped = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("items")
                    .column("label", ColumnType::BigInt)
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    let error = open(retyped).err().unwrap();
    assert!(error.to_string().contains("not supported"), "{error}");

    let required = compile_schema(
        &SchemaBuilder::new()
            .table(items(|t| t.column("must", ColumnType::Text)))
            .build(),
    );
    // Nothing was installed: the store still opens under v1 with its row.
    let db = open(v1()).unwrap();
    assert_eq!(cell(&db, "items", "label").len(), 1);
    // A deploy admits a new version before any store opens under it, and
    // admission refuses a required column old rows cannot fill.
    let error = block_on(db.admit_schema_with_lenses(required, &[])).unwrap_err();
    assert!(error.to_string().contains("not supported"), "{error}");
}
