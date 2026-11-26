//! A store opened from the bytes `Db::image` took of another one behaves as
//! that store reopened: every version the image admitted stays admitted, so
//! it reads and writes as a fresh open under the same schema and migrations
//! does, without replaying the genesis DDL and the migration ALTERs. Bytes
//! that are not such an image are refused, not opened.

use crate::common;

use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol::{LensOp, MigrationLens, TableLens};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

/// `notes(title)`: the genesis version.
fn v1() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("title", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

/// v1 plus `notes.rank: bigint?`.
fn v2() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("title", ColumnType::Text)
                    .nullable_column("rank", ColumnType::BigInt)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn v1_to_v2() -> MigrationLens {
    MigrationLens::new(
        v1().version_id(),
        v2().version_id(),
        vec![TableLens {
            source_table: "notes".into(),
            target_table: "notes".into(),
            ops: vec![LensOp::AddColumn {
                column: "rank".into(),
                default: Value::Nullable(None),
            }],
        }],
    )
    .unwrap()
}

/// A client on v2 that ships the migration from v1, as the React Native
/// relay's foreground store is configured.
fn config() -> DbConfig {
    DbConfig::new(
        v2(),
        DbIdentity {
            node: NodeUuid::from_bytes([3; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
    .with_migrations(vec![v1()], vec![v1_to_v2()])
}

fn insert_note(db: &Db, n: u8, rank: Option<i64>) {
    block_on(db.insert(
        "notes",
        BTreeMap::from([
            ("title".to_owned(), Value::String(format!("n{n}"))),
            (
                "rank".to_owned(),
                Value::Nullable(rank.map(|rank| Box::new(Value::I64(rank)))),
            ),
        ]),
        InsertOptions {
            row_id: Some(RowUuid::from_bytes([n; 16])),
            ..Default::default()
        },
    ))
    .unwrap();
}

fn notes(db: &Db) -> Vec<(Option<Value>, Option<Value>)> {
    let prepared = db.prepare_query(&Query::from("notes")).unwrap();
    let mut rows: Vec<_> = block_on(db.all(&prepared, ReadOpts::default()))
        .unwrap()
        .iter()
        .map(|row| {
            let cell = |column: &str| {
                row.cells()
                    .find(|(name, _)| *name == column)
                    .and_then(|(_, value)| value.cloned())
            };
            (row.row_uuid(), (cell("title"), cell("rank")))
        })
        .collect();
    rows.sort_by_key(|(id, _)| *id);
    rows.into_iter().map(|(_, cells)| cells).collect()
}

#[test]
fn a_store_opened_from_an_image_reads_and_writes_as_a_fresh_open() {
    let fresh = block_on(Db::open(config())).unwrap();
    let image = fresh.image().unwrap();
    let reopened = block_on(Db::open_image(config(), &image)).unwrap();

    for db in [&fresh, &reopened] {
        insert_note(db, 1, Some(10));
        insert_note(db, 2, None);
    }
    assert_eq!(notes(&reopened), notes(&fresh));
    assert_eq!(notes(&reopened).len(), 2);
}

#[test]
fn an_image_keeps_the_rows_written_before_it_was_taken() {
    let source = block_on(Db::open(config())).unwrap();
    insert_note(&source, 7, Some(70));
    let reopened = block_on(Db::open_image(config(), &source.image().unwrap())).unwrap();
    assert_eq!(notes(&reopened), notes(&source));
}

#[test]
fn bytes_that_are_not_a_store_image_are_refused() {
    let image = block_on(Db::open(config())).unwrap().image().unwrap();
    let truncated = &image[..image.len() / 2];
    let garbage = vec![0x5a; image.len()];
    for bytes in [truncated, &garbage[..], &[][..]] {
        assert!(block_on(Db::open_image(config(), bytes)).is_err());
    }
}
