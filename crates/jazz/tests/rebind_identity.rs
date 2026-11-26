//! `Db::rebind_identity` hands a store opened ahead of time (the React Native
//! relay's spare foreground) to the identity that turns up, but only while
//! nothing in it carries the identity it was opened under.

use crate::common;

use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol::{LensOp, MigrationLens, TableLens};
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

#[test]
fn a_store_keeps_its_identity_once_it_was_written() {
    let other = DbIdentity {
        node: NodeUuid::from_bytes([4; 16]),
        author: AuthorSubject::SYSTEM,
    };
    let mut untouched = block_on(Db::open(config())).unwrap();
    assert!(untouched.rebind_identity(other));
    let mut written = block_on(Db::open(config())).unwrap();
    insert_note(&written, 1, None);
    assert!(!written.rebind_identity(other));
}
