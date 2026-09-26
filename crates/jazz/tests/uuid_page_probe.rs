//! One-shot Global UUID pages remain exact after authorization and deletion.
//! The fixture uses public schema, mutation, and read APIs; expected IDs are
//! independent of the query engine's complete-source plan.

use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, block_on};
use jazz::groove::{records::Value, storage::TestStorage};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

mod common;

use common::{compile_schema, read_and_allow_all_writes, session_eq};

fn row(seed: u8) -> RowUuid {
    RowUuid::from_bytes([seed; 16])
}

fn reader() -> AuthorSubject {
    AuthorSubject::for_test_bytes([0x71; 16])
}

fn other_reader() -> AuthorSubject {
    AuthorSubject::for_test_bytes([0x72; 16])
}

fn open_db() -> Db<TestStorage> {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("documents")
                    .column("owner", ColumnType::Text)
                    .policies(read_and_allow_all_writes(session_eq(
                        "owner",
                        &["user", "identity", "subject"],
                    ))),
            )
            .build(),
    );
    let families = schema.column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    block_on(Db::open(DbConfig::new(
        schema,
        TestStorage::new(&refs),
        DbIdentity {
            node: NodeUuid::from_bytes([0x73; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .expect("open authority")
}

fn insert(db: &Db<TestStorage>, id: u8, owner: AuthorSubject) {
    let input = jazz::row_input!("owner" => owner.principal_parts().1);
    let cells = input
        .into_iter()
        .map(|(name, value)| match value {
            jazz::tools::Value::Text(value) => (name, Value::String(value)),
            other => panic!("unexpected fixture value: {other:?}"),
        })
        .collect::<BTreeMap<_, _>>();
    let written = block_on(db.insert(
        "documents",
        cells,
        InsertOptions {
            row_id: Some(row(id)),
            ..Default::default()
        },
    ))
    .expect("insert document");
    db.finalize_local_mergeable_commit_for_test(written.mergeable_tx_id())
        .expect("settle document on authority");
}

fn page(db: &Db<TestStorage>, identity: AuthorSubject, limit: usize) -> Vec<RowUuid> {
    let prepared = db
        .prepare_query(&Query::from("documents").limit(limit))
        .expect("prepare UUID page");
    block_on(db.all_for_identity(
        &prepared,
        ReadOpts {
            tier: DurabilityTier::Global,
            ..ReadOpts::default()
        },
        identity,
    ))
    .expect("read UUID page")
    .into_iter()
    .map(|row| row.row_uuid())
    .collect()
}

#[test]
fn global_uuid_page_skips_hidden_and_deleted_prefix_rows() {
    let db = open_db();
    for id in 1..=20 {
        insert(
            &db,
            id,
            if id % 2 == 0 {
                reader()
            } else {
                other_reader()
            },
        );
    }
    assert_eq!(page(&db, reader(), 3), [row(2), row(4), row(6)]);
    assert_eq!(page(&db, other_reader(), 3), [row(1), row(3), row(5)]);

    let deleted = block_on(db.delete("documents", row(2), Default::default()))
        .expect("delete first visible document");
    db.finalize_local_mergeable_commit_for_test(deleted.mergeable_tx_id())
        .expect("settle deletion on authority");
    assert_eq!(page(&db, reader(), 3), [row(4), row(6), row(8)]);
    assert_eq!(page(&db, reader(), 20).len(), 9);
}

#[test]
fn sparse_visibility_falls_back_to_complete_global_source() {
    let db = open_db();
    for id in 1..=70 {
        insert(&db, id, other_reader());
    }
    insert(&db, 80, reader());
    insert(&db, 81, reader());
    assert_eq!(page(&db, reader(), 2), [row(80), row(81)]);
    assert!(page(&db, reader(), 1).contains(&row(80)));
}
