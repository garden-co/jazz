//! A write staged in an open transaction at the authority (how a thin
//! client and a backend write) is checked against the rows the same
//! transaction staged before it: an INSERT policy's `exists` sees them.

use crate::common;

use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, WriteIdentity, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    ColumnType, OpenTransactionId, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder,
};

use common::{compile_schema, exists, outer_eq};

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("boards")
                    .column("name", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::True),
                    ),
            )
            .table(
                TableSchemaBuilder::new("members")
                    .fk_column("board_id", "boards")
                    .column("role", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(exists("boards", vec![outer_eq("id", "board_id")])),
                    ),
            )
            .build(),
    )
}

fn session(author: AuthorSubject) -> InsertOptions {
    InsertOptions {
        identity: WriteIdentity::Session(author),
        ..Default::default()
    }
}

fn text(s: &str) -> Value {
    Value::String(s.to_owned())
}

fn open() -> Db {
    block_on(Db::open_history_complete(DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([0; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap()
}

fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([0x61; 16])
}

fn member_of(board: RowUuid) -> BTreeMap<String, Value> {
    BTreeMap::from([
        (
            "board_id".to_owned(),
            Value::Uuid(uuid::Uuid::from_bytes(*board.as_bytes())),
        ),
        ("role".to_owned(), text("owner")),
    ])
}

fn stage_board(db: &Db, open: OpenTransactionId) -> RowUuid {
    block_on(db.tx_ref(open).insert(
        "boards",
        BTreeMap::from([("name".to_owned(), text("Retro"))]),
        session(alice()),
    ))
    .unwrap()
    .row_uuid()
}

#[test]
fn a_staged_insert_without_its_board_is_still_refused() {
    let db = open();
    let open = OpenTransactionId::new();
    block_on(db.begin_mergeable(open)).unwrap();
    stage_board(&db, open);
    let error = block_on(db.tx_ref(open).insert(
        "members",
        member_of(RowUuid::from_bytes([9; 16])),
        session(alice()),
    ))
    .err()
    .expect("no such board");
    assert!(
        error.message.contains("INSERT policy denied"),
        "{}",
        error.message
    );
}

#[test]
fn a_commit_whose_later_write_removes_the_evidence_is_refused() {
    let db = open();
    let open = OpenTransactionId::new();
    block_on(db.begin_mergeable(open)).unwrap();
    let board = stage_board(&db, open);
    block_on(
        db.tx_ref(open)
            .insert("members", member_of(board), session(alice())),
    )
    .unwrap();
    block_on(db.tx_ref(open).delete(
        "boards",
        board,
        jazz::db::DeleteOptions {
            identity: WriteIdentity::Database,
            ..Default::default()
        },
    ))
    .unwrap();
    let error = block_on(db.commit_mergeable_handle(open))
        .err()
        .expect("the member's board is gone at the end of the unit");
    assert!(
        error.message.contains("INSERT policy denied"),
        "{}",
        error.message
    );
}

#[test]
fn a_staged_insert_sees_the_rows_its_transaction_staged_before_it() {
    let version = schema().version_id();
    let db = open();
    let governed = db.governed_view(version).expect("admitted");
    for (label, view) in [("owner", &db), ("governed view", &governed)] {
        let open = OpenTransactionId::new();
        block_on(view.begin_mergeable(open)).unwrap();
        let board = stage_board(view, open);
        block_on(
            view.tx_ref(open)
                .insert("members", member_of(board), session(alice())),
        )
        .unwrap_or_else(|error| panic!("{label}: staging the member: {error}"));
        block_on(view.commit_mergeable_handle(open))
            .unwrap_or_else(|error| panic!("{label}: commit: {error}"));
    }
}
