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
            .table(
                // A node hangs under a parent that already exists.
                TableSchemaBuilder::new("nodes")
                    .fk_column("parent_id", "nodes")
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(exists("nodes", vec![outer_eq("id", "parent_id")]))
                            .with_update(
                                Some(PolicyExpr::True),
                                exists("nodes", vec![outer_eq("id", "parent_id")]),
                            ),
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
        error
            .message
            .contains("once the rest of its transaction applied"),
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

fn uuid(row: RowUuid) -> Value {
    Value::Uuid(uuid::Uuid::from_bytes(*row.as_bytes()))
}

/// A root, its own parent, as only the database may write it.
fn root_node(db: &Db) -> RowUuid {
    let root = RowUuid::from_bytes([1; 16]);
    block_on(db.insert(
        "nodes",
        BTreeMap::from([("parent_id".to_owned(), uuid(root))]),
        InsertOptions {
            row_id: Some(root),
            ..database()
        },
    ))
    .unwrap();
    root
}

fn database() -> InsertOptions {
    InsertOptions {
        identity: WriteIdentity::Database,
        ..Default::default()
    }
}

#[test]
fn a_row_the_database_staged_is_evidence_at_staging_and_at_commit() {
    let db = open();
    let open = OpenTransactionId::new();
    block_on(db.begin_mergeable(open)).unwrap();
    let board = block_on(db.tx_ref(open).insert(
        "boards",
        BTreeMap::from([("name".to_owned(), text("Retro"))]),
        database(),
    ))
    .unwrap()
    .row_uuid();
    block_on(
        db.tx_ref(open)
            .insert("members", member_of(board), session(alice())),
    )
    .unwrap();
    block_on(db.commit_mergeable_handle(open)).expect("both checks see the board");
}

#[test]
fn a_row_staged_and_deleted_is_no_evidence() {
    let db = open();
    let open = OpenTransactionId::new();
    block_on(db.begin_mergeable(open)).unwrap();
    let board = stage_board(&db, open);
    block_on(db.tx_ref(open).delete(
        "boards",
        board,
        jazz::db::DeleteOptions {
            identity: WriteIdentity::Database,
            ..Default::default()
        },
    ))
    .unwrap();
    let error = block_on(
        db.tx_ref(open)
            .insert("members", member_of(board), session(alice())),
    )
    .err()
    .expect("the board is gone");
    assert!(
        error.message.contains("INSERT policy denied"),
        "{}",
        error.message
    );
}

#[test]
fn several_writes_to_the_evidence_row_keep_it_evidence() {
    let db = open();
    let open = OpenTransactionId::new();
    block_on(db.begin_mergeable(open)).unwrap();
    let board = stage_board(&db, open);
    block_on(db.tx_ref(open).update(
        "boards",
        board,
        BTreeMap::from([("name".to_owned(), text("Retro 2"))]),
        jazz::db::UpdateOptions {
            identity: WriteIdentity::Database,
            ..Default::default()
        },
    ))
    .unwrap();
    block_on(
        db.tx_ref(open)
            .insert("members", member_of(board), session(alice())),
    )
    .unwrap();
    block_on(db.commit_mergeable_handle(open)).unwrap();
}

/// A row cannot vouch for itself: a node whose only parent is itself is
/// refused, whether it is inserted that way or updated to it.
#[test]
fn a_staged_row_is_no_evidence_for_its_own_check() {
    let db = open();
    let root = root_node(&db);

    let open = OpenTransactionId::new();
    block_on(db.begin_mergeable(open)).unwrap();
    let selfish = RowUuid::from_bytes([7; 16]);
    let error = block_on(db.tx_ref(open).insert(
        "nodes",
        BTreeMap::from([("parent_id".to_owned(), uuid(selfish))]),
        InsertOptions {
            row_id: Some(selfish),
            ..session(alice())
        },
    ))
    .err()
    .expect("its own parent");
    assert!(
        error.message.contains("INSERT policy denied"),
        "{}",
        error.message
    );

    let node = block_on(db.tx_ref(open).insert(
        "nodes",
        BTreeMap::from([("parent_id".to_owned(), uuid(root))]),
        session(alice()),
    ))
    .unwrap()
    .row_uuid();
    let error = block_on(db.tx_ref(open).update(
        "nodes",
        node,
        BTreeMap::from([("parent_id".to_owned(), uuid(node))]),
        jazz::db::UpdateOptions {
            identity: WriteIdentity::Session(alice()),
            ..Default::default()
        },
    ))
    .err()
    .expect("its own parent, after an earlier write to it");
    assert!(error.message.contains("WITH CHECK"), "{}", error.message);
}

/// A chain staged link by link commits in one pass over the unit, whatever
/// its depth.
#[test]
fn a_deep_chain_staged_in_order_commits() {
    let db = open();
    let mut parent = root_node(&db);
    let open = OpenTransactionId::new();
    block_on(db.begin_mergeable(open)).unwrap();
    for _ in 0..150 {
        parent = block_on(db.tx_ref(open).insert(
            "nodes",
            BTreeMap::from([("parent_id".to_owned(), uuid(parent))]),
            session(alice()),
        ))
        .unwrap()
        .row_uuid();
    }
    block_on(db.commit_mergeable_handle(open)).unwrap();
}
