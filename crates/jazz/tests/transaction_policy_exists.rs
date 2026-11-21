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
            .table(
                // An item's `kind` is fixed once written: an update must keep
                // the kind its committed version has.
                TableSchemaBuilder::new("items")
                    .column("kind", ColumnType::Text)
                    .column("title", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::True)
                            .with_update(
                                Some(PolicyExpr::True),
                                exists(
                                    "items",
                                    vec![outer_eq("id", "id"), outer_eq("kind", "kind")],
                                ),
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
    .expect_err("no such board");
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
        .expect_err("the member's board is gone at the end of the unit");
    assert!(
        error
            .message
            .contains("on the state the whole transaction leaves behind"),
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
    .expect_err("the board is gone");
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
    .expect_err("its own parent");
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
    .expect_err("its own parent, after an earlier write to it");
    // The insert decides a row the transaction wrote first, as on commit.
    assert!(
        error.message.contains("INSERT policy denied"),
        "{}",
        error.message
    );
}

/// A chain staged link by link commits: the commit grounds it from the
/// committed root outwards, as the authority does on upload.
#[test]
fn a_deep_chain_staged_in_order_commits() {
    let db = open();
    let mut parent = root_node(&db);
    let open = OpenTransactionId::new();
    block_on(db.begin_mergeable(open)).unwrap();
    for _ in 0..100 {
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

/// Rows can't ground each other: a cycle each link of which passed as it
/// was staged is refused at commit, as the authority would refuse it.
#[test]
fn a_cycle_of_staged_rows_is_refused_at_commit() {
    let db = open();
    let root = root_node(&db);
    let open = OpenTransactionId::new();
    block_on(db.begin_mergeable(open)).unwrap();
    let insert = |parent: RowUuid| {
        block_on(db.tx_ref(open).insert(
            "nodes",
            BTreeMap::from([("parent_id".to_owned(), uuid(parent))]),
            session(alice()),
        ))
        .unwrap()
        .row_uuid()
    };
    let x = insert(root);
    let y = insert(x);
    block_on(db.tx_ref(open).update(
        "nodes",
        x,
        BTreeMap::from([("parent_id".to_owned(), uuid(y))]),
        jazz::db::UpdateOptions {
            identity: WriteIdentity::Session(alice()),
            ..Default::default()
        },
    ))
    .expect("y is staged, so x's new parent passes as it is staged");
    let error =
        block_on(db.commit_mergeable_handle(open)).expect_err("x and y only ground each other");
    assert!(error.is_authorization_denied(), "{}", error.message);
    assert!(
        error
            .message
            .contains("on the state the whole transaction leaves behind"),
        "{}",
        error.message
    );
}

fn update_as(author: AuthorSubject) -> jazz::db::UpdateOptions {
    jazz::db::UpdateOptions {
        identity: WriteIdentity::Session(author),
        ..Default::default()
    }
}

fn stage_item(db: &Db, open: OpenTransactionId) -> RowUuid {
    block_on(db.tx_ref(open).insert(
        "items",
        BTreeMap::from([
            ("kind".to_owned(), text("a")),
            ("title".to_owned(), text("x")),
        ]),
        session(alice()),
    ))
    .unwrap()
    .row_uuid()
}

/// A row the transaction inserted has no committed version, so its insert
/// decides it (SPEC 7): alice's later staged updates to it are checked as
/// the commit and the authority check them, the row's end state against
/// its INSERT policy, not by their own WITH CHECK against a committed
/// version that doesn't exist yet.
///
/// ```text
/// alice ──insert item──► open tx ──update item──► open tx ──commit──► ✓
///                                     │
///                                     └── WITH CHECK would read the
///                                         committed item: there is none
/// ```
#[test]
fn a_staged_update_to_a_row_staged_earlier_is_decided_by_its_insert() {
    let db = open();
    let open = OpenTransactionId::new();
    block_on(db.begin_mergeable(open)).unwrap();
    let item = stage_item(&db, open);
    for title in ["y", "z"] {
        block_on(db.tx_ref(open).update(
            "items",
            item,
            BTreeMap::from([("title".to_owned(), text(title))]),
            update_as(alice()),
        ))
        .unwrap_or_else(|error| panic!("the insert decides the row: {error}"));
    }
    block_on(db.commit_mergeable_handle(open)).expect("the commit decides it the same way");
}

/// The insert's check still runs, on the row's end state: alice's staged
/// update that leaves her node under a parent that doesn't exist is
/// refused at the call, and the transaction still commits without it.
#[test]
fn a_staged_update_to_a_row_staged_earlier_is_checked_by_its_insert_policy() {
    let db = open();
    let root = root_node(&db);
    let open = OpenTransactionId::new();
    block_on(db.begin_mergeable(open)).unwrap();
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
        BTreeMap::from([("parent_id".to_owned(), uuid(RowUuid::from_bytes([9; 16])))]),
        update_as(alice()),
    ))
    .expect_err("no such parent");
    assert!(error.is_authorization_denied(), "{}", error.message);
    assert!(
        error.message.contains("INSERT policy denied"),
        "{}",
        error.message
    );
    block_on(db.commit_mergeable_handle(open)).expect("the refused update was not staged");
}

/// A committed row keeps its own WITH CHECK: alice's updates staged to it
/// are checked against its committed version, so changing its kind is
/// refused.
#[test]
fn a_staged_update_to_a_committed_row_keeps_its_with_check() {
    let db = open();
    let setup = OpenTransactionId::new();
    block_on(db.begin_mergeable(setup)).unwrap();
    let item = stage_item(&db, setup);
    block_on(db.commit_mergeable_handle(setup)).unwrap();

    let open = OpenTransactionId::new();
    block_on(db.begin_mergeable(open)).unwrap();
    block_on(db.tx_ref(open).update(
        "items",
        item,
        BTreeMap::from([("title".to_owned(), text("y"))]),
        update_as(alice()),
    ))
    .expect("the kind is kept");
    let error = block_on(db.tx_ref(open).update(
        "items",
        item,
        BTreeMap::from([("kind".to_owned(), text("b"))]),
        update_as(alice()),
    ))
    .expect_err("the kind changed");
    assert!(error.message.contains("WITH CHECK"), "{}", error.message);
}
