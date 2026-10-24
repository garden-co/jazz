//! History relations on SQLite: `history.<table>` reads list a row's
//! accepted content versions, current and superseded, with `$transactionId`
//! and `$acceptedAt`. Access follows the current row's read policy, deleted
//! rows hide their history unless deleted rows are included, branches
//! select one branch's versions, and schema views read old versions through
//! their lens. Ported from #4166's Groove history relations.

mod common;

use std::collections::BTreeMap;

use jazz::db::{
    CurrentRow, Db, DbConfig, DbIdentity, ReadOpts, ReadTier, Transport, block_on, duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol::{BranchSelector, SnapshotRef};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    CmpOp, ColumnType, PolicyExpr, PolicyValue, SchemaBuilder, TablePolicies, TableSchemaBuilder,
    Value as PublicValue,
};

use common::{allow_all_policies, compile_schema};

const NODE: [u8; 16] = [0x11; 16];

fn notes_schema() -> JazzSchema {
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

fn open(schema: JazzSchema, node: [u8; 16], author: AuthorSubject) -> Db {
    block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes(node),
            author,
        },
    )))
    .unwrap()
}

fn row(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}

fn title(text: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("title".to_owned(), Value::String(text.to_owned()))])
}

fn insert(db: &Db, id: RowUuid, text: &str) -> u64 {
    block_on(db.insert(
        "notes",
        title(text),
        jazz::db::InsertOptions {
            row_id: Some(id),
            ..Default::default()
        },
    ))
    .unwrap()
    .mergeable_tx_id()
}

fn update(db: &Db, id: RowUuid, text: &str) -> u64 {
    block_on(db.update("notes", id, title(text), Default::default()))
        .unwrap()
        .mergeable_tx_id()
}

fn history_of(id: RowUuid) -> Query {
    Query::from("history.notes")
        .filter(eq(col("id"), lit(Value::Uuid(id.0))))
        .order_by("$acceptedAt", OrderDirection::Asc)
}

fn read(db: &Db, query: &Query, opts: ReadOpts) -> Vec<CurrentRow> {
    block_on(db.all(&db.prepare_query(query).unwrap(), opts)).unwrap()
}

fn cell(row: &CurrentRow, name: &str) -> Option<Value> {
    row.cells()
        .find(|(cell, _)| *cell == name)
        .and_then(|(_, value)| value.cloned())
}

fn titles(rows: &[CurrentRow]) -> Vec<String> {
    rows.iter()
        .map(|row| match cell(row, "title") {
            Some(Value::String(text)) => text,
            other => panic!("title {other:?}"),
        })
        .collect()
}

fn accepted_at(row: &CurrentRow) -> u64 {
    match cell(row, "$acceptedAt") {
        Some(Value::U64(at)) => at,
        other => panic!("$acceptedAt {other:?}"),
    }
}

fn transaction_id(row: &CurrentRow) -> (u64, [u8; 16]) {
    match cell(row, "$transactionId") {
        Some(Value::Tuple(parts)) => match parts.as_slice() {
            [Value::U64(time), Value::Uuid(node)] => (*time, *node.as_bytes()),
            other => panic!("$transactionId {other:?}"),
        },
        other => panic!("$transactionId {other:?}"),
    }
}

fn error_of(db: &Db, query: &Query, opts: ReadOpts) -> String {
    let error = match db.prepare_query(query) {
        Err(error) => error,
        Ok(prepared) => block_on(db.all(&prepared, opts)).expect_err("the read must be refused"),
    };
    error.message
}

/// Alice writes a note three times. Its history lists all three versions,
/// the current one included, each with the transaction that wrote it and
/// an acceptance time that orders them; every version keeps the row's id.
#[test]
fn history_lists_every_accepted_version_with_its_transaction() {
    let db = open(notes_schema(), NODE, AuthorSubject::SYSTEM);
    let note = row(1);
    let txs = [
        insert(&db, note, "first"),
        update(&db, note, "second"),
        update(&db, note, "third"),
    ];
    insert(&db, row(2), "other row");

    let versions = read(&db, &history_of(note), ReadOpts::default());
    assert_eq!(titles(&versions), ["first", "second", "third"]);
    assert!(versions.iter().all(|version| version.row_uuid() == note));
    assert_eq!(
        versions.iter().map(transaction_id).collect::<Vec<_>>(),
        txs.map(|tx| (tx, NODE)).to_vec(),
        "the authority's own commits are named by their seq and its node"
    );
    let times: Vec<u64> = versions.iter().map(accepted_at).collect();
    assert!(times.windows(2).all(|pair| pair[0] < pair[1]), "{times:?}");
    assert!(
        times
            .iter()
            .all(|at| at & ((1 << 18) - 1) == 0 && at >> 18 > 0),
        "acceptance time is an HLC: milliseconds over a zero counter"
    );

    let newest_first = read(
        &db,
        &Query::from("history.notes")
            .filter(eq(col("id"), lit(Value::Uuid(note.0))))
            .order_by("$acceptedAt", OrderDirection::Desc)
            .limit(2),
        ReadOpts::default(),
    );
    assert_eq!(titles(&newest_first), ["third", "second"]);
}

/// History reads take an id list, filter historical values and select
/// columns: filters run over versions, not over current rows.
#[test]
fn history_filters_versions_of_an_id_list() {
    let db = open(notes_schema(), NODE, AuthorSubject::SYSTEM);
    insert(&db, row(1), "draft");
    update(&db, row(1), "final");
    insert(&db, row(2), "draft");
    insert(&db, row(3), "draft");

    let drafts = read(
        &db,
        &Query::from("history.notes")
            .filter(jazz::query::Predicate::In(
                jazz::query::Operand::Column("id".into()),
                [row(1), row(2)]
                    .map(|id| jazz::query::Operand::Literal(Value::Uuid(id.0)))
                    .to_vec(),
            ))
            .filter(eq(col("title"), lit(Value::String("draft".into()))))
            .select(["title", "$acceptedAt"]),
        ReadOpts::default(),
    );
    assert_eq!(
        drafts.iter().map(CurrentRow::row_uuid).collect::<Vec<_>>(),
        [row(1), row(2)],
        "row 1's superseded draft matches, row 3 is not in the id list"
    );
    assert_eq!(
        drafts[0].cells().map(|(name, _)| name).collect::<Vec<_>>(),
        ["title", "$acceptedAt"]
    );
}

/// Shapes history does not support are refused by name, never answered
/// partially: a read over all rows' histories, `$transactionId` filters and
/// ordering, relationships, aggregates, snapshot reads and branch bases.
#[test]
fn unsupported_history_shapes_are_explicit_errors() {
    let db = open(notes_schema(), NODE, AuthorSubject::SYSTEM);
    insert(&db, row(1), "first");
    let one = || Query::from("history.notes").filter(eq(col("id"), lit(Value::Uuid(row(1).0))));
    let cases = [
        (
            Query::from("history.notes"),
            "history without an id equality or IN constraint",
        ),
        (
            Query::from("history.notes").filter(eq(col("title"), lit(Value::String("a".into())))),
            "history without an id equality or IN constraint",
        ),
        (
            one().filter(eq(
                col("$transactionId"),
                lit(Value::Tuple(vec![
                    Value::U64(1),
                    Value::Uuid(uuid::Uuid::from_bytes(NODE)),
                ])),
            )),
            "$transactionId predicates",
        ),
        (
            one().order_by("$transactionId", OrderDirection::Asc),
            "ordering history by $transactionId",
        ),
        (
            one().include("missing"),
            "joins or relationships in history queries",
        ),
    ];
    for (query, expected) in cases {
        let message = error_of(&db, &query, ReadOpts::default());
        assert!(
            message.contains(expected) && message.contains("not supported yet"),
            "{query:?}: {message}"
        );
    }

    let snapshot = SnapshotRef {
        owner: NodeUuid::from_bytes(NODE),
        global_base: jazz::time::GlobalTime(1),
        local_base: jazz::time::TxTime(0),
        dots: Vec::new(),
    };
    let message = error_of(
        &db,
        &one(),
        ReadOpts {
            read_view: jazz::protocol::ReadViewSpec {
                source: jazz::protocol::ReadViewSourceSpec::Snapshot { snapshot },
                include_deleted: false,
            },
            ..Default::default()
        },
    );
    assert!(message.contains("history reads at a snapshot"), "{message}");
}

/// Deleting a note hides its history; including deleted rows lists its
/// content versions, never the deletion itself. A restore without content
/// brings the history back without a new version; a restore with content
/// adds one.
#[test]
fn deletion_hides_history_and_restores_reveal_it() {
    let db = open(notes_schema(), NODE, AuthorSubject::SYSTEM);
    let note = row(1);
    insert(&db, note, "first");
    update(&db, note, "second");
    block_on(db.delete("notes", note, Default::default())).unwrap();

    let deleted = ReadOpts {
        read_view: jazz::protocol::ReadViewSpec {
            include_deleted: true,
            ..Default::default()
        },
        ..Default::default()
    };
    assert!(read(&db, &history_of(note), ReadOpts::default()).is_empty());
    assert_eq!(
        titles(&read(&db, &history_of(note), deleted.clone())),
        ["first", "second"]
    );

    block_on(db.restore("notes", note, None, Default::default())).unwrap();
    assert_eq!(
        titles(&read(&db, &history_of(note), ReadOpts::default())),
        ["first", "second"],
        "a restore without content is not a version"
    );

    block_on(db.delete("notes", note, Default::default())).unwrap();
    block_on(db.restore("notes", note, Some(title("restored")), Default::default())).unwrap();
    assert_eq!(
        titles(&read(&db, &history_of(note), ReadOpts::default())),
        ["first", "second", "restored"]
    );
}

/// Access to history follows the current row: Bob reads every version of a
/// note he owns now, including versions from before it was his, and none
/// of a note he no longer owns, including versions from when it was.
#[test]
fn the_current_rows_read_policy_gates_its_history() {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("owner", ColumnType::Text)
                    .column("title", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::Cmp {
                                column: "owner".to_owned(),
                                op: CmpOp::Eq,
                                value: PolicyValue::SessionRef(vec![
                                    "user".to_owned(),
                                    "identity".to_owned(),
                                    "subject".to_owned(),
                                ]),
                            })
                            .with_insert(PolicyExpr::True)
                            .with_update(Some(PolicyExpr::True), PolicyExpr::True),
                    ),
            )
            .build(),
    );
    let db = open(schema, NODE, AuthorSubject::SYSTEM);
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb0; 16]);
    let owned = |author: AuthorSubject, text: &str| {
        BTreeMap::from([
            (
                "owner".to_owned(),
                Value::String(author.principal_parts().1),
            ),
            ("title".to_owned(), Value::String(text.to_owned())),
        ])
    };
    let given = row(1);
    let taken = row(2);
    for (id, from, to) in [(given, alice, bob), (taken, bob, alice)] {
        block_on(db.insert(
            "notes",
            owned(from, "before"),
            jazz::db::InsertOptions {
                row_id: Some(id),
                ..Default::default()
            },
        ))
        .unwrap();
        block_on(db.update("notes", id, owned(to, "after"), Default::default())).unwrap();
    }
    let history = |id: RowUuid, who: AuthorSubject| {
        let prepared = db.prepare_query(&history_of(id)).unwrap();
        titles(&block_on(db.all_for_identity(&prepared, ReadOpts::default(), who)).unwrap())
    };
    assert_eq!(history(given, bob), ["before", "after"]);
    assert!(history(taken, bob).is_empty());
    assert_eq!(history(taken, alice), ["before", "after"]);
}

/// History reads select one branch with a branch view and refuse a base;
/// without a view they read every branch's versions.
#[test]
fn branch_history_reads_one_branch_and_refuses_a_base() {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("branch_id", ColumnType::Uuid)
                    .column("title", ColumnType::Text)
                    .branch_by("branch_id")
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    let db = open(schema, NODE, AuthorSubject::SYSTEM);
    let branch =
        |n: u8| BranchSelector::new([("branch_id", Value::Uuid(uuid::Uuid::from_bytes([n; 16])))]);
    let note = row(1);
    for (n, text) in [(1, "main"), (2, "draft")] {
        block_on(db.insert(
            "notes",
            title(text),
            jazz::db::InsertOptions {
                row_id: Some(note),
                target: jazz::db::ExactWriteTarget::Branch(branch(n)),
                ..Default::default()
            },
        ))
        .unwrap();
    }
    block_on(db.update(
        "notes",
        note,
        title("draft 2"),
        jazz::db::UpdateOptions {
            target: jazz::db::WriteTarget::BranchView {
                head: branch(2),
                base: None,
            },
            ..Default::default()
        },
    ))
    .unwrap();

    let in_branch = |n: u8| ReadOpts::default().branch_view(branch(n), None);
    assert_eq!(
        titles(&read(&db, &history_of(note), in_branch(1))),
        ["main"]
    );
    assert_eq!(
        titles(&read(&db, &history_of(note), in_branch(2))),
        ["draft", "draft 2"]
    );
    assert_eq!(
        titles(&read(&db, &history_of(note), ReadOpts::default())),
        ["main", "draft", "draft 2"]
    );
    let message = error_of(
        &db,
        &history_of(note),
        ReadOpts::default().branch_view(
            branch(2),
            Some(jazz::protocol::BranchViewBase::Current(branch(1))),
        ),
    );
    assert!(
        message.contains("history reads through a branch view with a base"),
        "{message}"
    );
}

/// A schema view reads history through its lens: a column it adds reads
/// its default on versions written before it existed, and a handle on the
/// older schema does not see the column at all.
#[test]
fn history_reads_through_schema_lenses() {
    let v2 = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("title", ColumnType::Text)
                    .column_with_default("tag", ColumnType::Text, PublicValue::Text("none".into()))
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    let db = open(notes_schema(), NODE, AuthorSubject::SYSTEM);
    let note = row(1);
    insert(&db, note, "first");
    let view = block_on(db.register_schema_view(v2)).unwrap();
    block_on(view.update(
        "notes",
        note,
        BTreeMap::from([
            ("title".to_owned(), Value::String("second".into())),
            ("tag".to_owned(), Value::String("urgent".into())),
        ]),
        Default::default(),
    ))
    .unwrap();

    let through_view = read(&view, &history_of(note), ReadOpts::default());
    assert_eq!(
        through_view
            .iter()
            .map(|version| cell(version, "tag"))
            .collect::<Vec<_>>(),
        [
            Some(Value::String("none".into())),
            Some(Value::String("urgent".into()))
        ]
    );
    let through_owner = read(&db, &history_of(note), ReadOpts::default());
    assert_eq!(titles(&through_owner), ["first", "second"]);
    assert!(
        through_owner
            .iter()
            .all(|version| cell(version, "tag").is_none())
    );
}

/// An uploaded commit's versions are named by the transaction the client
/// committed, at the authority and in the client's own replica. A replica
/// never lists its pending writes as history.
#[test]
fn uploaded_versions_keep_the_writers_transaction_id() {
    let server_node = [0x01; 16];
    let client_node = [0x02; 16];
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let server = open(notes_schema(), server_node, AuthorSubject::SYSTEM);
    let client = open(notes_schema(), client_node, alice);
    let (client_side, server_side): (Box<dyn Transport>, Box<dyn Transport>) = duplex();
    let _upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice);
    let pump = || {
        for _ in 0..4 {
            block_on(client.tick()).unwrap();
            block_on(server.tick()).unwrap();
        }
    };

    let note = row(1);
    let first = insert(&client, note, "first");
    let local = ReadOpts {
        tier: ReadTier::LocalOnly,
        ..Default::default()
    };
    assert!(
        read(&client, &history_of(note), local.clone()).is_empty(),
        "a pending write is not history"
    );
    pump();
    let second = update(&client, note, "second");
    pump();

    let at_server = read(&server, &history_of(note), ReadOpts::default());
    assert_eq!(titles(&at_server), ["first", "second"]);
    assert_eq!(
        at_server.iter().map(transaction_id).collect::<Vec<_>>(),
        [(first, client_node), (second, client_node)]
    );
    let at_client = read(&client, &history_of(note), local);
    assert_eq!(
        at_client.iter().map(transaction_id).collect::<Vec<_>>(),
        [(first, client_node), (second, client_node)],
        "the replica lists its own accepted commits"
    );
    assert_eq!(
        at_client.iter().map(accepted_at).collect::<Vec<_>>(),
        at_server.iter().map(accepted_at).collect::<Vec<_>>(),
        "acceptance times are the authority's"
    );
}

fn next_delta(
    subscription: &mut jazz::db::SubscriptionStream,
) -> (Vec<String>, Vec<String>, usize) {
    match subscription.try_next_event() {
        Some(jazz::db::SubscriptionEvent::Delta {
            added,
            updated,
            removed,
            ..
        }) => {
            let added: Vec<CurrentRow> = added.iter().map(|row| (**row).clone()).collect();
            let updated: Vec<CurrentRow> = updated.iter().map(|row| (**row).clone()).collect();
            (titles(&added), titles(&updated), removed.len())
        }
        other => panic!("expected a delta, got {other:?}"),
    }
}

/// A history subscription gains a version when one is accepted, keeps the
/// versions it has (they share the row's id, so a new version is never
/// mistaken for an update of an old one), and follows the current row:
/// deleting it removes every version, restoring brings them back.
#[test]
fn history_subscriptions_follow_accepted_versions() {
    let db = open(notes_schema(), NODE, AuthorSubject::SYSTEM);
    let note = row(1);
    insert(&db, note, "first");
    let prepared = db.prepare_query(&history_of(note)).unwrap();
    let mut subscription = block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap();
    assert_eq!(
        next_delta(&mut subscription),
        (vec!["first".into()], vec![], 0)
    );

    update(&db, note, "second");
    assert_eq!(
        next_delta(&mut subscription),
        (vec!["second".into()], vec![], 0)
    );
    insert(&db, row(2), "another row");
    assert!(
        subscription.try_next_event().is_none(),
        "another row's write does not touch this history"
    );

    block_on(db.delete("notes", note, Default::default())).unwrap();
    assert_eq!(next_delta(&mut subscription), (vec![], vec![], 2));
    block_on(db.restore("notes", note, None, Default::default())).unwrap();
    assert_eq!(
        next_delta(&mut subscription),
        (vec!["first".into(), "second".into()], vec![], 0)
    );
}

/// A history subscription under a read policy follows the current row's
/// access: when the note is handed to someone else, its whole history
/// leaves Bob's result.
#[test]
fn history_subscriptions_follow_current_access() {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("owner", ColumnType::Text)
                    .column("title", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::Cmp {
                                column: "owner".to_owned(),
                                op: CmpOp::Eq,
                                value: PolicyValue::SessionRef(vec![
                                    "user".to_owned(),
                                    "identity".to_owned(),
                                    "subject".to_owned(),
                                ]),
                            })
                            .with_insert(PolicyExpr::True)
                            .with_update(Some(PolicyExpr::True), PolicyExpr::True),
                    ),
            )
            .build(),
    );
    let db = open(schema, NODE, AuthorSubject::SYSTEM);
    let bob = AuthorSubject::for_test_bytes([0xb0; 16]);
    let note = row(1);
    let owner = |who: String| BTreeMap::from([("owner".to_owned(), Value::String(who))]);
    let mut cells = owner(bob.principal_parts().1);
    cells.insert("title".to_owned(), Value::String("mine".into()));
    block_on(db.insert(
        "notes",
        cells,
        jazz::db::InsertOptions {
            row_id: Some(note),
            ..Default::default()
        },
    ))
    .unwrap();
    let prepared = db.prepare_query(&history_of(note)).unwrap();
    let mut subscription =
        block_on(db.subscribe_for_identity(&prepared, ReadOpts::default(), bob)).unwrap();
    assert_eq!(
        next_delta(&mut subscription),
        (vec!["mine".into()], vec![], 0)
    );
    block_on(db.update(
        "notes",
        note,
        owner("someone else".into()),
        Default::default(),
    ))
    .unwrap();
    assert_eq!(next_delta(&mut subscription), (vec![], vec![], 1));
}

/// A server, plus clients for Alice and Bob connected to it.
struct Synced {
    server: Db,
    alice: Db,
    bob: Db,
    /// Bob's replica holds every note, so he can update them.
    _bob_notes: jazz::db::SubscriptionStream,
}

const SERVER_NODE: [u8; 16] = [0x01; 16];
const ALICE_NODE: [u8; 16] = [0x02; 16];
const BOB_NODE: [u8; 16] = [0x03; 16];

impl Synced {
    fn new() -> Self {
        let server = open(notes_schema(), SERVER_NODE, AuthorSubject::SYSTEM);
        let connect = |node: [u8; 16], who: u8| {
            let author = AuthorSubject::for_test_bytes([who; 16]);
            let client = open(notes_schema(), node, author);
            let (client_side, server_side) = duplex();
            std::mem::forget(block_on(client.connect_upstream(client_side)));
            server.accept_subscriber(server_side, author);
            client.set_remote_link_hint(jazz::db::RemoteLinkHint::Live);
            client
        };
        let alice = connect(ALICE_NODE, 0xa1);
        let bob = connect(BOB_NODE, 0xb0);
        let notes = bob.prepare_query(&Query::from("notes")).unwrap();
        let _bob_notes = block_on(bob.subscribe(&notes, ReadOpts::default())).unwrap();
        Self {
            server,
            alice,
            bob,
            _bob_notes,
        }
    }

    fn pump(&self) {
        for _ in 0..4 {
            block_on(self.alice.tick()).unwrap();
            block_on(self.bob.tick()).unwrap();
            block_on(self.server.tick()).unwrap();
        }
        block_on(self.alice.tick()).unwrap();
        block_on(self.bob.tick()).unwrap();
    }

    /// A host one-shot read on Alice's client, ticking everyone while it
    /// waits for the server.
    fn alice_reads(&self, query: &Query, opts: ReadOpts) -> Result<Vec<CurrentRow>, String> {
        use std::future::Future;
        let bytes = postcard::to_allocvec(query).unwrap();
        let read = self.alice.all_serialized_query(
            &bytes,
            opts,
            None,
            None,
            None,
            false,
            || false,
            |attachment| self.alice.detach_query(attachment),
        );
        let mut read = std::pin::pin!(read);
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        for _ in 0..50 {
            if let std::task::Poll::Ready(result) = read.as_mut().poll(&mut context) {
                return match result.map_err(|error| error.message)? {
                    jazz::db::SerializedReadResult::Rows(rows) => Ok(rows),
                    other => panic!("{other:?}"),
                };
            }
            block_on(self.server.tick()).unwrap();
            block_on(self.alice.tick()).unwrap();
        }
        panic!("the read did not complete");
    }
}

fn local_only() -> ReadOpts {
    ReadOpts {
        tier: ReadTier::LocalOnly,
        ..Default::default()
    }
}

/// Alice has never seen the note. A remote history read fetches every
/// version with the transaction that wrote it (the server's own commits
/// and Bob's upload alike) and keeps them, so a later local read answers
/// the same.
#[test]
fn remote_history_reads_fetch_versions_with_their_transactions() {
    let synced = Synced::new();
    let note = row(1);
    let first = insert(&synced.server, note, "first");
    synced.pump();
    let second = update(&synced.bob, note, "second");
    synced.pump();
    let third = update(&synced.server, note, "third");
    synced.pump();
    assert!(read(&synced.alice, &history_of(note), local_only()).is_empty());

    let remote = ReadOpts {
        tier: ReadTier::Remote,
        ..Default::default()
    };
    let versions = synced.alice_reads(&history_of(note), remote).unwrap();
    let expected = [
        (first, SERVER_NODE),
        (second, BOB_NODE),
        (third, SERVER_NODE),
    ];
    assert_eq!(titles(&versions), ["first", "second", "third"]);
    assert_eq!(
        versions.iter().map(transaction_id).collect::<Vec<_>>(),
        expected
    );
    let at_server = read(&synced.server, &history_of(note), ReadOpts::default());
    assert_eq!(
        versions.iter().map(accepted_at).collect::<Vec<_>>(),
        at_server.iter().map(accepted_at).collect::<Vec<_>>()
    );

    let cached = read(&synced.alice, &history_of(note), local_only());
    assert_eq!(titles(&cached), ["first", "second", "third"]);
    let current = read(
        &synced.alice,
        &Query::from("notes").filter(eq(col("id"), lit(Value::Uuid(note.0)))),
        local_only(),
    );
    assert_eq!(
        titles(&current),
        ["third"],
        "fetched history never stands in for the current row"
    );
}

/// Alice subscribes to a note's history. Versions arrive from the server
/// with their transactions; Bob's accepted update adds one; Alice's own
/// update joins only once the server accepted it.
#[test]
fn local_first_history_subscriptions_receive_accepted_versions() {
    let synced = Synced::new();
    let note = row(1);
    let first = insert(&synced.server, note, "first");
    synced.pump();
    let prepared = synced.alice.prepare_query(&history_of(note)).unwrap();
    let mut subscription =
        block_on(synced.alice.subscribe(&prepared, ReadOpts::default())).unwrap();
    let mut drain = || {
        let mut added = Vec::new();
        while let Some(event) = subscription.try_next_event() {
            if let jazz::db::SubscriptionEvent::Delta { added: rows, .. } = event {
                added.extend(rows.iter().map(|row| (**row).clone()));
            }
        }
        added
    };
    synced.pump();
    assert_eq!(titles(&drain()), ["first"]);

    let second = update(&synced.bob, note, "second");
    synced.pump();
    let added = drain();
    assert_eq!(titles(&added), ["second"]);
    assert_eq!(transaction_id(&added[0]), (second, BOB_NODE));

    let third = update(&synced.alice, note, "third");
    assert!(drain().is_empty(), "a pending write is not a version");
    synced.pump();
    let added = drain();
    assert_eq!(titles(&added), ["third"]);
    assert_eq!(transaction_id(&added[0]), (third, ALICE_NODE));

    assert_eq!(
        read(&synced.alice, &history_of(note), local_only())
            .iter()
            .map(transaction_id)
            .collect::<Vec<_>>(),
        [
            (first, SERVER_NODE),
            (second, BOB_NODE),
            (third, ALICE_NODE)
        ]
    );
}

/// Aggregates over history count and group every listed version, current
/// and superseded, equal values included; a deleted row's versions drop
/// out with it. Aggregates over the history metadata are refused.
#[test]
fn history_aggregates_summarize_every_version() {
    let db = open(notes_schema(), NODE, AuthorSubject::SYSTEM);
    insert(&db, row(1), "a");
    update(&db, row(1), "a");
    update(&db, row(1), "b");
    insert(&db, row(2), "a");
    let rows = |ids: &[u8]| {
        Query::from("history.notes").filter(jazz::query::in_list(
            col("id"),
            ids.iter().map(|n| lit(Value::Uuid(row(*n).0))),
        ))
    };
    let count = |query: Query| {
        read(&db, &query.count(), ReadOpts::default())
            .iter()
            .map(|row| (cell(row, "title"), cell(row, "count")))
            .collect::<Vec<_>>()
    };
    assert_eq!(count(rows(&[1])), [(None, Some(Value::U64(3)))]);
    // A repeated id contributes once.
    assert_eq!(count(rows(&[1, 2, 1])), [(None, Some(Value::U64(4)))]);
    assert_eq!(count(rows(&[])), [(None, Some(Value::U64(0)))]);
    let grouped = read(
        &db,
        &rows(&[1, 2])
            .count()
            .group_by("title")
            .order_by("title", OrderDirection::Asc),
        ReadOpts::default(),
    );
    assert_eq!(
        grouped
            .iter()
            .map(|row| (titles(std::slice::from_ref(row)), cell(row, "count")))
            .collect::<Vec<_>>(),
        [
            (vec!["a".to_owned()], Some(Value::U64(3))),
            (vec!["b".to_owned()], Some(Value::U64(1))),
        ]
    );

    block_on(db.delete("notes", row(1), Default::default())).unwrap();
    assert_eq!(count(rows(&[1, 2])), [(None, Some(Value::U64(1)))]);

    let message = error_of(&db, &rows(&[2]).max("$acceptedAt"), ReadOpts::default());
    assert!(
        message.contains("aggregates over history metadata"),
        "{message}"
    );
}
