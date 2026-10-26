//! Deleted rows on SQLite: `includeDeleted` subscriptions and the
//! `$deletedAt` / `$deletedBy` columns. A deleted row's deletion metadata
//! is the update stamp of its deleted current version; live and restored
//! rows read NULL, and every history version reads its row's current
//! deletion. Include-deleted subscriptions keep deleted rows through their
//! deletion and restoration, locally and synced (a server ships a deleted
//! row's current version, and a resumed client keeps the deleted versions
//! it holds). Ported from #4173 and #4182's Groove behaviour.

use crate::common;

use std::collections::BTreeMap;

use jazz::db::{
    CurrentRow, Db, DbConfig, DbIdentity, ReadOpts, ReadTier, SubscriptionEvent,
    SubscriptionStream, block_on, duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol::{BranchSelector, ReadViewSpec};
use jazz::query::{OrderDirection, Query, col, eq, is_null, lit, not};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    CmpOp, ColumnType, PolicyExpr, PolicyValue, SchemaBuilder, TablePolicies, TableSchemaBuilder,
};

use common::{allow_all_policies, compile_schema};

fn notes_schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("title", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("tags")
                    .fk_column("note", "notes")
                    .column("label", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn open(schema: JazzSchema, node: u8, author: AuthorSubject) -> Db {
    block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )))
    .unwrap()
}

fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([0xa1; 16])
}

fn row(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}

fn title(text: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("title".to_owned(), Value::String(text.to_owned()))])
}

fn insert(db: &Db, id: RowUuid, text: &str) {
    block_on(db.insert(
        "notes",
        title(text),
        jazz::db::InsertOptions {
            row_id: Some(id),
            ..Default::default()
        },
    ))
    .unwrap();
}

fn update(db: &Db, id: RowUuid, text: &str) {
    block_on(db.update("notes", id, title(text), Default::default())).unwrap();
}

fn delete(db: &Db, id: RowUuid) {
    block_on(db.delete("notes", id, Default::default())).unwrap();
}

fn restore(db: &Db, id: RowUuid) {
    block_on(db.restore("notes", id, None, Default::default())).unwrap();
}

fn deleted() -> ReadOpts {
    ReadOpts {
        read_view: ReadViewSpec {
            include_deleted: true,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn local_only(opts: ReadOpts) -> ReadOpts {
    ReadOpts {
        tier: ReadTier::LocalOnly,
        ..opts
    }
}

/// Notes with their title and deletion metadata, by id.
fn notes() -> Query {
    Query::from("notes")
        .select([
            "title",
            "$updatedAt",
            "$deletedAt",
            "$deletedBy.identity.subject",
        ])
        .order_by("id", OrderDirection::Asc)
}

fn history_of(id: RowUuid) -> Query {
    Query::from("history.notes")
        .filter(eq(col("id"), lit(Value::Uuid(id.0))))
        .select(["title", "$deletedAt", "$deletedBy.identity.subject"])
        .order_by("$acceptedAt", OrderDirection::Asc)
}

fn read(db: &Db, query: &Query, opts: ReadOpts) -> Vec<CurrentRow> {
    block_on(db.all(&db.prepare_query(query).unwrap(), opts)).unwrap()
}

fn error_of(db: &Db, query: &Query, opts: ReadOpts) -> String {
    let error = match db.prepare_query(query) {
        Err(error) => error,
        Ok(prepared) => block_on(db.all(&prepared, opts)).expect_err("the read must be refused"),
    };
    error.message
}

fn cell(row: &CurrentRow, name: &str) -> Option<Value> {
    row.cells()
        .find(|(cell, _)| *cell == name)
        .and_then(|(_, value)| value.cloned())
        .and_then(|value| match value {
            Value::Nullable(inner) => inner.map(|inner| *inner),
            value => Some(value),
        })
}

fn text(row: &CurrentRow, name: &str) -> Option<String> {
    match cell(row, name) {
        Some(Value::String(text)) => Some(text),
        None => None,
        other => panic!("{name} {other:?}"),
    }
}

fn millis(row: &CurrentRow, name: &str) -> Option<u64> {
    match cell(row, name) {
        Some(Value::U64(at)) => Some(at),
        None => None,
        other => panic!("{name} {other:?}"),
    }
}

/// (title, $deletedAt, $deletedBy subject) of each row.
type Seen = (String, Option<u64>, Option<String>);

fn seen(rows: &[CurrentRow]) -> Vec<Seen> {
    rows.iter()
        .map(|row| {
            (
                text(row, "title").expect("title"),
                millis(row, "$deletedAt"),
                text(row, "$deletedBy.identity.subject"),
            )
        })
        .collect()
}

fn titles(rows: &[CurrentRow]) -> Vec<String> {
    seen(rows).into_iter().map(|(title, ..)| title).collect()
}

/// Every delta event pending on `subscription`: (added, updated, removed).
fn drain(subscription: &mut SubscriptionStream) -> (Vec<Seen>, Vec<Seen>, usize) {
    let (mut added, mut updated, mut removed) = (Vec::new(), Vec::new(), 0);
    while let Some(event) = subscription.try_next_event() {
        if let SubscriptionEvent::Delta {
            added: a,
            updated: u,
            removed: r,
            ..
        } = event
        {
            let rows = |rows: &[jazz::db::SubscriptionOutputRow]| {
                seen(&rows.iter().map(|row| row.row.clone()).collect::<Vec<_>>())
            };
            added.extend(rows(&a));
            updated.extend(rows(&u));
            removed += r.len();
        }
    }
    (added, updated, removed)
}

/// Alice deletes a note. Including deleted rows, it reads its content
/// with the deletion's stamp and author; the live note reads NULL, and so
/// does the deleted one once restored.
#[test]
fn deletion_metadata_names_the_deleting_version() {
    let db = open(notes_schema(), 1, alice());
    insert(&db, row(1), "doomed");
    update(&db, row(1), "doomed, edited");
    insert(&db, row(2), "kept");
    delete(&db, row(1));

    let rows = read(&db, &notes(), deleted());
    let me = alice().principal_parts().1;
    let doomed = &rows[0];
    assert!(doomed.is_deleted());
    assert_eq!(text(doomed, "title").as_deref(), Some("doomed, edited"));
    assert!(millis(doomed, "$deletedAt").is_some());
    assert_eq!(
        millis(doomed, "$deletedAt"),
        millis(doomed, "$updatedAt"),
        "the deletion is the deleted version's update"
    );
    assert_eq!(text(doomed, "$deletedBy.identity.subject"), Some(me));
    assert_eq!(seen(&rows[1..]), [("kept".into(), None, None)]);
    assert_eq!(
        titles(&read(&db, &notes().select(["title"]), ReadOpts::default())),
        ["kept"]
    );

    restore(&db, row(1));
    assert_eq!(
        seen(&read(&db, &notes(), deleted())),
        [
            ("doomed, edited".into(), None, None),
            ("kept".into(), None, None)
        ]
    );
}

/// The deletion columns filter and order like any nullable column.
#[test]
fn deletion_metadata_filters_and_orders() {
    let db = open(notes_schema(), 1, alice());
    insert(&db, row(1), "gone");
    insert(&db, row(2), "kept");
    delete(&db, row(1));

    let live = notes().filter(is_null(col("$deletedAt")));
    assert_eq!(titles(&read(&db, &live, deleted())), ["kept"]);
    let gone = notes().filter(not(is_null(col("$deletedAt"))));
    assert_eq!(titles(&read(&db, &gone, deleted())), ["gone"]);
    let by_deletion = Query::from("notes")
        .select(["title", "$deletedAt", "$deletedBy.identity.subject"])
        .order_by("$deletedAt", OrderDirection::Desc);
    assert_eq!(
        titles(&read(&db, &by_deletion, deleted())),
        ["gone", "kept"]
    );
}

/// Deletion metadata exists only where deleted rows are read: without
/// `includeDeleted` it is an error, and joined rows (always live) have none.
#[test]
fn deletion_metadata_needs_include_deleted_and_a_plain_root() {
    let db = open(notes_schema(), 1, alice());
    let message = error_of(&db, &notes(), ReadOpts::default());
    assert!(
        message.contains("Reading $deletedAt requires includeDeleted"),
        "{message}"
    );
    let filtered = Query::from("notes")
        .select(["title"])
        .filter(is_null(col("$deletedBy.identity.issuer")));
    let message = error_of(&db, &filtered, ReadOpts::default());
    assert!(message.contains("requires includeDeleted"), "{message}");
    let message = error_of(&db, &history_of(row(1)), ReadOpts::default());
    assert!(message.contains("requires includeDeleted"), "{message}");

    let joined = notes().join_via("tags", "note", []);
    let message = error_of(&db, &joined, deleted());
    assert!(message.contains("in joins"), "{message}");
}

/// `$deletedBy` reads as the whole author record (account and identity)
/// like `$createdBy`, its parts filter, and a live row reads NULL.
#[test]
fn deletion_author_reads_as_a_record() {
    let db = open(notes_schema(), 1, alice());
    insert(&db, row(1), "gone");
    insert(&db, row(2), "kept");
    delete(&db, row(1));

    let query = Query::from("notes").select(["$deletedBy", "$deletedBy.identity"]);
    let rows = read(&db, &query, deleted());
    let cell = |row: &CurrentRow, name: &str| -> Option<Vec<Value>> {
        match row.cells().find(|(n, _)| *n == name).unwrap().1 {
            Some(Value::Nullable(None)) | None => None,
            Some(Value::Nullable(Some(value))) => match &**value {
                Value::Record(record) => Some(record.to_values().unwrap()),
                other => panic!("{name}: {other:?}"),
            },
            other => panic!("{name}: {other:?}"),
        }
    };
    let (issuer, subject) = alice().principal_parts();
    let identity = vec![Value::String(issuer), Value::String(subject)];
    let whole = cell(&rows[0], "$deletedBy").expect("the deletion's author");
    let account = alice()
        .account_id()
        .unwrap_or(jazz::account_registry::SYSTEM_ACCOUNT_ID);
    assert_eq!(whole[0], Value::Uuid(account.0));
    let Value::Record(whole_identity) = &whole[1] else {
        panic!("{:?}", whole[1]);
    };
    assert_eq!(whole_identity.to_values().unwrap(), identity);
    assert_eq!(cell(&rows[0], "$deletedBy.identity"), Some(identity));
    assert_eq!(cell(&rows[1], "$deletedBy"), None);
    assert_eq!(cell(&rows[1], "$deletedBy.identity"), None);

    let by_account = notes().filter(eq(col("$deletedBy.account"), lit(Value::Uuid(account.0))));
    assert_eq!(titles(&read(&db, &by_account, deleted())), ["gone"]);
}

/// Every history version of a deleted row reads the row's current
/// deletion; a restoration clears it on all of them.
#[test]
fn history_versions_share_the_current_deletion() {
    let db = open(notes_schema(), 1, alice());
    insert(&db, row(1), "first");
    update(&db, row(1), "second");
    delete(&db, row(1));

    let versions = seen(&read(&db, &history_of(row(1)), deleted()));
    let current = seen(&read(&db, &notes(), deleted()));
    let deletion = (current[0].1, current[0].2.clone());
    assert!(deletion.0.is_some());
    assert_eq!(
        versions,
        [
            ("first".into(), deletion.0, deletion.1.clone()),
            ("second".into(), deletion.0, deletion.1.clone()),
        ]
    );

    restore(&db, row(1));
    assert_eq!(
        seen(&read(&db, &history_of(row(1)), deleted())),
        [("first".into(), None, None), ("second".into(), None, None)]
    );
}

/// An include-deleted subscription keeps a deleted row (updated with its
/// deletion) and a restored one; an ordinary subscription loses and
/// regains it.
#[test]
fn include_deleted_subscriptions_keep_deleted_rows() {
    let db = open(notes_schema(), 1, alice());
    insert(&db, row(1), "note");
    let prepared = db.prepare_query(&notes()).unwrap();
    let mut with_deleted = block_on(db.subscribe(&prepared, deleted())).unwrap();
    let live = db.prepare_query(&notes().select(["title"])).unwrap();
    let mut without = block_on(db.subscribe(&live, ReadOpts::default())).unwrap();
    assert_eq!(drain(&mut with_deleted).0, [("note".into(), None, None)]);
    assert_eq!(drain(&mut without).0.len(), 1);

    delete(&db, row(1));
    let (added, updated, removed) = drain(&mut with_deleted);
    assert!(added.is_empty() && removed == 0);
    assert_eq!(updated.len(), 1);
    assert!(updated[0].1.is_some(), "{updated:?}");
    assert_eq!(updated[0].2, Some(alice().principal_parts().1));
    assert_eq!(drain(&mut without).2, 1);

    restore(&db, row(1));
    assert_eq!(
        drain(&mut with_deleted),
        (vec![], vec![("note".into(), None, None)], 0)
    );
    assert_eq!(drain(&mut without).0.len(), 1);
}

/// An include-deleted history subscription keeps the versions of a
/// deleted row, each updated with the deletion, and clears it on restore.
#[test]
fn include_deleted_history_subscriptions_follow_the_deletion() {
    let db = open(notes_schema(), 1, alice());
    insert(&db, row(1), "first");
    update(&db, row(1), "second");
    let prepared = db.prepare_query(&history_of(row(1))).unwrap();
    let mut subscription = block_on(db.subscribe(&prepared, deleted())).unwrap();
    assert_eq!(drain(&mut subscription).0.len(), 2);

    delete(&db, row(1));
    let (added, updated, removed) = drain(&mut subscription);
    assert!(added.is_empty() && removed == 0);
    assert_eq!(
        updated
            .iter()
            .map(|(title, ..)| title.as_str())
            .collect::<Vec<_>>(),
        ["first", "second"]
    );
    assert!(updated.iter().all(|(_, at, _)| at.is_some()));

    restore(&db, row(1));
    assert_eq!(
        drain(&mut subscription),
        (
            vec![],
            vec![("first".into(), None, None), ("second".into(), None, None)],
            0
        )
    );
}

/// Including deleted rows does not widen read access: a deleted note
/// Bob may not read is never listed, and one he loses access to leaves.
#[test]
fn include_deleted_subscriptions_keep_the_read_policy() {
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
                            .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                            .with_delete(PolicyExpr::True),
                    ),
            )
            .build(),
    );
    let db = open(schema, 1, AuthorSubject::SYSTEM);
    let bob = AuthorSubject::for_test_bytes([0xb0; 16]);
    let owned = |who: String, text: &str| {
        let mut cells = title(text);
        cells.insert("owner".to_owned(), Value::String(who));
        cells
    };
    for (n, who, text) in [
        (1, bob.principal_parts().1, "bob's"),
        (2, "someone else".to_owned(), "theirs"),
    ] {
        block_on(db.insert(
            "notes",
            owned(who, text),
            jazz::db::InsertOptions {
                row_id: Some(row(n)),
                ..Default::default()
            },
        ))
        .unwrap();
        delete(&db, row(n));
    }
    let query = Query::from("notes").select(["title", "$deletedAt"]);
    let prepared = db.prepare_query(&query).unwrap();
    let mut subscription = block_on(db.subscribe_for_identity(&prepared, deleted(), bob)).unwrap();
    let (added, ..) = drain(&mut subscription);
    assert_eq!(
        added
            .iter()
            .map(|(title, ..)| title.as_str())
            .collect::<Vec<_>>(),
        ["bob's"]
    );

    restore(&db, row(1));
    block_on(db.update(
        "notes",
        row(1),
        owned("someone else".to_owned(), "given away"),
        Default::default(),
    ))
    .unwrap();
    assert_eq!(drain(&mut subscription).2, 1);
}

/// Deleted rows read through a branch view without a base: a row deleted
/// in the branch is listed with its deletion there and live elsewhere.
#[test]
fn include_deleted_reads_through_a_branch_view() {
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
    let db = open(schema, 1, alice());
    let branch =
        |n: u8| BranchSelector::new([("branch_id", Value::Uuid(uuid::Uuid::from_bytes([n; 16])))]);
    for n in [1, 2] {
        block_on(db.insert(
            "notes",
            title("note"),
            jazz::db::InsertOptions {
                row_id: Some(row(1)),
                target: jazz::db::ExactWriteTarget::Branch(branch(n)),
                ..Default::default()
            },
        ))
        .unwrap();
    }
    block_on(db.delete(
        "notes",
        row(1),
        jazz::db::DeleteOptions {
            target: jazz::db::WriteTarget::BranchView {
                head: branch(2),
                base: None,
            },
            ..Default::default()
        },
    ))
    .unwrap();

    let query = Query::from("notes").select(["title", "$deletedAt", "$deletedBy.identity.subject"]);
    let in_branch = |n: u8| deleted().branch_view(branch(n), None);
    assert_eq!(
        seen(&read(&db, &query, in_branch(1))),
        [("note".into(), None, None)]
    );
    let draft = seen(&read(&db, &query, in_branch(2)));
    assert_eq!(draft.len(), 1);
    assert!(draft[0].1.is_some());
    assert!(
        read(
            &db,
            &query.select(["title"]),
            ReadOpts::default().branch_view(branch(2), None)
        )
        .is_empty()
    );
}

// ---- synced -----------------------------------------------------------------

/// A server and Alice's client, reconnectable.
struct Synced {
    server: Db,
    alice: Db,
    upstream: Option<jazz::db::Upstream>,
}

impl Synced {
    fn new() -> Self {
        let server = open(notes_schema(), 0, AuthorSubject::SYSTEM);
        let alice = open(notes_schema(), 2, alice());
        let mut synced = Self {
            server,
            alice,
            upstream: None,
        };
        synced.connect();
        synced
    }

    fn connect(&mut self) {
        let (client_side, server_side) = duplex();
        self.upstream = Some(block_on(self.alice.connect_upstream(client_side)));
        self.server.accept_subscriber(server_side, alice());
        self.alice
            .set_remote_link_hint(jazz::db::RemoteLinkHint::Live);
    }

    fn disconnect(&mut self) {
        let upstream = self.upstream.take().expect("connected");
        assert!(self.alice.detach_connection(&upstream));
    }

    fn pump(&self) {
        for _ in 0..4 {
            block_on(self.alice.tick()).unwrap();
            block_on(self.server.tick()).unwrap();
        }
        block_on(self.alice.tick()).unwrap();
    }

    /// A host one-shot read on Alice's client, ticking both while it
    /// waits for the server.
    fn alice_reads(&self, query: &Query, opts: ReadOpts) -> Vec<CurrentRow> {
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
                return match result.unwrap() {
                    jazz::db::SerializedReadResult::Rows(rows) => rows,
                    other => panic!("{other:?}"),
                };
            }
            block_on(self.server.tick()).unwrap();
            block_on(self.alice.tick()).unwrap();
        }
        panic!("the read did not complete");
    }
}

fn remote(opts: ReadOpts) -> ReadOpts {
    ReadOpts {
        tier: ReadTier::Remote,
        ..opts
    }
}

/// A note was deleted before Alice ever saw it. A remote include-deleted
/// read fetches its deleted version (content and deletion) and keeps it;
/// an ordinary remote read does not list it.
#[test]
fn remote_include_deleted_reads_fetch_deleted_rows() {
    let synced = Synced::new();
    insert(&synced.server, row(1), "gone");
    update(&synced.server, row(1), "gone, edited");
    insert(&synced.server, row(2), "kept");
    delete(&synced.server, row(1));
    synced.pump();

    assert_eq!(
        titles(&synced.alice_reads(&notes().select(["title"]), remote(ReadOpts::default()))),
        ["kept"]
    );
    let at_server = seen(&read(&synced.server, &notes(), deleted()));
    let fetched = seen(&synced.alice_reads(&notes(), remote(deleted())));
    assert_eq!(fetched, at_server);
    assert!(fetched[0].1.is_some());
    assert_eq!(
        seen(&read(&synced.alice, &notes(), local_only(deleted()))),
        at_server
    );

    let history = seen(&synced.alice_reads(&history_of(row(1)), remote(deleted())));
    assert_eq!(
        history,
        seen(&read(&synced.server, &history_of(row(1)), deleted()))
    );
    assert_eq!(history.len(), 2);
}

/// Alice's include-deleted subscription follows the server: a row deleted
/// there stays with its deletion, a restoration clears it, and after a
/// reconnect the deleted rows she holds (and one deleted while she was
/// away) are still listed.
#[test]
fn synced_include_deleted_subscriptions_keep_deleted_rows() {
    let mut synced = Synced::new();
    insert(&synced.server, row(1), "first");
    insert(&synced.server, row(2), "second");
    insert(&synced.server, row(3), "third");
    delete(&synced.server, row(1));
    synced.pump();

    let prepared = synced.alice.prepare_query(&notes()).unwrap();
    let mut subscription = block_on(synced.alice.subscribe(&prepared, deleted())).unwrap();
    synced.pump();
    let listed = |synced: &Synced| seen(&read(&synced.alice, &notes(), local_only(deleted())));
    assert_eq!(
        listed(&synced),
        seen(&read(&synced.server, &notes(), deleted()))
    );
    assert!(listed(&synced)[0].1.is_some());
    drain(&mut subscription);

    delete(&synced.server, row(2));
    synced.pump();
    let (_, updated, removed) = drain(&mut subscription);
    assert_eq!(removed, 0);
    assert_eq!(updated.len(), 1);
    assert_eq!(updated[0].0, "second");
    assert!(updated[0].1.is_some());

    restore(&synced.server, row(1));
    synced.pump();
    assert_eq!(drain(&mut subscription).1, [("first".into(), None, None)]);

    synced.disconnect();
    delete(&synced.server, row(3));
    synced.connect();
    synced.pump();
    assert_eq!(
        listed(&synced),
        seen(&read(&synced.server, &notes(), deleted()))
    );
    assert_eq!(
        listed(&synced)
            .iter()
            .map(|(title, at, _)| (title.as_str(), at.is_some()))
            .collect::<Vec<_>>(),
        [("first", false), ("second", true), ("third", true)]
    );
}

/// A remote read asked right after a write, before any tick sent it, goes
/// out behind that write: the server answers with the writer's own row.
#[test]
fn a_remote_read_after_an_unsent_write_sees_it() {
    let synced = Synced::new();
    synced.pump();
    insert(&synced.alice, row(7), "mine");
    assert_eq!(
        titles(&synced.alice_reads(&notes().select(["title"]), remote(ReadOpts::default()))),
        ["mine"]
    );
}

/// A subscription's first answer (an empty result) and a row the server
/// committed after it arrive in one batch: the answer lists its result as of
/// its own seq, so the later row stays listed and reaches the subscription.
#[test]
fn a_row_arriving_with_an_earlier_empty_answer_stays_listed() {
    let synced = Synced::new();
    synced.pump();
    let prepared = synced
        .alice
        .prepare_query(&notes().select(["title"]))
        .unwrap();
    let mut subscription =
        block_on(synced.alice.subscribe(&prepared, ReadOpts::default())).unwrap();
    // The Subscribe goes out and the server answers it, empty.
    block_on(synced.alice.tick()).unwrap();
    block_on(synced.server.tick()).unwrap();
    // A write lands before Alice reads that answer.
    insert(&synced.server, row(5), "from bob");
    block_on(synced.server.tick()).unwrap();
    synced.pump();
    assert_eq!(
        titles(&read(
            &synced.alice,
            &notes().select(["title"]),
            local_only(ReadOpts::default())
        )),
        ["from bob"]
    );
    let (added, ..) = drain(&mut subscription);
    assert_eq!(added.len(), 1, "the subscription shows the row");
}
