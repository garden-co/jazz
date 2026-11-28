//! Partial-value updates (splices, JSON sets, `text_edit`) through a branch
//! view (SPEC 11 §11.4, SPEC 19.12).
//!
//! A partial edit applies to the value the view shows for the row: the
//! head's own value when the head has a version of it, else the base's
//! (a live base's current value, a frozen base's value at its cut). An edit
//! of an inherited value copies on write into the head, as a whole-value
//! update does; from then on the head masks the base, and later base edits
//! never flow into it. A head tombstone hides the row: the edit fails and
//! never reads the base beneath it.
//!
//! ```text
//! main:  insert body ─────────── edit main ───────────────►
//!            │ (inherited)            (masked: not seen by draft)
//! draft:     └─► splice ─► copy-on-write ─► splice (head-owned) ─►
//! ```

use jazz_types::legacy_test_future::ResultFutureExt;
use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use jazz::db::{
    Db, DbConfig, DbIdentity, DeleteOptions, ExactWriteTarget, InsertOptions, JsonSetEdit,
    LargeValueUpdate, LargeValueUpdatePage, LargeValueUpdateSplice, ReadOpts, ReadTier,
    SyncMessage, Transport, TransportError, UpdateOptions, WireBase, WriteTarget, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol::{BranchSelector, BranchViewBase};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    ColumnDescriptor, ColumnMergeStrategy, ColumnType, OpenTransactionId, RowDescriptor, Schema,
    TableName, TableSchema,
};

use crate::common::{allow_all_policies, compile_schema};

/// Over the 64 KiB inline threshold and several leaves long, so the value
/// is chunked and edits land in its epoch's edit tail.
const LARGE: usize = 300 * 1024;

/// Branched `docs`: a plain chunked text `body`, a markdown-merging
/// `notes`, a bytes `blob` and a JSON `meta`.
fn schema() -> JazzSchema {
    compile_schema(&Schema::from([(
        TableName::new("docs"),
        TableSchema {
            columns: RowDescriptor::new(vec![
                ColumnDescriptor::new("branch_id", ColumnType::Uuid),
                ColumnDescriptor::new("body", ColumnType::Text),
                ColumnDescriptor::new("notes", ColumnType::Text)
                    .merge_strategy(ColumnMergeStrategy::Markdown),
                ColumnDescriptor::new("blob", ColumnType::Bytea),
                ColumnDescriptor::new("meta", ColumnType::Json { schema: None }),
            ]),
            indexed_columns: None,
            composite_indexes: Vec::new(),
            policies: allow_all_policies(),
            branch_by: vec!["branch_id".into()],
            history: Default::default(),
        },
    )]))
}

fn open(node: u8) -> Db {
    block_on(Db::open(DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap()
}

fn branch(byte: u8) -> BranchSelector {
    BranchSelector::new([("branch_id", Value::Uuid(uuid::Uuid::from_bytes([byte; 16])))])
}

fn main_branch() -> BranchSelector {
    branch(0x10)
}

fn draft() -> BranchSelector {
    branch(0x20)
}

fn row() -> RowUuid {
    RowUuid::from_bytes([0xd0; 16])
}

/// Deterministic text that does not repeat, so chunk boundaries are real.
fn text(len: usize, seed: u64) -> String {
    let mut state = seed | 1;
    let mut out = String::with_capacity(len);
    while out.len() < len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.push(match state % 9 {
            0 => ' ',
            1 => '\n',
            n => char::from(b'a' + (n as u8) * 3),
        });
    }
    out
}

fn live() -> WriteTarget {
    WriteTarget::BranchView {
        head: draft(),
        base: Some(BranchViewBase::Current(main_branch())),
    }
}

fn on(target: WriteTarget) -> UpdateOptions {
    UpdateOptions {
        target,
        ..Default::default()
    }
}

fn on_main() -> UpdateOptions {
    on(WriteTarget::BranchView {
        head: main_branch(),
        base: None,
    })
}

fn view_of(target: &WriteTarget) -> ReadOpts {
    match target {
        WriteTarget::BranchView { head, base } => {
            ReadOpts::default().branch_view(head.clone(), base.clone())
        }
        WriteTarget::Root => ReadOpts::default(),
    }
}

fn main_view() -> ReadOpts {
    ReadOpts::default().branch_view(main_branch(), None)
}

/// Insert the row on `main` with a chunked `body` and `notes`.
fn seed(db: &Db, body: &str, notes: &str) {
    block_on(db.insert(
        "docs",
        BTreeMap::from([
            ("body".to_owned(), Value::String(body.to_owned())),
            ("notes".to_owned(), Value::String(notes.to_owned())),
            ("blob".to_owned(), Value::Bytes(vec![0xab; LARGE])),
            (
                "meta".to_owned(),
                Value::String(r#"{"title":"plan","items":[1,2,3]}"#.to_owned()),
            ),
        ]),
        InsertOptions {
            row_id: Some(row()),
            target: ExactWriteTarget::Branch(main_branch()),
            ..Default::default()
        },
    ))
    .unwrap();
}

fn cell(db: &Db, column: &str, opts: ReadOpts) -> Option<Value> {
    let query = db
        .prepare_query(&Query::from("docs").filter(eq(col("id"), lit(row().0))))
        .unwrap();
    let rows = block_on(db.all(
        &query,
        ReadOpts {
            tier: ReadTier::LocalOnly,
            ..opts
        },
    ))
    .unwrap();
    rows.first().and_then(|row| row.cell(column))
}

fn string(db: &Db, column: &str, opts: ReadOpts) -> String {
    match cell(db, column, opts) {
        Some(Value::String(text)) => text,
        other => panic!("`{column}` is {other:?}"),
    }
}

/// One UTF-8 splice of `column` at `at`, through `opts`' target.
fn splice(
    db: &Db,
    column: &str,
    at: usize,
    delete: usize,
    insert: &str,
    opts: UpdateOptions,
) -> Result<jazz::db::WriteHandle, jazz::db::Error> {
    block_on(db.update_with_large_value_mutations(
        "docs",
        row(),
        BTreeMap::new(),
        vec![LargeValueUpdate::Splice {
            column: column.to_owned(),
            within: LargeValueUpdatePage::text(at..at + delete),
            splices: vec![LargeValueUpdateSplice::text(0, delete, insert)],
        }],
        opts,
    ))
}

fn edited(at: &str, index: usize, delete: usize, insert: &str) -> String {
    let mut out = at.to_owned();
    out.replace_range(index..index + delete, insert);
    out
}

/// Alice splices a document her draft inherits from main. The draft gets
/// main's value with her edit, written as an edit of main's chunked value
/// (no new tree), and main is untouched. Her second splice edits the
/// draft's own value. A later edit of main does not reach the draft: the
/// draft's copy masks it.
#[test]
fn a_splice_through_a_live_view_copies_the_inherited_value_on_write() {
    let db = open(0x01);
    let body = text(LARGE, 3);
    seed(&db, &body, "notes");
    let nodes = db.large_value_nodes_for_test().unwrap().1;

    splice(&db, "body", 1000, 4, "[draft]", on(live())).unwrap();
    let first = edited(&body, 1000, 4, "[draft]");
    assert_eq!(string(&db, "body", view_of(&live())), first);
    assert_eq!(
        string(&db, "body", main_view()),
        body,
        "copy-on-write leaves the base's value as it was"
    );
    assert_eq!(
        db.large_value_nodes_for_test().unwrap().1,
        nodes,
        "the draft's first value is an edit of main's tree, not a new tree"
    );

    splice(&db, "body", 5, 0, "<head>", on(live())).unwrap();
    let second = edited(&first, 5, 0, "<head>");
    assert_eq!(string(&db, "body", view_of(&live())), second);

    splice(&db, "body", 0, 1, "M", on_main()).unwrap();
    assert_eq!(string(&db, "body", main_view()), edited(&body, 0, 1, "M"));
    assert_eq!(
        string(&db, "body", view_of(&live())),
        second,
        "the head masks the base: a later base edit does not flow into it"
    );
}

/// Alice's draft is over main frozen at a capture. Bob edits main
/// afterwards. Alice's splice applies to main's value at the capture, not
/// Bob's newer one, and main keeps Bob's value.
#[test]
fn a_splice_through_a_frozen_view_edits_the_base_value_at_its_cut() {
    let db = open(0x02);
    let body = text(LARGE, 5);
    seed(&db, &body, "notes");
    let snapshot = block_on(db.capture_snapshot()).unwrap();
    splice(&db, "body", 0, 3, "BOB", on_main()).unwrap();
    let frozen = WriteTarget::BranchView {
        head: draft(),
        base: Some(BranchViewBase::Snapshot {
            branch: main_branch(),
            snapshot,
        }),
    };

    splice(&db, "body", 10, 2, "alice", on(frozen.clone())).unwrap();
    assert_eq!(
        string(&db, "body", view_of(&frozen)),
        edited(&body, 10, 2, "alice"),
        "the edit's base is main as of the capture"
    );
    assert_eq!(string(&db, "body", main_view()), edited(&body, 0, 3, "BOB"));
    assert_eq!(
        string(&db, "body", view_of(&live())),
        edited(&body, 10, 2, "alice"),
        "once copied, the head's own value shows over a live base too"
    );
}

/// Alice deletes the row in her draft; main still has it. Her splice
/// through the draft fails as for any row the view does not show, and
/// neither the draft nor main changes: the edit never reads main's value
/// beneath the head's tombstone.
#[test]
fn a_head_tombstone_hides_the_row_from_a_partial_edit() {
    let db = open(0x03);
    let body = text(LARGE, 7);
    seed(&db, &body, "notes");
    block_on(db.delete(
        "docs",
        row(),
        DeleteOptions {
            target: live(),
            ..Default::default()
        },
    ))
    .unwrap();

    let error = splice(&db, "body", 0, 1, "x", on(live())).unwrap_err();
    assert_eq!(error.code, jazz::db::ErrorCode::NotObserved, "{error:?}");
    assert_eq!(cell(&db, "body", view_of(&live())), None);
    assert_eq!(string(&db, "body", main_view()), body);
}

/// Alice's editor shows the draft's inherited notes. Before her edit
/// lands, Bob changes main's notes (the draft still inherits them). Her
/// `text_edit` merges over the value the view holds, main's newer notes,
/// keeping Bob's change, as it does outside branches; main keeps Bob's.
#[test]
fn edited_from_over_an_inherited_value_merges_with_the_bases_newer_value() {
    let db = open(0x04);
    let before = format!("{}\n\nalpha\n\nomega\n", text(LARGE, 9));
    seed(&db, &text(LARGE, 1), &before);
    let bob = before.replace("alpha", "alpha bob");
    block_on(db.update(
        "docs",
        row(),
        BTreeMap::from([("notes".to_owned(), Value::String(bob.clone()))]),
        on_main(),
    ))
    .unwrap();

    let alice = before.replace("omega", "omega alice");
    block_on(db.update_with_large_value_mutations(
        "docs",
        row(),
        BTreeMap::new(),
        vec![LargeValueUpdate::TextEdit {
            column: "notes".to_owned(),
            text: alice,
            edited_from: before.clone(),
        }],
        on(live()),
    ))
    .unwrap();
    let merged = string(&db, "notes", view_of(&live()));
    assert!(merged.contains("alpha bob"), "keeps Bob's base edit");
    assert!(merged.contains("omega alice"), "keeps Alice's edit");
    assert_eq!(string(&db, "notes", main_view()), bob);
}

/// JSON sets and byte splices through the view copy on write the same way.
#[test]
fn json_sets_and_byte_splices_through_a_view_copy_on_write() {
    let db = open(0x05);
    seed(&db, &text(LARGE, 11), "notes");
    block_on(db.update_with_large_value_mutations(
        "docs",
        row(),
        BTreeMap::new(),
        vec![
            LargeValueUpdate::JsonSet {
                column: "meta".to_owned(),
                edits: vec![JsonSetEdit {
                    at: "/items/1".to_owned(),
                    value: serde_json::json!("two"),
                }],
            },
            LargeValueUpdate::Splice {
                column: "blob".to_owned(),
                within: LargeValueUpdatePage::bytes(8..12),
                splices: vec![LargeValueUpdateSplice {
                    at: 0,
                    delete: 4,
                    insert: vec![1, 2, 3],
                }],
            },
        ],
        on(live()),
    ))
    .unwrap();
    let meta =
        |opts| -> serde_json::Value { serde_json::from_str(&string(&db, "meta", opts)).unwrap() };
    assert_eq!(
        meta(view_of(&live()))["items"],
        serde_json::json!([1, "two", 3])
    );
    assert_eq!(meta(main_view())["items"], serde_json::json!([1, 2, 3]));
    let blob = |opts| match cell(&db, "blob", opts) {
        Some(Value::Bytes(bytes)) => bytes,
        other => panic!("{other:?}"),
    };
    let mut expected = vec![0xab; LARGE];
    expected.splice(8..12, [1, 2, 3]);
    assert_eq!(blob(view_of(&live())), expected);
    assert_eq!(blob(main_view()), vec![0xab; LARGE]);
}

/// In one mergeable transaction, two splices through the view compose:
/// the second addresses the value the first made, the transaction's own
/// copy of the inherited row, and the commit is one head version.
#[test]
fn splices_in_a_transaction_through_a_view_compose_over_the_copy() {
    let db = open(0x06);
    let body = text(LARGE, 13);
    seed(&db, &body, "notes");
    let tx = OpenTransactionId::new();
    block_on(db.begin_mergeable(tx)).unwrap();
    for (at, insert) in [(100, "one"), (101, "two")] {
        block_on(db.tx_ref(tx).update_with_large_value_mutations(
            "docs",
            row(),
            BTreeMap::new(),
            vec![LargeValueUpdate::Splice {
                column: "body".to_owned(),
                within: LargeValueUpdatePage::text(at..at),
                splices: vec![LargeValueUpdateSplice::text(0, 0, insert)],
            }],
            on(live()),
        ))
        .unwrap();
    }
    block_on(db.commit_mergeable_handle(tx)).unwrap();
    let expected = edited(&edited(&body, 100, 0, "one"), 101, 0, "two");
    assert_eq!(string(&db, "body", view_of(&live())), expected);
    assert_eq!(string(&db, "body", main_view()), body);

    let history = |head: BranchSelector| {
        let query = Query::from("history.docs")
            .filter(eq(col("id"), lit(Value::Uuid(row().0))))
            .order_by("$acceptedAt", OrderDirection::Asc);
        let prepared = db.prepare_query(&query).unwrap();
        block_on(db.all(&prepared, ReadOpts::default().branch_view(head, None)))
            .unwrap()
            .into_iter()
            .map(|row| match row.cell("body") {
                Some(Value::String(body)) => body,
                other => panic!("{other:?}"),
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        history(draft()),
        [expected],
        "one head version, no base parent"
    );
    assert_eq!(
        history(main_branch()),
        [body],
        "main's history is untouched"
    );
}

/// Outside a branch view a row id does not name one branch-local row of a
/// branched table, so a partial edit there is refused explicitly.
#[test]
fn a_partial_edit_of_a_branched_table_outside_a_view_is_refused() {
    let db = open(0x07);
    seed(&db, &text(LARGE, 15), "notes");
    let error = splice(&db, "body", 0, 1, "x", UpdateOptions::default()).unwrap_err();
    assert!(
        error
            .message
            .contains("partial-value updates on a branched table outside a branch view"),
        "{error:?}"
    );
}

// ---- across sync ----

struct Pipe {
    out: Rc<RefCell<VecDeque<SyncMessage>>>,
    inbound: Rc<RefCell<VecDeque<SyncMessage>>>,
}

impl Transport for Pipe {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.out.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }
}

fn connect(server: &Db, client: &Db) {
    connect_tapped(server, client);
}

/// [`connect`], with the queue of what the client sends the server.
fn connect_tapped(server: &Db, client: &Db) -> Rc<RefCell<VecDeque<SyncMessage>>> {
    let up = Rc::new(RefCell::new(VecDeque::new()));
    let down = Rc::new(RefCell::new(VecDeque::new()));
    let upstream = block_on(client.connect_upstream(Box::new(Pipe {
        out: up.clone(),
        inbound: down.clone(),
    })));
    server
        .accept_subscriber(
            Box::new(Pipe {
                out: down,
                inbound: up.clone(),
            }),
            AuthorSubject::SYSTEM,
        )
        .unwrap();
    std::mem::forget(upstream);
    up
}

fn pump(server: &Db, clients: &[&Db]) {
    for _ in 0..12 {
        for client in clients {
            block_on(client.tick()).unwrap();
        }
        block_on(server.tick()).unwrap();
    }
    for client in clients {
        block_on(client.tick()).unwrap();
    }
}

fn subscribe(db: &Db, opts: ReadOpts) -> jazz::db::SubscriptionStream {
    let query = db
        .prepare_query(&Query::from("docs").filter(eq(col("id"), lit(row().0))))
        .unwrap();
    block_on(db.subscribe(
        &query,
        ReadOpts {
            tier: ReadTier::Remote,
            ..opts
        },
    ))
    .unwrap()
}

/// Alice splices, through her draft, a document her replica holds from
/// main. The head's first version travels with its chunked value as a
/// tree, not as a delta over main's image, so Carol, whose replica reads
/// only the draft and never held main's version, reads the edited value.
/// The authority stores it too, and main stays as it was everywhere.
///
/// ```text
/// alice ──view splice──► authority ──draft rows──► carol (draft only)
///   ▲ main rows             │
///   └───────────────────────┘
/// ```
#[test]
fn a_first_head_edit_reaches_a_replica_that_never_held_the_base() {
    let server = open(0x30);
    block_on(server.set_authority()).unwrap();
    let body = text(LARGE, 17);
    seed(&server, &body, "notes");
    let alice = open(0x31);
    let carol = open(0x32);
    connect(&server, &alice);
    connect(&server, &carol);
    let _alice = subscribe(&alice, view_of(&live()));
    let _carol = subscribe(&carol, ReadOpts::default().branch_view(draft(), None));
    pump(&server, &[&alice, &carol]);
    assert_eq!(string(&alice, "body", view_of(&live())), body);
    assert_eq!(
        cell(&carol, "body", main_view()),
        None,
        "carol holds no main version"
    );

    splice(&alice, "body", 2000, 5, "[alice]", on(live())).unwrap();
    pump(&server, &[&alice, &carol]);
    let expected = edited(&body, 2000, 5, "[alice]");
    let draft_only = ReadOpts::default().branch_view(draft(), None);
    assert_eq!(string(&server, "body", draft_only.clone()), expected);
    assert_eq!(string(&carol, "body", draft_only), expected);
    assert_eq!(string(&server, "body", main_view()), body);
    assert_eq!(cell(&carol, "body", main_view()), None);

    // Later edits build on the head's own settled version.
    splice(&alice, "body", 0, 0, ">", on(live())).unwrap();
    pump(&server, &[&alice, &carol]);
    assert_eq!(
        string(
            &carol,
            "body",
            ReadOpts::default().branch_view(draft(), None)
        ),
        edited(&expected, 0, 0, ">")
    );
}

/// Alice and Bob both make the draft's first edit of notes it inherits
/// from main, offline, each copying main's value; by `text_edit` or by
/// writing the notes whole. Their edits merge on the authority as two
/// concurrent edits of one row do: Bob's copy lands over Alice's head
/// version, and its merge base is the version of main both copied. Counter
/// and set columns already merge such copies; text does too.
///
/// ```text
/// main@s ──copy+edit──► alice ──┐
///    └─────copy+edit──► bob ────┴─► authority: merge3(main@s, alice, bob)
/// ```
#[test]
fn concurrent_first_head_edits_merge_their_text() {
    for whole in [false, true] {
        let server = open(0x40);
        block_on(server.set_authority()).unwrap();
        let notes = format!("{}\n\nalpha\n\nomega\n", text(LARGE, 19));
        seed(&server, &text(LARGE, 21), &notes);
        let alice = open(0x41);
        let bob = open(0x42);
        connect(&server, &alice);
        connect(&server, &bob);
        let _a = subscribe(&alice, view_of(&live()));
        let _b = subscribe(&bob, view_of(&live()));
        pump(&server, &[&alice, &bob]);

        let edit = |db: &Db, text: String| {
            if whole {
                return block_on(db.update(
                    "docs",
                    row(),
                    BTreeMap::from([("notes".to_owned(), Value::String(text))]),
                    on(live()),
                ))
                .unwrap();
            }
            block_on(db.update_with_large_value_mutations(
                "docs",
                row(),
                BTreeMap::new(),
                vec![LargeValueUpdate::TextEdit {
                    column: "notes".to_owned(),
                    text,
                    edited_from: notes.clone(),
                }],
                on(live()),
            ))
            .unwrap()
        };
        let first = edit(&alice, notes.replace("alpha", "alpha alice"));
        let second = edit(&bob, notes.replace("omega", "omega bob"));
        // Alice's copy reaches the authority first.
        for _ in 0..4 {
            block_on(alice.tick()).unwrap();
            block_on(server.tick()).unwrap();
        }
        pump(&server, &[&alice, &bob]);

        let fate = |db: &Db, write: &jazz::db::WriteHandle| {
            block_on(
                db.wait_for_transaction(write.mergeable_tx_id(), jazz::tx::DurabilityTier::Global),
            )
        };
        assert!(fate(&alice, &first).is_ok());
        assert!(fate(&bob, &second).is_ok());
        let draft_only = ReadOpts::default().branch_view(draft(), None);
        let held = string(&server, "notes", draft_only.clone());
        assert!(
            held.contains("alpha alice") && held.contains("omega bob"),
            "both first edits are kept (whole: {whole})"
        );
        assert_eq!(string(&alice, "notes", draft_only.clone()), held);
        assert_eq!(string(&bob, "notes", draft_only), held);
        assert_eq!(
            string(&server, "notes", main_view()),
            notes,
            "main is untouched"
        );
    }
}

/// Bob makes the draft's first edit of notes it inherits while Carol,
/// concurrently, edits main's notes. The authority copies main as it holds
/// it, Carol's edit included, as it copies every other column, and Bob's
/// edit merges over it with the version of main he copied as the base.
/// Later edits of main no longer reach the draft.
#[test]
fn a_first_head_edit_merges_with_a_concurrent_base_edit() {
    let server = open(0x48);
    block_on(server.set_authority()).unwrap();
    let notes = format!("{}\n\nalpha\n\nomega\n", text(LARGE, 23));
    seed(&server, &text(LARGE, 25), &notes);
    let bob = open(0x49);
    let carol = open(0x4a);
    connect(&server, &bob);
    connect(&server, &carol);
    let _b = subscribe(&bob, view_of(&live()));
    let _c = subscribe(&carol, main_view());
    pump(&server, &[&bob, &carol]);

    let put = |db: &Db, text: String, opts: UpdateOptions| {
        block_on(db.update(
            "docs",
            row(),
            BTreeMap::from([("notes".to_owned(), Value::String(text))]),
            opts,
        ))
        .unwrap();
    };
    put(&carol, notes.replace("alpha", "alpha carol"), on_main());
    put(&bob, notes.replace("omega", "omega bob"), on(live()));
    for _ in 0..4 {
        block_on(carol.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    pump(&server, &[&bob, &carol]);
    let draft_only = ReadOpts::default().branch_view(draft(), None);
    let held = string(&server, "notes", draft_only.clone());
    assert!(
        held.contains("alpha carol") && held.contains("omega bob"),
        "{}",
        &held[held.len() - 40..]
    );
    assert_eq!(string(&bob, "notes", view_of(&live())), held);

    put(&carol, notes.replace("alpha", "alpha later"), on_main());
    pump(&server, &[&bob, &carol]);
    assert_eq!(
        string(&bob, "notes", view_of(&live())),
        held,
        "the head masks the base now"
    );
}

/// Alice deletes the row in the draft while Bob, concurrently, makes the
/// draft's first edit of its notes, copying main's value. Bob's content
/// write follows the ordinary update-versus-delete rule: it lands, but a
/// content write never implies restoration (INV-BVIEW-10), so the draft's
/// tombstone keeps the row hidden everywhere, and main still shows it.
#[test]
fn a_first_head_edit_concurrent_with_a_head_delete_does_not_restore() {
    let server = open(0x58);
    block_on(server.set_authority()).unwrap();
    let notes = format!("{}\n\nalpha\n", text(LARGE, 27));
    seed(&server, &text(LARGE, 29), &notes);
    let alice = open(0x59);
    let bob = open(0x5a);
    connect(&server, &alice);
    connect(&server, &bob);
    let _a = subscribe(&alice, view_of(&live()));
    let _b = subscribe(&bob, view_of(&live()));
    pump(&server, &[&alice, &bob]);

    block_on(alice.delete(
        "docs",
        row(),
        DeleteOptions {
            target: live(),
            ..Default::default()
        },
    ))
    .unwrap();
    block_on(bob.update_with_large_value_mutations(
        "docs",
        row(),
        BTreeMap::new(),
        vec![LargeValueUpdate::TextEdit {
            column: "notes".to_owned(),
            text: notes.replace("alpha", "alpha bob"),
            edited_from: notes.clone(),
        }],
        on(live()),
    ))
    .unwrap();
    for _ in 0..4 {
        block_on(alice.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    pump(&server, &[&alice, &bob]);
    for db in [&server, &alice, &bob] {
        assert_eq!(
            cell(db, "notes", view_of(&live())),
            None,
            "the draft's tombstone stands"
        );
    }
    assert_eq!(string(&server, "notes", main_view()), notes);
}

/// The bases the commits queued for the server name.
fn queued_bases(up: &Rc<RefCell<VecDeque<SyncMessage>>>) -> Vec<WireBase> {
    up.borrow()
        .iter()
        .flat_map(|message| match message {
            SyncMessage::Commit { ops, .. } => ops.iter().flat_map(|op| op.bases.clone()).collect(),
            _ => Vec::new(),
        })
        .collect()
}

/// Mallory's replica makes the draft's first edit of an inherited row, and
/// her commit's base for main's row is then rewritten by `tamper` before the
/// authority reads it: to a version of another row, or to a version on
/// another branch key she may read. Either would steer the text merge to
/// the wrong ancestor, so the authority rejects the commit and the draft
/// stays without a version of the row.
fn a_tampered_inherited_base_is_rejected(tamper: impl Fn(&mut WireBase, &Tampering)) {
    let server = open(0x70);
    block_on(server.set_authority()).unwrap();
    let notes = format!("{}\n\nalpha\n", text(LARGE, 31));
    seed(&server, &text(LARGE, 33), &notes);
    let insert = |row: RowUuid, on: BranchSelector| {
        block_on(server.insert(
            "docs",
            BTreeMap::from([
                ("body".to_owned(), Value::String("other".to_owned())),
                ("notes".to_owned(), Value::String(notes.clone())),
                ("blob".to_owned(), Value::Bytes(vec![1])),
                ("meta".to_owned(), Value::String("{}".to_owned())),
            ]),
            InsertOptions {
                row_id: Some(row),
                target: ExactWriteTarget::Branch(on),
                ..Default::default()
            },
        ))
        .unwrap()
        .version()
        .seq
    };
    let other_branch = branch(0x30);
    let other_row = insert(RowUuid::from_bytes([0xd1; 16]), main_branch());
    let other_key_version = insert(row(), other_branch.clone());
    let mallory = open(0x71);
    let up = connect_tapped(&server, &mallory);
    let _m = subscribe(&mallory, view_of(&live()));
    let _o = subscribe(
        &mallory,
        ReadOpts::default().branch_view(
            branch(0x31),
            Some(BranchViewBase::Current(other_branch.clone())),
        ),
    );
    pump(&server, &[&mallory]);

    // An honest copy over the other branch names that branch's key.
    let over_other = WriteTarget::BranchView {
        head: branch(0x31),
        base: Some(BranchViewBase::Current(other_branch)),
    };
    let edit = |opts: UpdateOptions| {
        block_on(mallory.update_with_large_value_mutations(
            "docs",
            row(),
            BTreeMap::new(),
            vec![LargeValueUpdate::TextEdit {
                column: "notes".to_owned(),
                text: notes.replace("alpha", "alpha mallory"),
                edited_from: notes.clone(),
            }],
            opts,
        ))
        .unwrap()
    };
    edit(on(over_other));
    block_on(mallory.tick()).unwrap();
    let other_key = queued_bases(&up)
        .into_iter()
        .map(|base| base.key)
        .next()
        .expect("the copy over the other branch names its base");
    pump(&server, &[&mallory]);

    let write = edit(on(live()));
    block_on(mallory.tick()).unwrap();
    let tampering = Tampering {
        other_row,
        other_key,
        other_key_version,
    };
    let mut tampered = 0;
    for message in up.borrow_mut().iter_mut() {
        if let SyncMessage::Commit { ops, .. } = message {
            for base in ops.iter_mut().flat_map(|op| op.bases.iter_mut()) {
                tamper(base, &tampering);
                tampered += 1;
            }
        }
    }
    assert_eq!(tampered, 1, "the copy names main's version only");
    pump(&server, &[&mallory]);
    let fate = block_on(
        mallory.wait_for_transaction(write.mergeable_tx_id(), jazz::tx::DurabilityTier::Global),
    );
    assert!(fate.is_err(), "the tampered commit is rejected: {fate:?}");
    assert_eq!(
        cell(
            &server,
            "notes",
            ReadOpts::default().branch_view(draft(), None)
        ),
        None,
        "the draft has no version of the row"
    );
    assert_eq!(string(&server, "notes", main_view()), notes);
}

struct Tampering {
    other_row: u64,
    other_key: Vec<u8>,
    other_key_version: u64,
}

#[test]
fn an_inherited_base_naming_another_rows_version_is_rejected() {
    a_tampered_inherited_base_is_rejected(|base, tampering| {
        base.on = jazz::db::BuiltOn::Seq(tampering.other_row);
    });
}

#[test]
fn an_inherited_base_on_another_branch_key_is_rejected() {
    a_tampered_inherited_base_is_rejected(|base, tampering| {
        base.key = tampering.other_key.clone();
        base.on = jazz::db::BuiltOn::Seq(tampering.other_key_version);
    });
}
