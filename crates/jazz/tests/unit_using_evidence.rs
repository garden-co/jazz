//! The rows a unit's USING and read-for-write checks read (SPEC 7,
//! INV-RLS-9): the committed rows the unit acts on, never the unit's own
//! earlier writes. A unit is decided the same way on every path, whatever
//! order its writes come in: uploaded by a client, relayed, or staged in
//! an open transaction at the authority.
//!
//! `notes` belong to a team: any member updates a note, but only its owner
//! deletes it, hands it on, or pins it. `members` list who is in which
//! team; an account moves its own membership.

use crate::common;
use jazz_types::legacy_test_future::ResultFutureExt;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use jazz::db::{
    Acting, Db, DbConfig, DbIdentity, DeleteOptions, InsertOptions, ReadOpts, ReadTier,
    RestoreOptions, SyncMessage, Transport, TransportError, UpdateOptions, WireKind, WireOp,
    WireTarget, WireWrite, WriteIdentity, block_on,
};
use jazz::db::{ExactWriteTarget, WriteTarget};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol::{BranchSelector, BranchViewBase};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::time::TxTime;
use jazz::tools::{
    CmpOp, ColumnType, OpenTransactionId, PolicyExpr, PolicyValue, SchemaBuilder, TablePolicies,
    TableSchemaBuilder,
};
use jazz::tx::{DurabilityTier, TxId};

use common::{compile_schema, stored};

fn me() -> PolicyValue {
    PolicyValue::SessionRef(vec!["identity".to_owned(), "subject".to_owned()])
}

fn outer(column: &str) -> PolicyValue {
    PolicyValue::SessionRef(vec!["__jazz_outer_row".to_owned(), column.to_owned()])
}

fn cmp(column: &str, value: PolicyValue) -> PolicyExpr {
    PolicyExpr::Cmp {
        column: column.to_owned(),
        op: CmpOp::Eq,
        value,
    }
}

/// The committed version of the note under check, with `column` as the
/// new row has it: a WITH CHECK's own row reads as committed.
fn kept(column: &str) -> PolicyExpr {
    PolicyExpr::Exists {
        table: "notes".to_owned(),
        condition: Box::new(PolicyExpr::And(vec![
            cmp("id", outer("id")),
            cmp(column, outer(column)),
        ])),
    }
}

fn schema() -> JazzSchema {
    let member = PolicyExpr::Exists {
        table: "members".to_owned(),
        condition: Box::new(PolicyExpr::And(vec![
            cmp("team", outer("team")),
            cmp("account", me()),
        ])),
    };
    let mine = cmp("owner", me());
    let member_of_team = member.clone();
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("members")
                    .column("team", ColumnType::Text)
                    .column("account", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::False)
                            .with_update(Some(cmp("account", me())), cmp("account", me())),
                    ),
            )
            .table(
                TableSchemaBuilder::new("notes")
                    .column("team", ColumnType::Text)
                    .column("owner", ColumnType::Text)
                    .column("body", ColumnType::Text)
                    .column("pinned", ColumnType::Boolean)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::And(vec![member.clone(), mine.clone()]))
                            .with_update(
                                Some(member.clone()),
                                PolicyExpr::And(vec![
                                    member,
                                    // Only the owner hands a note on or pins it.
                                    kept("owner"),
                                    PolicyExpr::Or(vec![kept("pinned"), mine.clone()]),
                                ]),
                            )
                            .with_delete(mine.clone()),
                    ),
            )
            .table(
                // Only a team's members read its memos; anyone who may
                // read one may edit it.
                TableSchemaBuilder::new("memos")
                    .column("team", ColumnType::Text)
                    .column("body", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(member_of_team.clone())
                            .with_insert(PolicyExpr::False)
                            .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                            .with_delete(PolicyExpr::False),
                    ),
            )
            .table(
                // Notes on branches: any member edits one, its owner
                // deletes it.
                TableSchemaBuilder::new("drafts")
                    .column("branch_id", ColumnType::Uuid)
                    .column("team", ColumnType::Text)
                    .column("owner", ColumnType::Text)
                    .column("body", ColumnType::Text)
                    .branch_by("branch_id")
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::False)
                            .with_update(Some(member_of_team.clone()), member_of_team)
                            .with_delete(mine),
                    ),
            )
            .build(),
    )
}

fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([0xa1; 16])
}

fn bob() -> AuthorSubject {
    AuthorSubject::for_test_bytes([0xb0; 16])
}

fn subject(author: AuthorSubject) -> Value {
    Value::String(author.principal_parts().1)
}

fn text(s: &str) -> Value {
    Value::String(s.to_owned())
}

fn note_row() -> RowUuid {
    RowUuid::from_bytes([0x10; 16])
}

fn other_row() -> RowUuid {
    RowUuid::from_bytes([0x11; 16])
}

fn alice_in() -> RowUuid {
    RowUuid::from_bytes([0x20; 16])
}

fn database() -> InsertOptions {
    InsertOptions {
        identity: WriteIdentity::Database,
        ..Default::default()
    }
}

fn config(node: u8, author: AuthorSubject) -> DbConfig {
    DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )
}

/// An authority where alice and bob are in team `red` and bob owns
/// `note_row()` (and `other_row()`, in `blue`, a team alice is not in).
fn authority() -> Db {
    authority_with(config(0xee, AuthorSubject::SYSTEM))
}

fn authority_with(config: DbConfig) -> Db {
    let server = block_on(Db::open_history_complete(config)).unwrap();
    for (row, team, who) in [
        (alice_in(), "blue-less", alice()),
        (RowUuid::from_bytes([0x21; 16]), "red", alice()),
        (RowUuid::from_bytes([0x22; 16]), "red", bob()),
        (RowUuid::from_bytes([0x23; 16]), "blue", bob()),
    ] {
        block_on(server.insert(
            "members",
            BTreeMap::from([
                ("team".to_owned(), text(team)),
                ("account".to_owned(), subject(who)),
            ]),
            InsertOptions {
                row_id: Some(row),
                ..database()
            },
        ))
        .unwrap();
    }
    for (row, team) in [(note_row(), "red"), (other_row(), "blue")] {
        block_on(server.insert(
            "notes",
            BTreeMap::from([
                ("team".to_owned(), text(team)),
                ("owner".to_owned(), subject(bob())),
                ("body".to_owned(), text("bob's")),
                ("pinned".to_owned(), Value::Bool(false)),
            ]),
            InsertOptions {
                row_id: Some(row),
                ..database()
            },
        ))
        .unwrap();
    }
    block_on(server.insert(
        "memos",
        BTreeMap::from([
            ("team".to_owned(), text("blue")),
            ("body".to_owned(), text("bob's")),
        ]),
        InsertOptions {
            row_id: Some(memo_row()),
            ..database()
        },
    ))
    .unwrap();
    block_on(server.insert(
        "drafts",
        BTreeMap::from([
            ("team".to_owned(), text("red")),
            ("owner".to_owned(), subject(bob())),
            ("body".to_owned(), text("bob's")),
        ]),
        InsertOptions {
            row_id: Some(note_row()),
            target: ExactWriteTarget::Branch(base_branch()),
            ..database()
        },
    ))
    .unwrap();
    // The same row on another base, alice's.
    block_on(server.insert(
        "drafts",
        BTreeMap::from([
            ("team".to_owned(), text("red")),
            ("owner".to_owned(), subject(alice())),
            ("body".to_owned(), text("alice's")),
        ]),
        InsertOptions {
            row_id: Some(note_row()),
            target: ExactWriteTarget::Branch(alices_branch()),
            ..database()
        },
    ))
    .unwrap();
    server
}

fn alices_branch() -> BranchSelector {
    BranchSelector::new([("branch_id", Value::Uuid(uuid::Uuid::from_bytes([0xa2; 16])))])
}

/// The head branch over `base`.
fn view_over(base: BranchSelector) -> WriteTarget {
    WriteTarget::BranchView {
        head: head_branch(),
        base: Some(BranchViewBase::Current(base)),
    }
}

/// The draft as the head view over `base` shows it on `db`: its body.
fn draft_over(db: &Db, base: BranchSelector) -> Option<String> {
    let prepared = db.prepare_query(&Query::from("drafts")).unwrap();
    let opts = ReadOpts {
        tier: ReadTier::LocalOnly,
        ..ReadOpts::default()
    }
    .branch_view(head_branch(), Some(BranchViewBase::Current(base)));
    let rows = block_on(db.all(&prepared, opts)).unwrap();
    rows.iter()
        .find(|r| r.row_uuid() == note_row())
        .map(|r| format!("{:?}", r.cell("body")))
}

fn memo_row() -> RowUuid {
    RowUuid::from_bytes([0x12; 16])
}

fn base_branch() -> BranchSelector {
    BranchSelector::new([("branch_id", Value::Uuid(uuid::Uuid::from_bytes([0xba; 16])))])
}

fn head_branch() -> BranchSelector {
    BranchSelector::new([("branch_id", Value::Uuid(uuid::Uuid::from_bytes([0xbe; 16])))])
}

/// Alice's view of the head branch over the base.
fn draft_view() -> WriteTarget {
    WriteTarget::BranchView {
        head: head_branch(),
        base: Some(BranchViewBase::Current(base_branch())),
    }
}

fn draft_read() -> ReadOpts {
    ReadOpts {
        tier: ReadTier::LocalOnly,
        ..ReadOpts::default()
    }
    .branch_view(head_branch(), Some(BranchViewBase::Current(base_branch())))
}

/// The draft as `db`'s head view shows it: (owner is bob, body).
fn draft(db: &Db) -> Option<(bool, String)> {
    let prepared = db.prepare_query(&Query::from("drafts")).unwrap();
    let rows = block_on(db.all(&prepared, draft_read())).unwrap();
    rows.iter().find(|r| r.row_uuid() == note_row()).map(|r| {
        (
            r.cell("owner") == Some(subject(bob())),
            match r.cell("body") {
                Some(Value::String(body)) => body,
                other => format!("{other:?}"),
            },
        )
    })
}

fn memo(db: &Db) -> String {
    let prepared = db.prepare_query(&Query::from("memos")).unwrap();
    let rows = block_on(db.all(
        &prepared,
        ReadOpts {
            tier: ReadTier::LocalOnly,
            ..ReadOpts::default()
        },
    ))
    .unwrap();
    format!(
        "{:?}",
        rows.iter().map(|r| r.cell("body")).collect::<Vec<_>>()
    )
}

/// The note as `db` holds it: (owner is bob, pinned, body), or `None` once
/// deleted.
fn note(db: &Db, row: RowUuid) -> Option<(bool, bool, String)> {
    let prepared = db.prepare_query(&Query::from("notes")).unwrap();
    let rows = block_on(db.all(
        &prepared,
        ReadOpts {
            tier: ReadTier::LocalOnly,
            ..ReadOpts::default()
        },
    ))
    .unwrap();
    rows.iter().find(|r| r.row_uuid() == row).map(|r| {
        let owner = r.cell("owner");
        let pinned = r.cell("pinned");
        let body = match r.cell("body") {
            Some(Value::String(body)) => body,
            other => format!("{other:?}"),
        };
        (
            owner == Some(subject(bob())),
            pinned == Some(Value::Bool(true)),
            body,
        )
    })
}

fn bobs_note_intact(db: &Db, row: RowUuid) {
    assert_eq!(
        note(db, row),
        Some((true, false, "bob's".to_owned())),
        "bob's note is left as it was"
    );
}

// ---------------------------------------------------------------------
// Raw uploads: a client sends its unit's versions in an order it picks.

type Q = Rc<RefCell<VecDeque<SyncMessage>>>;

struct Pipe {
    out: Q,
    inbound: Q,
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

/// (server side, client->server queue, server->client queue)
fn pipe() -> (Box<dyn Transport>, Q, Q) {
    let up: Q = Default::default();
    let down: Q = Default::default();
    (
        Box::new(Pipe {
            out: down.clone(),
            inbound: up.clone(),
        }),
        up,
        down,
    )
}

fn fates(down: &Q) -> Vec<(u64, bool)> {
    down.borrow()
        .iter()
        .filter_map(|m| match m {
            SyncMessage::Fate {
                id: TxId {
                    time: TxTime(tx), ..
                },
                outcome,
                ..
            } => Some((*tx, outcome.result().is_ok())),
            _ => None,
        })
        .collect()
}

enum W {
    Update(RowUuid, &'static str, Value),
    Delete(RowUuid),
    Restore(RowUuid),
    UpdateMember(&'static str),
    UpdateMemo(&'static str),
    UpdateDraft(&'static str, Value),
    DeleteDraft,
}

fn wire(db: &Db, write: &W) -> WireOp {
    match write {
        W::Update(row, column, value) => WireWrite::Update {
            table: "notes".to_owned(),
            row: *row.as_bytes(),
            cells: BTreeMap::from([(stored(db, "notes", column), value.clone())]),
            target: WireTarget::Root,
            at: None,
        },
        W::Delete(row) => WireWrite::Delete {
            table: "notes".to_owned(),
            row: *row.as_bytes(),
            target: WireTarget::Root,
            at: None,
        },
        W::Restore(row) => WireWrite::Restore {
            table: "notes".to_owned(),
            row: *row.as_bytes(),
            cells: None,
            target: WireTarget::Root,
            at: None,
        },
        W::UpdateMember(team) => WireWrite::Update {
            table: "members".to_owned(),
            row: *alice_in().as_bytes(),
            cells: BTreeMap::from([(stored(db, "members", "team"), text(team))]),
            target: WireTarget::Root,
            at: None,
        },
        W::UpdateMemo(body) => WireWrite::Update {
            table: "memos".to_owned(),
            row: *memo_row().as_bytes(),
            cells: BTreeMap::from([(stored(db, "memos", "body"), text(body))]),
            target: WireTarget::Root,
            at: None,
        },
        W::UpdateDraft(..) | W::DeleteDraft => {
            unreachable!("a branch-view write is uploaded by a client")
        }
    }
    .into()
}

/// Upload `writes` from alice's node `node` as one mergeable unit to
/// `server`, and return whether it was accepted.
fn upload(server: &Db, node: u8, writes: &[W]) -> bool {
    let (s, up, down) = pipe();
    server.accept_subscriber(s, alice()).unwrap();
    up.borrow_mut().push_back(SyncMessage::Commit {
        clock: 0,
        id: TxId::new(TxTime(1), NodeUuid::from_bytes([node; 16])),
        kind: WireKind::Mergeable,
        ops: writes.iter().map(|w| wire(server, w)).collect(),
        acting: Acting::Own,
    });
    block_on(server.tick()).unwrap();
    let fates = fates(&down);
    assert_eq!(fates.len(), 1, "{fates:?}");
    fates[0].1
}

fn alice_is(owner: bool) -> Value {
    if owner {
        subject(alice())
    } else {
        subject(bob())
    }
}

/// The report: a member hands bob's note to herself and deletes it, in
/// one unit. The delete is judged on the committed note, which bob owns,
/// in either order.
#[test]
fn an_uploaded_unit_cannot_take_a_note_over_and_delete_it() {
    for (label, writes) in [
        (
            "update then delete",
            vec![
                W::Update(note_row(), "owner", alice_is(true)),
                W::Delete(note_row()),
            ],
        ),
        (
            "delete then update",
            vec![
                W::Delete(note_row()),
                W::Update(note_row(), "owner", alice_is(true)),
            ],
        ),
    ] {
        let server = authority();
        assert!(!upload(&server, 1, &writes), "{label}: accepted");
        bobs_note_intact(&server, note_row());
    }
}

/// Laundering the delete through a restore that puts the note back as it
/// was still deletes bob's note: the delete is judged on what bob owns.
#[test]
fn an_uploaded_unit_cannot_delete_through_a_takeover_it_undoes() {
    let server = authority();
    assert!(!upload(
        &server,
        1,
        &[
            W::Update(note_row(), "owner", alice_is(true)),
            W::Delete(note_row()),
            W::Restore(note_row()),
        ],
    ));
    bobs_note_intact(&server, note_row());
}

/// Pinning is the owner's: taking the note over first does not make
/// alice its owner for the pin.
#[test]
fn an_uploaded_unit_cannot_take_a_note_over_and_pin_it() {
    let server = authority();
    assert!(!upload(
        &server,
        1,
        &[
            W::Update(note_row(), "owner", alice_is(true)),
            W::Update(note_row(), "pinned", Value::Bool(true)),
        ],
    ));
    bobs_note_intact(&server, note_row());
}

/// One row's write is not evidence for another's USING either: moving
/// her membership into bob's `blue` team in the unit does not let alice
/// edit a `blue` note in it. In a later unit, it does.
#[test]
fn an_uploaded_unit_s_writes_are_no_using_evidence_for_its_other_rows() {
    let server = authority();
    assert!(!upload(
        &server,
        1,
        &[
            W::UpdateMember("blue"),
            W::Update(other_row(), "body", text("alice's")),
        ],
    ));
    bobs_note_intact(&server, other_row());
    // Planted positive: the same writes, one unit after the other.
    assert!(upload(&server, 2, &[W::UpdateMember("blue")]));
    assert!(upload(
        &server,
        3,
        &[W::Update(other_row(), "body", text("alice's"))]
    ));
    assert_eq!(
        note(&server, other_row()),
        Some((true, false, "alice's".to_owned()))
    );
}

/// Planted positives: a member edits a note, and edits it twice, in one
/// unit; the owner deletes it.
#[test]
fn an_uploaded_unit_still_writes_what_its_writer_may() {
    let server = authority();
    assert!(upload(
        &server,
        1,
        &[
            W::Update(note_row(), "body", text("one")),
            W::Update(note_row(), "body", text("two")),
        ],
    ));
    assert_eq!(
        note(&server, note_row()),
        Some((true, false, "two".to_owned()))
    );
}

// ---------------------------------------------------------------------
// A relay that admits a unit decides it by the same procedure.

#[test]
fn a_relayed_unit_cannot_take_a_note_over_and_delete_it() {
    use duplex_transport::duplex;
    let server = authority();
    let relay = block_on(Db::open(config(0x5e, alice()))).unwrap();
    let (relay_side, server_side) = duplex();
    block_on(relay.connect_upstream(relay_side));
    server.accept_subscriber(server_side, alice()).unwrap();
    // The relay holds bob's note, as alice's worker would.
    let prepared = relay.prepare_query(&Query::from("notes")).unwrap();
    let _live = block_on(relay.subscribe(&prepared, ReadOpts::default())).unwrap();
    for _ in 0..6 {
        block_on(relay.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    assert!(
        note(&relay, note_row()).is_some(),
        "the relay holds the note"
    );
    let (s, up, down) = pipe();
    relay.accept_subscriber(s, alice()).unwrap();
    up.borrow_mut().push_back(SyncMessage::Commit {
        clock: 0,
        id: TxId::new(TxTime(1), NodeUuid::from_bytes([0x1a; 16])),
        kind: WireKind::Mergeable,
        ops: vec![
            wire(&relay, &W::Update(note_row(), "owner", alice_is(true))),
            wire(&relay, &W::Delete(note_row())),
        ],
        acting: Acting::Own,
    });
    for _ in 0..20 {
        block_on(relay.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    assert_eq!(
        fates(&down).last().map(|f| f.1),
        Some(false),
        "{:?}",
        fates(&down)
    );
    bobs_note_intact(&server, note_row());
}

// ---------------------------------------------------------------------
// A real client: what it uploads for a transaction it commits.

fn drive<T>(dbs: &[&Db], future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    for _ in 0..200 {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
    }
    panic!("did not complete");
}

#[test]
fn a_client_transaction_cannot_take_a_note_over_and_delete_it() {
    use duplex_transport::duplex;
    let server = authority();
    let client = block_on(Db::open(config(0xa1, alice()))).unwrap();
    let (client_side, server_side) = duplex();
    block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice()).unwrap();
    let prepared = client.prepare_query(&Query::from("notes")).unwrap();
    let _live = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
    for _ in 0..6 {
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    assert!(
        note(&client, note_row()).is_some(),
        "the client holds the note"
    );

    let batch = OpenTransactionId::new();
    block_on(client.begin_mergeable(batch)).unwrap();
    let tx = client.tx_ref(batch);
    let staged = block_on(tx.update(
        "notes",
        note_row(),
        BTreeMap::from([("owner".to_owned(), subject(alice()))]),
        UpdateOptions::default(),
    ))
    .and_then(|_| block_on(tx.delete("notes", note_row(), DeleteOptions::default())));
    let committed = staged.and_then(|_| block_on(client.commit_mergeable_handle(batch)));
    if let Ok(receipt) = committed {
        let verdict = drive(
            &[&client, &server],
            client.wait_for_transaction(receipt.0, DurabilityTier::Global),
        );
        assert!(verdict.is_err(), "the authority accepted the takeover");
    }
    bobs_note_intact(&server, note_row());
}

// ---------------------------------------------------------------------
// Staged at the authority, as a thin client's or a backend's writes are.

fn update_as_alice() -> UpdateOptions {
    UpdateOptions {
        identity: WriteIdentity::Session(alice()),
        ..Default::default()
    }
}

fn delete_as_alice() -> DeleteOptions {
    DeleteOptions {
        identity: WriteIdentity::Session(alice()),
        ..Default::default()
    }
}

fn restore_as_alice() -> RestoreOptions {
    RestoreOptions {
        identity: WriteIdentity::Session(alice()),
        ..Default::default()
    }
}

/// Stage `writes` as alice in one open transaction at `server` and commit
/// it; whether it committed.
fn staged(server: &Db, writes: &[W]) -> bool {
    let open = OpenTransactionId::new();
    block_on(server.begin_mergeable(open)).unwrap();
    let tx = server.tx_ref(open);
    for write in writes {
        let result = match write {
            W::Update(row, column, value) => block_on(tx.update(
                "notes",
                *row,
                BTreeMap::from([((*column).to_owned(), value.clone())]),
                update_as_alice(),
            ))
            .map(drop),
            W::Delete(row) => block_on(tx.delete("notes", *row, delete_as_alice())).map(drop),
            W::Restore(row) => {
                block_on(tx.restore("notes", *row, None, restore_as_alice())).map(drop)
            }
            W::UpdateMember(team) => block_on(tx.update(
                "members",
                alice_in(),
                BTreeMap::from([("team".to_owned(), text(team))]),
                update_as_alice(),
            ))
            .map(drop),
            W::UpdateMemo(body) => block_on(tx.update(
                "memos",
                memo_row(),
                BTreeMap::from([("body".to_owned(), text(body))]),
                update_as_alice(),
            ))
            .map(drop),
            W::UpdateDraft(column, value) => block_on(tx.update(
                "drafts",
                note_row(),
                BTreeMap::from([((*column).to_owned(), value.clone())]),
                UpdateOptions {
                    target: draft_view(),
                    ..update_as_alice()
                },
            ))
            .map(drop),
            W::DeleteDraft => block_on(tx.delete(
                "drafts",
                note_row(),
                DeleteOptions {
                    target: draft_view(),
                    ..delete_as_alice()
                },
            ))
            .map(drop),
        };
        if result.is_err() {
            return false;
        }
    }
    block_on(server.commit_mergeable_handle(open)).is_ok()
}

#[test]
fn a_staged_transaction_cannot_take_a_note_over_and_delete_it() {
    let server = authority();
    assert!(!staged(
        &server,
        &[
            W::Update(note_row(), "owner", alice_is(true)),
            W::Delete(note_row())
        ]
    ));
    bobs_note_intact(&server, note_row());
    assert!(!staged(
        &server,
        &[
            W::Update(note_row(), "owner", alice_is(true)),
            W::Delete(note_row()),
            W::Restore(note_row())
        ]
    ));
    bobs_note_intact(&server, note_row());
    assert!(!staged(
        &server,
        &[
            W::Update(note_row(), "owner", alice_is(true)),
            W::Update(note_row(), "pinned", Value::Bool(true)),
        ]
    ));
    bobs_note_intact(&server, note_row());
    assert!(!staged(
        &server,
        &[
            W::UpdateMember("blue"),
            W::Update(other_row(), "body", text("alice's"))
        ]
    ));
    bobs_note_intact(&server, other_row());
    // Planted positives.
    assert!(staged(
        &server,
        &[
            W::Update(note_row(), "body", text("one")),
            W::Update(note_row(), "body", text("two")),
        ]
    ));
    assert_eq!(
        note(&server, note_row()),
        Some((true, false, "two".to_owned()))
    );
}

// ---------------------------------------------------------------------
// Read-for-write: the unit's own writes do not make a row readable.

/// Moving her membership into `blue` in the unit does not let alice read,
/// and so edit, a `blue` memo in it. In a later unit, it does.
#[test]
fn a_unit_s_writes_do_not_make_its_other_rows_readable_for_writing() {
    let writes = [W::UpdateMember("blue"), W::UpdateMemo("alice's")];
    let server = authority();
    assert!(!upload(&server, 1, &writes));
    assert!(!staged(&server, &writes));
    assert_eq!(memo(&server), r#"[Some(String("bob's"))]"#);
    // Planted positive: one unit after the other.
    assert!(staged(&server, &[W::UpdateMember("blue")]));
    assert!(upload(&server, 2, &[W::UpdateMemo("alice's")]));
    assert_eq!(memo(&server), r#"[Some(String("alice's"))]"#);
}

// ---------------------------------------------------------------------
// Branch views: a head row the unit copied from its base acts on the base.

#[test]
fn a_staged_branch_view_unit_cannot_take_a_draft_over_and_delete_it() {
    let server = authority();
    // Planted negative: alice may not delete bob's draft.
    assert!(!staged(&server, &[W::DeleteDraft]));
    assert!(!staged(
        &server,
        &[W::UpdateDraft("owner", subject(alice())), W::DeleteDraft]
    ));
    assert_eq!(draft(&server), Some((true, "bob's".to_owned())));
    // Planted positive: a member edits the draft twice in one unit.
    assert!(staged(
        &server,
        &[
            W::UpdateDraft("body", text("one")),
            W::UpdateDraft("body", text("two")),
        ]
    ));
    assert_eq!(draft(&server), Some((true, "two".to_owned())));
}

#[test]
fn a_client_branch_view_transaction_cannot_take_a_draft_over_and_delete_it() {
    use duplex_transport::duplex;
    let server = authority();
    let client = block_on(Db::open(config(0xa1, alice()))).unwrap();
    let (client_side, server_side) = duplex();
    block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice()).unwrap();
    let prepared = client.prepare_query(&Query::from("drafts")).unwrap();
    let _live = block_on(
        client.subscribe(
            &prepared,
            ReadOpts::default()
                .branch_view(head_branch(), Some(BranchViewBase::Current(base_branch()))),
        ),
    )
    .unwrap();
    for _ in 0..6 {
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    assert!(draft(&client).is_some(), "the client holds the draft");

    let batch = OpenTransactionId::new();
    block_on(client.begin_mergeable(batch)).unwrap();
    let tx = client.tx_ref(batch);
    let staged = block_on(tx.update(
        "drafts",
        note_row(),
        BTreeMap::from([("owner".to_owned(), subject(alice()))]),
        UpdateOptions {
            target: draft_view(),
            ..Default::default()
        },
    ))
    .and_then(|_| {
        block_on(tx.delete(
            "drafts",
            note_row(),
            DeleteOptions {
                target: draft_view(),
                ..Default::default()
            },
        ))
    });
    let committed = staged.and_then(|_| block_on(client.commit_mergeable_handle(batch)));
    if let Ok(receipt) = committed {
        let verdict = drive(
            &[&client, &server],
            client.wait_for_transaction(receipt.0, DurabilityTier::Global),
        );
        assert!(verdict.is_err(), "the authority accepted the takeover");
    }
    assert_eq!(draft(&server), Some((true, "bob's".to_owned())));
}

// ---------------------------------------------------------------------
// A head row's base is the one its own unit copied it from, never one
// another unit's copy named.

fn edit_draft(tx: &jazz::db::TxRef<'_>, base: BranchSelector, body: &str, who: WriteIdentity) {
    block_on(tx.update(
        "drafts",
        note_row(),
        BTreeMap::from([("body".to_owned(), text(body))]),
        UpdateOptions {
            identity: who,
            target: view_over(base),
            ..Default::default()
        },
    ))
    .unwrap();
}

fn delete_draft(
    tx: &jazz::db::TxRef<'_>,
    base: BranchSelector,
    who: WriteIdentity,
) -> Result<(), jazz::db::Error> {
    block_on(tx.delete(
        "drafts",
        note_row(),
        DeleteOptions {
            identity: who,
            target: view_over(base),
            ..Default::default()
        },
    ))
    .map(drop)
}

/// Another open transaction's copy over bob's base does not make alice's
/// own draft (over her base) bob's for her delete.
#[test]
fn another_staged_unit_s_copy_does_not_change_a_head_row_s_base() {
    let alice = || WriteIdentity::Session(alice());
    for other_first in [false, true] {
        let server = authority();
        let other = OpenTransactionId::new();
        if other_first {
            block_on(server.begin_mergeable(other)).unwrap();
            edit_draft(
                &server.tx_ref(other),
                base_branch(),
                "bob's, edited",
                alice(),
            );
        }
        let open = OpenTransactionId::new();
        block_on(server.begin_mergeable(open)).unwrap();
        let tx = server.tx_ref(open);
        edit_draft(&tx, alices_branch(), "alice's, edited", alice());
        delete_draft(&tx, alices_branch(), alice())
            .unwrap_or_else(|e| panic!("other first {other_first}: {e}"));
        block_on(server.commit_mergeable_handle(open))
            .unwrap_or_else(|e| panic!("other first {other_first}: {e}"));
        assert_eq!(draft_over(&server, alices_branch()), None);
    }
}

/// A refused unit's copy over alice's base does not make bob's draft
/// alice's for the next unit's delete, however soon it is decided: with
/// the authority's clock standing still, the next unit takes the refused
/// one's seq.
#[test]
fn a_refused_unit_s_copy_does_not_change_the_next_unit_s_base() {
    use duplex_transport::duplex;
    let server = authority_with(config(0xee, AuthorSubject::SYSTEM).with_clock(|| 1_000));
    // Two of alice's devices, each holding both views of the draft.
    let devices: Vec<Db> = [0xa1, 0xa3]
        .into_iter()
        .map(|node| {
            let client = block_on(Db::open(config(node, alice()))).unwrap();
            let (client_side, server_side) = duplex();
            block_on(client.connect_upstream(client_side));
            server.accept_subscriber(server_side, alice()).unwrap();
            client
        })
        .collect();
    let mut live = Vec::new();
    for client in &devices {
        let drafts = client.prepare_query(&Query::from("drafts")).unwrap();
        for base in [base_branch(), alices_branch()] {
            live.push(
                block_on(
                    client.subscribe(
                        &drafts,
                        ReadOpts::default()
                            .branch_view(head_branch(), Some(BranchViewBase::Current(base))),
                    ),
                )
                .unwrap(),
            );
        }
        let members = client.prepare_query(&Query::from("members")).unwrap();
        live.push(block_on(client.subscribe(&members, ReadOpts::default())).unwrap());
    }
    let pump = || {
        for _ in 0..6 {
            for client in &devices {
                block_on(client.tick()).unwrap();
            }
            block_on(server.tick()).unwrap();
        }
    };
    pump();
    for client in &devices {
        assert!(draft_over(client, base_branch()).is_some());
        assert!(draft_over(client, alices_branch()).is_some());
    }
    let (one, two) = (&devices[0], &devices[1]);

    // Unit A on one device: an edit over alice's base, and a write alice
    // may not make.
    let a = OpenTransactionId::new();
    block_on(one.begin_mergeable(a)).unwrap();
    let tx = one.tx_ref(a);
    edit_draft(
        &tx,
        alices_branch(),
        "alice's, edited",
        WriteIdentity::default(),
    );
    block_on(tx.update(
        "members",
        RowUuid::from_bytes([0x22; 16]),
        BTreeMap::from([("team".to_owned(), text("gone"))]),
        UpdateOptions::default(),
    ))
    .unwrap();
    let a = block_on(one.commit_mergeable_handle(a)).unwrap();
    // Unit B on the other, decided right after A: bob's draft over his
    // base, edited and deleted.
    let b = OpenTransactionId::new();
    block_on(two.begin_mergeable(b)).unwrap();
    let tx = two.tx_ref(b);
    edit_draft(
        &tx,
        base_branch(),
        "bob's, edited",
        WriteIdentity::default(),
    );
    delete_draft(&tx, base_branch(), WriteIdentity::default()).unwrap();
    let b = block_on(two.commit_mergeable_handle(b)).unwrap();
    let all: Vec<&Db> = devices.iter().chain([&server]).collect();
    let refused_a = drive(&all, one.wait_for_transaction(a.0, DurabilityTier::Global)).is_err();
    let refused_b = drive(&all, two.wait_for_transaction(b.0, DurabilityTier::Global)).is_err();
    assert!(refused_a, "unit A is refused");
    assert!(refused_b, "unit B deletes bob's draft");
    assert_eq!(
        draft_over(&server, base_branch()),
        Some(format!("{:?}", Some(text("bob's"))))
    );
}

/// A unit staged at the authority acts on the base its own commit copies
/// from, never one an upload decided in between named: the upload settled
/// the staging overlay, and with the clock standing still the commit takes
/// the refused upload's seq.
#[test]
fn an_upload_decided_while_a_unit_is_staged_does_not_change_its_base() {
    use duplex_transport::duplex;
    let server = authority_with(config(0xee, AuthorSubject::SYSTEM).with_clock(|| 1_000));
    // A third base, where the row is alice's too.
    let third =
        BranchSelector::new([("branch_id", Value::Uuid(uuid::Uuid::from_bytes([0xa3; 16])))]);
    block_on(server.insert(
        "drafts",
        BTreeMap::from([
            ("team".to_owned(), text("red")),
            ("owner".to_owned(), subject(alice())),
            ("body".to_owned(), text("alice's")),
        ]),
        InsertOptions {
            row_id: Some(note_row()),
            target: ExactWriteTarget::Branch(third.clone()),
            ..database()
        },
    ))
    .unwrap();
    let client = block_on(Db::open(config(0xa1, alice()))).unwrap();
    let (client_side, server_side) = duplex();
    block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice()).unwrap();
    let drafts = client.prepare_query(&Query::from("drafts")).unwrap();
    let _draft = block_on(
        client.subscribe(
            &drafts,
            ReadOpts::default()
                .branch_view(head_branch(), Some(BranchViewBase::Current(third.clone()))),
        ),
    )
    .unwrap();
    let members = client.prepare_query(&Query::from("members")).unwrap();
    let _members = block_on(client.subscribe(&members, ReadOpts::default())).unwrap();
    let pump = || {
        for _ in 0..6 {
            block_on(client.tick()).unwrap();
            block_on(server.tick()).unwrap();
        }
    };
    pump();
    assert!(draft_over(&client, third.clone()).is_some());

    // Alice stages an edit and a delete over her base.
    let alice_session = || WriteIdentity::Session(alice());
    let open = OpenTransactionId::new();
    block_on(server.begin_mergeable(open)).unwrap();
    let tx = server.tx_ref(open);
    edit_draft(&tx, alices_branch(), "alice's, edited", alice_session());
    delete_draft(&tx, alices_branch(), alice_session()).unwrap();
    // Bob takes that row back.
    block_on(server.update(
        "drafts",
        note_row(),
        BTreeMap::from([("owner".to_owned(), subject(bob()))]),
        jazz::db::UpdateOptions {
            identity: WriteIdentity::Database,
            target: WriteTarget::BranchView {
                head: alices_branch(),
                base: None,
            },
            ..Default::default()
        },
    ))
    .unwrap();
    // A refused upload copies the row into the head from the third base.
    let refused = OpenTransactionId::new();
    block_on(client.begin_mergeable(refused)).unwrap();
    let up = client.tx_ref(refused);
    edit_draft(&up, third, "alice's, edited", WriteIdentity::default());
    block_on(up.update(
        "members",
        RowUuid::from_bytes([0x22; 16]),
        BTreeMap::from([("team".to_owned(), text("gone"))]),
        UpdateOptions::default(),
    ))
    .unwrap();
    let refused = block_on(client.commit_mergeable_handle(refused)).unwrap();
    let verdict = drive(
        &[&client, &server],
        client.wait_for_transaction(refused.0, DurabilityTier::Global),
    );
    assert!(verdict.is_err(), "the upload is refused");

    // Alice's commit: the row she deletes is bob's now.
    assert!(
        block_on(server.commit_mergeable_handle(open)).is_err(),
        "the commit deletes bob's row"
    );
    assert!(draft_over(&server, alices_branch()).is_some());
}
