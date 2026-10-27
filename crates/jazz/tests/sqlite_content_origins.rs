//! Ordinary replica reads retain the transaction behind accepted content.
//! Metadata is exercised through the public history API after ordinary reads;
//! transport capture checks ordering, omissions and bounded witness payloads.
mod common;

use jazz::db::{
    CurrentRow, Db, DbConfig, DbIdentity, ReadOpts, ReadTier, RemoteLinkHint, SerializedReadResult,
    SyncMessage, Transport, TransportError, block_on, duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{Query, col, eq, lit};
use jazz::records::Value;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use jazz::tx::DurabilityTier;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::future::Future;
use std::path::Path;
use std::pin::pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

fn open(node: u8, path: Option<&Path>, private: bool) -> Db {
    open_as(node, path, private, AuthorSubject::SYSTEM)
}
fn open_as(node: u8, path: Option<&Path>, private: bool, author: AuthorSubject) -> Db {
    let policies = if private {
        TablePolicies::new()
            .with_select(PolicyExpr::False)
            .with_insert(PolicyExpr::True)
    } else {
        common::allow_all_policies()
    };
    let schema = common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .policies(policies),
            )
            .build(),
    );
    let config = DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    );
    match path {
        Some(path) => block_on(Db::open_at(config, path, Default::default())),
        None => block_on(Db::open(config)),
    }
    .unwrap()
}

struct Capture {
    inner: Box<dyn Transport>,
    sent: Rc<RefCell<Vec<SyncMessage>>>,
    strip_origins: bool,
    legacy_rows: bool,
}
impl Transport for Capture {
    fn send(&mut self, mut message: SyncMessage) -> Result<(), TransportError> {
        if self.legacy_rows {
            match &mut message {
                SyncMessage::ContentWitnessesV1 { .. } => return Ok(()),
                SyncMessage::Rows { images } => {
                    let mut latest = BTreeMap::new();
                    for image in images.iter() {
                        let seq = latest
                            .entry((image.table.clone(), image.id, image.key.clone()))
                            .or_insert(0);
                        *seq = (*seq).max(image.seq);
                    }
                    images.retain(|image| {
                        latest[&(image.table.clone(), image.id, image.key.clone())] == image.seq
                    });
                }
                _ => {}
            }
        }
        if self.strip_origins && matches!(message, SyncMessage::Origins { .. }) {
            return Ok(());
        }
        self.sent.borrow_mut().push(message.clone());
        self.inner.send(message)
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inner.try_recv()
    }
}
fn connect(
    client: &Db,
    server: &Db,
    author: AuthorSubject,
    strip_origins: bool,
) -> Rc<RefCell<Vec<SyncMessage>>> {
    connect_mode(client, server, author, strip_origins, false)
}
fn connect_mode(
    client: &Db,
    server: &Db,
    author: AuthorSubject,
    strip_origins: bool,
    legacy_rows: bool,
) -> Rc<RefCell<Vec<SyncMessage>>> {
    let (a, b) = duplex();
    let sent = Rc::new(RefCell::new(Vec::new()));
    block_on(client.connect_upstream(a));
    server.accept_subscriber(
        Box::new(Capture {
            inner: b,
            sent: sent.clone(),
            strip_origins,
            legacy_rows,
        }),
        author,
    );
    sent
}
fn turn(dbs: &[&Db]) {
    for db in dbs {
        block_on(db.tick()).unwrap();
    }
}
fn drive<T>(dbs: &[&Db], future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    for _ in 0..100 {
        if let Poll::Ready(result) = future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            return result;
        }
        turn(dbs);
    }
    panic!("read or settlement did not finish");
}
fn text(value: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("text".into(), Value::String(value.into()))])
}
fn insert(db: &Db, value: &str) -> jazz::db::WriteHandle {
    block_on(db.insert("notes", text(value), Default::default())).unwrap()
}
fn opts(tier: ReadTier, deleted: bool) -> ReadOpts {
    let mut opts = ReadOpts::for_read_tier(tier);
    opts.read_view.include_deleted = deleted;
    opts
}
fn fetch(client: &Db, dbs: &[&Db], deleted: bool) -> Vec<CurrentRow> {
    try_fetch(client, dbs, deleted).unwrap()
}
fn try_fetch(client: &Db, dbs: &[&Db], deleted: bool) -> Result<Vec<CurrentRow>, jazz::db::Error> {
    try_fetch_at_tier(client, dbs, deleted, ReadTier::Remote)
}
fn try_fetch_at_tier(
    client: &Db,
    dbs: &[&Db],
    deleted: bool,
    tier: ReadTier,
) -> Result<Vec<CurrentRow>, jazz::db::Error> {
    let bytes = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let read = client.all_serialized_query_as(
        &bytes,
        opts(tier, deleted),
        None,
        None,
        None,
        false,
        || false,
        |_| {},
    );
    match drive(dbs, read)? {
        SerializedReadResult::Rows(rows) => Ok(rows),
        other => panic!("{other:?}"),
    }
}
fn history(db: &Db, row: RowUuid) -> Vec<CurrentRow> {
    let query = Query::from("history.notes").filter(eq(col("id"), lit(Value::Uuid(row.0))));
    block_on(db.all(
        &db.prepare_query(&query).unwrap(),
        opts(ReadTier::LocalOnly, true),
    ))
    .unwrap()
}
fn cell(row: &CurrentRow, name: &str) -> Value {
    row.cells()
        .find(|(n, _)| *n == name)
        .unwrap()
        .1
        .unwrap()
        .clone()
}
fn origin(row: &CurrentRow) -> (u64, [u8; 16]) {
    let Value::Tuple(parts) = cell(row, "$transactionId") else {
        panic!("not a transaction tuple")
    };
    let [Value::U64(tx), Value::Uuid(node)] = parts.as_slice() else {
        panic!("not a transaction identity")
    };
    (*tx, *node.as_bytes())
}
fn origins(messages: &[SyncMessage]) -> Vec<(u64, [u8; 16], u64)> {
    messages
        .iter()
        .filter_map(|m| match m {
            SyncMessage::Origins { commits } => Some(commits.iter().copied()),
            _ => None,
        })
        .flatten()
        .collect()
}

/// Bob uploads content; Alice cold-fetches only its latest state. A deletion
/// or content-free restore must keep Bob's content transaction, not the later
/// state-change transaction. Only the latest content witness is sent.
#[test]
fn cold_fetch_retains_uploaded_content_origin_after_delete_and_restore() {
    for state in ["live", "deleted", "restored"] {
        let server = open(3, None, false);
        server.set_authority();
        let bob = open(2, None, false);
        connect(&bob, &server, AuthorSubject::SYSTEM, false);
        let first = insert(&bob, "superseded");
        drive(
            &[&bob, &server],
            bob.wait_for_transaction(first.mergeable_tx_id(), DurabilityTier::Global),
        )
        .unwrap();
        let last = block_on(bob.update(
            "notes",
            first.row_uuid(),
            text("current"),
            Default::default(),
        ))
        .unwrap();
        drive(
            &[&bob, &server],
            bob.wait_for_transaction(last.mergeable_tx_id(), DurabilityTier::Global),
        )
        .unwrap();
        if state != "live" {
            block_on(server.delete("notes", first.row_uuid(), Default::default())).unwrap();
        }
        if state == "restored" {
            block_on(server.restore("notes", first.row_uuid(), None, Default::default())).unwrap();
        }
        // Another deleted row must not affect the content selected for this one.
        let unrelated = insert(&server, "unrelated");
        block_on(server.delete("notes", unrelated.row_uuid(), Default::default())).unwrap();
        let alice = open(1, None, false);
        let sent = connect(&alice, &server, AuthorSubject::SYSTEM, false);
        let rows = fetch(&alice, &[&alice, &server], state == "deleted");
        assert!(rows.iter().any(|r| r.row_uuid() == first.row_uuid()));
        let versions = history(&alice, first.row_uuid());
        assert_eq!(versions.len(), 1);
        assert_eq!(cell(&versions[0], "text"), Value::String("current".into()));
        assert_eq!(origin(&versions[0]), (last.mergeable_tx_id(), [2; 16]));
        let sent = sent.borrow();
        let first_origins = sent
            .iter()
            .position(|m| matches!(m, SyncMessage::Origins { .. }))
            .unwrap();
        let first_rows = sent
            .iter()
            .position(|m| matches!(m, SyncMessage::Rows { .. }))
            .unwrap();
        assert!(first_origins < first_rows);
        let shipped = sent
            .iter()
            .filter_map(|m| match m {
                SyncMessage::Rows { images } => Some(images),
                _ => None,
            })
            .flatten()
            .filter(|i| i.id == *first.row_uuid().0.as_bytes())
            .map(|image| image.seq)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(shipped.len(), if state == "live" { 1 } else { 2 });
        assert!(
            !origins(&sent)
                .iter()
                .any(|(_, node, tx)| *node == [2; 16] && *tx == first.mergeable_tx_id())
        );
    }
}

/// Alice's ordinary subscription gets new content origins on deltas as well
/// as its opening. A subsequent delete preserves the last content witness.
#[test]
fn ordinary_subscription_carries_origins_on_opening_and_updates() {
    let server = open(3, None, false);
    server.set_authority();
    let row = insert(&server, "first");
    let alice = open(1, None, false);
    connect(&alice, &server, AuthorSubject::SYSTEM, false);
    let query = alice.prepare_query(&Query::from("notes")).unwrap();
    let _subscription =
        block_on(alice.subscribe(&query, opts(ReadTier::LocalFirst, true))).unwrap();
    for _ in 0..5 {
        turn(&[&alice, &server]);
    }
    assert_eq!(
        origin(&history(&alice, row.row_uuid())[0]),
        (row.mergeable_tx_id(), [3; 16])
    );
    let update =
        block_on(server.update("notes", row.row_uuid(), text("second"), Default::default()))
            .unwrap();
    block_on(server.delete("notes", row.row_uuid(), Default::default())).unwrap();
    for _ in 0..5 {
        turn(&[&alice, &server]);
    }
    let history = history(&alice, row.row_uuid());
    assert_eq!(history.len(), 2);
    assert!(
        history
            .iter()
            .any(|r| origin(r) == (update.mergeable_tx_id(), [3; 16]))
    );
}

/// Alice's foreground receives content through a storage owner; both hops
/// must preserve the authority's origin, never substitute the relay's node.
#[test]
fn relay_forwards_origins_for_ordinary_reads() {
    let server = open(3, None, false);
    server.set_authority();
    let row = insert(&server, "authority");
    block_on(server.delete("notes", row.row_uuid(), Default::default())).unwrap();
    let owner = open(2, None, false);
    connect(&owner, &server, AuthorSubject::SYSTEM, false);
    assert_eq!(fetch(&owner, &[&owner, &server], true).len(), 1);
    let alice = open(1, None, false);
    let sent = connect(&alice, &owner, AuthorSubject::SYSTEM, false);
    assert_eq!(fetch(&alice, &[&alice, &owner, &server], true).len(), 1);
    assert_eq!(
        origin(&history(&alice, row.row_uuid())[0]),
        (row.mergeable_tx_id(), [3; 16])
    );
    assert!(
        origins(&sent.borrow())
            .iter()
            .all(|(_, node, _)| *node == [3; 16])
    );
}

/// This transport fault models an older/incomplete owner cache. The row may
/// remain readable, but a missing upstream origin must not become the relay's
/// own transaction. Foregrounds read the offline cache at LocalFirst; a strict
/// Remote read would wait for the authority. Pending content has no accepted origin.
#[test]
fn relay_never_invents_missing_or_optimistic_origins() {
    let server = open(3, None, false);
    server.set_authority();
    let row = insert(&server, "unknown origin");
    let account = AuthorSubject::authenticated("https://origins.example", "alice").unwrap();
    let owner = open_as(2, None, false, account);
    connect(&owner, &server, account, true);
    owner.set_scope_isolated_storage_owner().unwrap();
    fetch(&owner, &[&owner, &server], false);
    assert!(history(&owner, row.row_uuid()).is_empty());
    owner.set_remote_link_hint(RemoteLinkHint::Failed);
    let alice = open_as(1, None, false, account);
    alice.set_private_owner_client().unwrap();
    let sent = connect(&alice, &owner, account, false);
    assert_eq!(
        try_fetch_at_tier(&alice, &[&alice, &owner], false, ReadTier::LocalFirst)
            .unwrap()
            .len(),
        1
    );
    assert!(history(&alice, row.row_uuid()).is_empty());
    assert!(origins(&sent.borrow()).is_empty());
    owner.set_remote_link_hint(RemoteLinkHint::Failed);
    block_on(owner.update("notes", row.row_uuid(), text("pending"), Default::default())).unwrap();
    let bob = open_as(4, None, false, account);
    bob.set_private_owner_client().unwrap();
    let sent = connect(&bob, &owner, account, false);
    let rows = try_fetch_at_tier(&bob, &[&bob, &owner], false, ReadTier::LocalFirst).unwrap();
    assert_eq!(cell(&rows[0], "text"), Value::String("pending".into()));
    assert!(origins(&sent.borrow()).is_empty());
    assert!(history(&bob, row.row_uuid()).is_empty());
}

/// Mallory cannot read Alice's private row. Ordinary fetches must not disclose
/// either its content witness or its transaction origin as extra metadata.
#[test]
fn unreadable_rows_do_not_leak_origins_or_content_witnesses() {
    let server = open(3, None, true);
    server.set_authority();
    insert(&server, "private");
    let mallory = open(1, None, true);
    let author = AuthorSubject::authenticated("https://issuer.example", "mallory").unwrap();
    let sent = connect(&mallory, &server, author, false);
    assert!(fetch(&mallory, &[&mallory, &server], true).is_empty());
    assert!(origins(&sent.borrow()).is_empty());
    assert!(
        !sent
            .borrow()
            .iter()
            .any(|m| matches!(m, SyncMessage::Rows { .. }))
    );
}

/// Alice restarts offline after a cold fetch of a tombstone. Its last content
/// and original transaction stay paired in persisted SQLite history.
#[test]
fn content_witness_and_origin_survive_an_offline_restart() {
    let server = open(3, None, false);
    server.set_authority();
    let row = insert(&server, "retained");
    block_on(server.delete("notes", row.row_uuid(), Default::default())).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alice.sqlite");
    {
        let alice = open(1, Some(&path), false);
        connect(&alice, &server, AuthorSubject::SYSTEM, false);
        fetch(&alice, &[&alice, &server], true);
    }
    let alice = open(1, Some(&path), false);
    alice.set_remote_link_hint(RemoteLinkHint::Failed);
    let versions = history(&alice, row.row_uuid());
    assert_eq!(versions.len(), 1);
    assert_eq!(origin(&versions[0]), (row.mergeable_tx_id(), [3; 16]));
    assert_eq!(cell(&versions[0], "text"), Value::String("retained".into()));
}

/// This lower-level fixture pins the pre-existing Origins envelope: adding it
/// to ordinary reads must not change its tag, field order or integer encoding.
#[test]
fn content_origins_v1_keeps_existing_wire_bytes() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../fixtures/sqlite-sync-v3/content-origins.json"
    ))
    .unwrap();
    assert_eq!(fixture["role"], "jazz.sqlite.sync.content-origins.v1");
    assert_eq!(fixture["version"], 3);
    let message = SyncMessage::Origins {
        commits: vec![(7, [0x11; 16], (1 << 52) + 3)],
    };
    let bytes = jazz::db::encode_message(&message).unwrap();
    assert_eq!(hex::encode(&bytes), fixture["hex"].as_str().unwrap());
    let decoded = jazz::db::decode_message(&bytes).unwrap();
    assert_eq!(origins(&[decoded]), vec![(7, [0x11; 16], (1 << 52) + 3)]);
}

/// Bob wrote the same payload twice, but Alice's owner missed the second
/// content version. A tombstone from a peer without witness refs must never
/// attach the owner's older cached transaction, even when its bytes match.
/// Alice reads that offline owner cache through a LocalFirst foreground.
#[test]
fn a_gap_in_relay_history_does_not_guess_a_content_reference() {
    let server = open(3, None, false);
    server.set_authority();
    let row = insert(&server, "identical");
    let owner = open(2, None, false);
    connect_mode(&owner, &server, AuthorSubject::SYSTEM, false, true);
    fetch(&owner, &[&owner, &server], true);
    assert_eq!(history(&owner, row.row_uuid()).len(), 1);
    block_on(server.update(
        "notes",
        row.row_uuid(),
        text("identical"),
        Default::default(),
    ))
    .unwrap();
    block_on(server.delete("notes", row.row_uuid(), Default::default())).unwrap();
    fetch(&owner, &[&owner, &server], true);
    assert_eq!(history(&owner, row.row_uuid()).len(), 1);
    owner.set_remote_link_hint(RemoteLinkHint::Failed);
    let alice = open(1, None, false);
    alice.set_non_durable_client();
    let sent = connect(&alice, &owner, AuthorSubject::SYSTEM, false);
    assert_eq!(
        try_fetch_at_tier(&alice, &[&alice, &owner], true, ReadTier::LocalFirst)
            .unwrap()
            .len(),
        1
    );
    assert!(history(&alice, row.row_uuid()).is_empty());
    assert!(origins(&sent.borrow()).is_empty());
    assert!(
        !sent
            .borrow()
            .iter()
            .any(|m| matches!(m, SyncMessage::ContentWitnessesV1 { .. }))
    );
}

/// Alice's transaction pins a cut before Bob restores new content and deletes
/// it again. The as-of tombstone must link to the content at that cut, not to
/// Bob's later content, even though both state changes are retained locally.
#[test]
fn snapshot_fetch_links_the_content_at_its_own_cut() {
    let server = open(3, None, false);
    server.set_authority();
    let first = insert(&server, "at snapshot");
    let deleted = block_on(server.delete("notes", first.row_uuid(), Default::default())).unwrap();
    let alice = open(1, None, false);
    let sent = connect(&alice, &server, AuthorSubject::SYSTEM, false);
    let tx = block_on(alice.serializable_tx()).unwrap();
    let empty = alice.prepare_query(&Query::from("notes").limit(0)).unwrap();
    assert!(
        drive(&[&alice, &server], tx.all_prepared(&empty))
            .unwrap()
            .is_empty()
    );
    block_on(server.restore(
        "notes",
        first.row_uuid(),
        Some(text("later")),
        Default::default(),
    ))
    .unwrap();
    block_on(server.delete("notes", first.row_uuid(), Default::default())).unwrap();
    let query = alice.prepare_query(&Query::from("notes")).unwrap();
    let rows = drive(&[&alice, &server], tx.all_prepared_with(&query, true)).unwrap();
    assert_eq!(cell(&rows[0], "text"), Value::String("at snapshot".into()));
    assert!(sent.borrow().iter().any(|m| {
        match m {
            SyncMessage::ContentWitnessesV1 { witnesses } => witnesses.iter().any(|w| {
                w.row_seq == deleted.mergeable_tx_id() && w.content_seq == first.mergeable_tx_id()
            }),
            _ => false,
        }
    }));
}

/// Alice's storage owner restarts offline and serves a tombstone to Bob.
/// The exact content link is durable, with a byte-pinned postcard u64 value.
/// SQL inspection is needed because public row values cannot prove those bytes.
#[test]
fn persisted_witness_references_forward_after_owner_restart() {
    let server = open(3, None, false);
    server.set_authority();
    server.seed_seq_floor(126);
    let first = insert(&server, "retained");
    assert_eq!(first.mergeable_tx_id(), 127);
    let deleted = block_on(server.delete("notes", first.row_uuid(), Default::default())).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("owner.sqlite");
    {
        let owner = open(2, Some(&path), false);
        connect(&owner, &server, AuthorSubject::SYSTEM, false);
        fetch(&owner, &[&owner, &server], true);
    }
    let sql = rusqlite::Connection::open(&path).unwrap();
    let bytes: Vec<u8> = sql
        .query_row(
            "SELECT content FROM _jazz_content_witness_v1 WHERE id=?1 AND seq=?2",
            rusqlite::params![
                first.row_uuid().0.as_bytes().as_slice(),
                deleted.mergeable_tx_id() as i64
            ],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(hex::encode(bytes), "7f");
    drop(sql);
    let owner = open(2, Some(&path), false);
    owner.set_remote_link_hint(RemoteLinkHint::Failed);
    let bob = open(1, None, false);
    bob.set_non_durable_client();
    let sent = connect(&bob, &owner, AuthorSubject::SYSTEM, false);
    assert_eq!(
        try_fetch_at_tier(&bob, &[&bob, &owner], true, ReadTier::LocalFirst)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(origin(&history(&bob, first.row_uuid())[0]), (127, [3; 16]));
    assert!(
        sent.borrow()
            .iter()
            .any(|m| matches!(m, SyncMessage::ContentWitnessesV1 { .. }))
    );
}

/// Deliberately corrupting Alice's persisted witness tests the fail-closed
/// codec boundary: her relay must refuse rather than guess or truncate a link.
#[test]
fn malformed_persisted_witnesses_refuse_relay_reads() {
    for bytes in [vec![0xff], vec![1, 0], vec![0], vec![2]] {
        let server = open(3, None, false);
        server.set_authority();
        let row = insert(&server, "retained");
        block_on(server.delete("notes", row.row_uuid(), Default::default())).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("owner.sqlite");
        {
            let owner = open(2, Some(&path), false);
            connect(&owner, &server, AuthorSubject::SYSTEM, false);
            fetch(&owner, &[&owner, &server], true);
        }
        let sql = rusqlite::Connection::open(&path).unwrap();
        sql.execute("UPDATE _jazz_content_witness_v1 SET content=?1", [bytes])
            .unwrap();
        drop(sql);
        let owner = open(2, Some(&path), false);
        owner.set_remote_link_hint(RemoteLinkHint::Failed);
        let bob = open(1, None, false);
        bob.set_non_durable_client();
        connect(&bob, &owner, AuthorSubject::SYSTEM, false);
        let error =
            try_fetch_at_tier(&bob, &[&bob, &owner], true, ReadTier::LocalFirst).unwrap_err();
        assert_eq!(error.code, jazz::db::ErrorCode::Storage);
        assert!(error.message.contains("content-witness-v1"));
    }
}

/// The new reference message has an append-only tag; durable reference values
/// use a named postcard-u64 encoding with exact boundary-byte fixtures.
#[test]
fn content_witness_v1_pins_wire_and_durable_value_bytes() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../fixtures/sqlite-sync-v3/content-witness.json"
    ))
    .unwrap();
    assert_eq!(fixture["wireRole"], "jazz.sqlite.sync.content-witness.v1");
    assert_eq!(fixture["storageRole"], "jazz.sqlite.content-witness.v1");
    assert_eq!(fixture["version"], 3);
    let message: SyncMessage = serde_json::from_value(fixture["message"].clone()).unwrap();
    let bytes = jazz::db::encode_message(&message).unwrap();
    assert_eq!(hex::encode(&bytes), fixture["hex"].as_str().unwrap());
    let decoded = jazz::db::decode_message(&bytes).unwrap();
    assert_eq!(serde_json::to_value(decoded).unwrap(), fixture["message"]);
    for case in fixture["durableValues"].as_array().unwrap() {
        let value = case["value"].as_u64().unwrap();
        let bytes = postcard::to_allocvec(&value).unwrap();
        assert_eq!(hex::encode(&bytes), case["hex"].as_str().unwrap());
        assert_eq!(
            postcard::take_from_bytes::<u64>(&bytes).unwrap(),
            (value, &[][..])
        );
    }
}

/// Alice has the same row id in two branches. Deleting both must send one
/// exact witness for each branch, without using the other branch's content.
#[test]
fn content_witness_lookup_keeps_branch_keys_separate() {
    let schema = common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("branch", ColumnType::Uuid)
                    .column("text", ColumnType::Text)
                    .branch_by("branch")
                    .policies(common::allow_all_policies()),
            )
            .build(),
    );
    let open = |node| {
        block_on(Db::open(DbConfig::new(
            schema.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([node; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )))
        .unwrap()
    };
    let server = open(3);
    server.set_authority();
    let row = RowUuid::from_bytes([0x44; 16]);
    let branch = |n| {
        jazz::protocol::BranchSelector::new([(
            "branch",
            Value::Uuid(uuid::Uuid::from_bytes([n; 16])),
        )])
    };
    let mut expected = BTreeMap::new();
    for (n, value) in [(1, "main"), (2, "draft")] {
        let write = block_on(server.insert(
            "notes",
            text(value),
            jazz::db::InsertOptions {
                row_id: Some(row),
                target: jazz::db::ExactWriteTarget::Branch(branch(n)),
                ..Default::default()
            },
        ))
        .unwrap();
        expected.insert(value.to_owned(), (write.mergeable_tx_id(), [3; 16]));
        block_on(server.delete(
            "notes",
            row,
            jazz::db::DeleteOptions {
                target: jazz::db::WriteTarget::BranchView {
                    head: branch(n),
                    base: None,
                },
                ..Default::default()
            },
        ))
        .unwrap();
    }
    let alice = open(1);
    let sent = connect(&alice, &server, AuthorSubject::SYSTEM, false);
    assert_eq!(fetch(&alice, &[&alice, &server], true).len(), 2);
    let actual = history(&alice, row)
        .iter()
        .map(|version| {
            let Value::String(text) = cell(version, "text") else {
                panic!("text")
            };
            (text, origin(version))
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(actual, expected);
    let keys = sent
        .borrow()
        .iter()
        .filter_map(|m| match m {
            SyncMessage::ContentWitnessesV1 { witnesses } => {
                Some(witnesses.iter().map(|w| w.key.clone()))
            }
            _ => None,
        })
        .flatten()
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(keys.len(), 2);
}

/// A malformed or conflicting upstream link must fail explicitly. Mallory
/// also cannot submit an otherwise valid server-to-client witness on a session
/// connection and have the authority treat it as an upload.
#[test]
fn witness_protocol_rejects_invalid_conflicting_and_wrong_direction_messages() {
    let reference = |row_seq, content_seq| jazz::db::ContentWitnessRef {
        table: "notes".into(),
        id: [0x22; 16],
        key: vec![],
        row_seq,
        content_seq,
    };
    let message = |reference| SyncMessage::ContentWitnessesV1 {
        witnesses: vec![reference],
    };
    for (state, content) in [(3, 0), (3, 3), (3, 4), (1 << 52, 1)] {
        let alice = open(1, None, false);
        let (a, mut b) = duplex();
        block_on(alice.connect_upstream(a));
        b.send(message(reference(state, content))).unwrap();
        let error = block_on(alice.tick()).unwrap_err();
        assert_eq!(error.code, jazz::db::ErrorCode::Protocol);
        assert!(error.message.contains("invalid content-witness-v1"));
    }
    let alice = open(1, None, false);
    let (a, mut b) = duplex();
    block_on(alice.connect_upstream(a));
    b.send(message(reference(3, 1))).unwrap();
    block_on(alice.tick()).unwrap();
    b.send(message(reference(3, 2))).unwrap();
    assert!(
        block_on(alice.tick())
            .unwrap_err()
            .message
            .contains("conflicting content-witness-v1")
    );
    let server = open(3, None, false);
    let (mut a, b) = duplex();
    let mallory = AuthorSubject::authenticated("https://issuer.example", "mallory").unwrap();
    let subscriber = server.accept_subscriber(b, mallory);
    a.send(message(reference(3, 1))).unwrap();
    block_on(server.tick()).unwrap();
    let closed = server.take_closed_subscribers();
    assert_eq!(closed.len(), 1);
    assert_eq!(closed[0].0, subscriber);
    assert!(
        closed[0]
            .1
            .message
            .contains("server received ContentWitnessesV1")
    );
    assert_eq!(server.open_subscribers(), 0);
}
