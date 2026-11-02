//! What a server accepts from the clients it serves, and what a store
//! resumes after a reopen: a client is not trusted to name another's node,
//! to send only messages the server serves, to write under policies a
//! sequence of its own writes would skip, or to send cells of other types.

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use crate::common::stored;

use jazz::db::{
    Acting, BuiltOn, Db, DbConfig, DbIdentity, DeleteOptions, ErrorCode, FileOptions,
    InsertOptions, ReadOpts, ReadSpan, ReadTier, SyncMessage, Transport, TransportError, WireBase,
    WireKind, WireOp, WireTarget, WireWrite, block_on,
};
use jazz::ids::RowUuid;
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::protocol::ReadViewSourceSpec;
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::time::TxTime;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use jazz::tx::TxId;

fn allow_all() -> TablePolicies {
    TablePolicies::new()
        .with_select(PolicyExpr::True)
        .with_insert(PolicyExpr::True)
        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
        .with_delete(PolicyExpr::True)
}

fn schema() -> JazzSchema {
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("messages")
                    .column("body", ColumnType::Text)
                    .policies(allow_all()),
            )
            .build(),
    )
    .unwrap()
}

fn user(n: u8) -> AuthorSubject {
    AuthorSubject::for_test_bytes([n; 16])
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

fn server() -> Db {
    let server = block_on(Db::open_history_complete(config(0, AuthorSubject::SYSTEM))).unwrap();
    server.track_closed_subscribers();
    server
}

fn client(node: u8, author: AuthorSubject) -> Db {
    block_on(Db::open(config(node, author))).unwrap()
}

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

/// (client side, server side, client->server queue, server->client queue)
fn pipe() -> (Box<dyn Transport>, Box<dyn Transport>, Q, Q) {
    let up: Q = Default::default();
    let down: Q = Default::default();
    (
        Box::new(Pipe {
            out: up.clone(),
            inbound: down.clone(),
        }),
        Box::new(Pipe {
            out: down.clone(),
            inbound: up.clone(),
        }),
        up,
        down,
    )
}

fn count(db: &Db) -> usize {
    let prepared = db.prepare_query(&Query::from("messages")).unwrap();
    block_on(db.all(
        &prepared,
        ReadOpts {
            tier: ReadTier::LocalOnly,
            ..ReadOpts::default()
        },
    ))
    .unwrap()
    .len()
}

fn post(db: &Db, body: &str) {
    let cells: BTreeMap<String, Value> =
        [("body".to_owned(), Value::String(body.to_owned()))].into();
    block_on(db.insert("messages", cells, InsertOptions::default())).unwrap();
}

fn rows(db: &Db) -> String {
    let prepared = db.prepare_query(&Query::from("messages")).unwrap();
    format!(
        "{:?}",
        block_on(db.all(&prepared, ReadOpts::default())).unwrap()
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

#[test]
fn a_message_the_server_does_not_serve_ends_only_that_session() {
    let server = server();
    let (_bad_client, bad_server, bad_up, _) = pipe();
    let bad = server.accept_subscriber(bad_server, user(9)).unwrap();
    let alice = client(1, user(1));
    let (a_client, a_server, _, _) = pipe();
    let _a = block_on(alice.connect_upstream(a_client));
    server.accept_subscriber(a_server, user(1)).unwrap();

    post(&alice, "hello");
    bad_up.borrow_mut().push_back(SyncMessage::Rows {
        images: Vec::new(),
        tx_ids: Vec::new(),
    });
    for _ in 0..4 {
        block_on(alice.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    assert_eq!(count(&server), 1, "alice's commit was served");
    let closed = server.take_closed_subscribers();
    assert_eq!(closed.len(), 1);
    assert_eq!(closed[0].0, bad);
    assert_eq!(server.open_subscribers(), 1);
}

/// A server-side transport that takes what the client sends and refuses
/// everything the server sends with `error`.
struct Refusing {
    error: TransportError,
    inbound: Q,
}

impl Transport for Refusing {
    fn send(&mut self, _: SyncMessage) -> Result<(), TransportError> {
        Err(self.error.clone())
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }
}

/// A session the server cannot send to is dropped with a typed reason for
/// the host to tell the client: one whose queue is full may reconnect
/// later, one sent a message that cannot be encoded may not.
#[test]
fn a_session_the_server_cannot_send_to_ends_with_why() {
    for (error, code) in [
        (TransportError::Backpressure, ErrorCode::Backpressure),
        (
            TransportError::Failed("over the message ceiling: not supported yet".to_owned()),
            ErrorCode::Protocol,
        ),
    ] {
        let server = server();
        post(&server, "hello");
        let alice = client(1, user(1));
        let (a_client, _, up, _) = pipe();
        let _a = block_on(alice.connect_upstream(a_client));
        let session = server
            .accept_subscriber(
                Box::new(Refusing {
                    error: error.clone(),
                    inbound: up,
                }),
                user(1),
            )
            .unwrap();
        let prepared = alice.prepare_query(&Query::from("messages")).unwrap();
        let _live = block_on(alice.subscribe(&prepared, ReadOpts::default())).unwrap();
        for _ in 0..4 {
            block_on(alice.tick()).unwrap();
            block_on(server.tick()).unwrap();
        }
        let closed = server.take_closed_subscribers();
        assert_eq!(closed.len(), 1, "{error:?}: {closed:?}");
        assert_eq!(closed[0].0, session);
        assert_eq!(closed[0].1.code, code, "{error:?}");
        assert!(
            closed[0].1.message.contains(&error.to_string()),
            "{:?}",
            closed[0].1
        );
        assert_eq!(server.open_subscribers(), 0);
    }
}

/// A host that never takes the sessions the server ends keeps none of
/// them: they are kept only once a host tracks them.
#[test]
fn closed_sessions_are_kept_only_for_a_host_that_tracks_them() {
    let server = block_on(Db::open_history_complete(config(0, AuthorSubject::SYSTEM))).unwrap();
    for _ in 0..3 {
        let (_client, server_side, up, _) = pipe();
        server.accept_subscriber(server_side, user(9)).unwrap();
        up.borrow_mut().push_back(SyncMessage::Rows {
            images: Vec::new(),
            tx_ids: Vec::new(),
        });
        block_on(server.tick()).unwrap();
    }
    assert_eq!(server.open_subscribers(), 0, "each was closed");
    server.track_closed_subscribers();
    assert!(
        server.take_closed_subscribers().is_empty(),
        "none were kept"
    );
}

#[test]
fn a_stale_subscriber_handle_does_not_close_the_session_reusing_its_slot() {
    let server = server();
    let (_old_client, old_server, _, _) = pipe();
    let old = server.accept_subscriber(old_server, user(1)).unwrap();
    server.disconnect_subscriber(old);
    let (_new_client, new_server, _, _) = pipe();
    let new = server.accept_subscriber(new_server, user(2)).unwrap();
    assert_ne!(old, new);
    server.disconnect_subscriber(old);
    assert_eq!(server.open_subscribers(), 1);
}

#[test]
fn a_commit_as_another_identitys_node_is_refused_and_claims_nothing() {
    let server = server();
    let alice = client(1, user(1));
    let (a_client, a_server, a_up, a_down) = pipe();
    let _a = block_on(alice.connect_upstream(a_client));
    server.accept_subscriber(a_server, user(1)).unwrap();
    let (_m_client, m_server, m_up, m_down) = pipe();
    server.accept_subscriber(m_server, user(9)).unwrap();

    post(&alice, "first");
    block_on(alice.tick()).unwrap();
    let Some(SyncMessage::Commit { id, kind, .. }) = a_up
        .borrow()
        .iter()
        .find(|m| matches!(m, SyncMessage::Commit { .. }))
        .cloned()
    else {
        panic!("alice's commit");
    };
    let tx = id.time.0;
    for _ in 0..4 {
        block_on(server.tick()).unwrap();
        block_on(alice.tick()).unwrap();
    }
    assert_eq!(count(&server), 1);

    // Mallory, another user, writes as alice's node at alice's next tx.
    m_up.borrow_mut().push_back(SyncMessage::Commit {
        id: TxId::new(TxTime(tx + 1), id.node),
        kind,
        ops: Vec::new(),
        acting: Acting::Own,
    });
    block_on(server.tick()).unwrap();
    assert_eq!(fates(&m_down), vec![(tx + 1, false)]);

    post(&alice, "second");
    for _ in 0..6 {
        block_on(alice.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    assert!(fates(&a_down).iter().all(|(_, ok)| *ok));
    assert_eq!(count(&server), 2, "alice's second write landed");
}

fn account_of(n: u8) -> jazz::account_registry::AccountId {
    user(n).account_id().expect("a test user has an account")
}

/// A local-first guest of account `n`, as the account registry admits it.
fn guest_of(n: u8) -> AuthorSubject {
    AuthorSubject::reserved(AuthorSubject::LOCAL_FIRST_ISSUER, &format!("key-{n}"))
        .unwrap()
        .with_account(account_of(n))
}

/// The same account signed in through the app's backend.
fn signed_in_as(n: u8, issuer: &str) -> AuthorSubject {
    AuthorSubject::authenticated(issuer, &format!("user-{n}"))
        .unwrap()
        .with_account(account_of(n))
}

/// The first commit alice's store `node` makes as `owner`, landed at the
/// server: its id and kind, from which raw commits as that node are built.
fn owned_node(server: &Db, owner: AuthorSubject) -> (Db, TxId, WireKind) {
    let store = client(1, owner);
    let (c, s, up, _down) = pipe();
    let upstream = block_on(store.connect_upstream(c));
    server.accept_subscriber(s, owner).unwrap();
    post(&store, "first");
    block_on(store.tick()).unwrap();
    let Some(SyncMessage::Commit { id, kind, .. }) = up
        .borrow()
        .iter()
        .find(|m| matches!(m, SyncMessage::Commit { .. }))
        .cloned()
    else {
        panic!("the owner's commit");
    };
    for _ in 0..4 {
        block_on(server.tick()).unwrap();
        block_on(store.tick()).unwrap();
    }
    assert_eq!(count(server), 1);
    drop(upstream);
    (store, id, kind)
}

/// Send a raw commit as `node` at `tx` over a session of `identity`, and
/// return whether it was accepted.
fn commit_as(
    server: &Db,
    identity: AuthorSubject,
    node: NodeUuid,
    tx: u64,
    kind: &WireKind,
) -> bool {
    let (_c, s, up, down) = pipe();
    server.accept_subscriber(s, identity).unwrap();
    up.borrow_mut().push_back(SyncMessage::Commit {
        id: TxId::new(TxTime(tx), node),
        kind: kind.clone(),
        ops: Vec::new(),
        acting: Acting::Own,
    });
    block_on(server.tick()).unwrap();
    let fates = fates(&down);
    assert_eq!(fates.len(), 1, "{fates:?}");
    fates[0] == (tx, true)
}

/// A local-first guest that signs up keeps its account and its store, and
/// so its node: the signed-in principal takes the node over, and from then
/// on the guest's key no longer writes as it.
#[test]
fn a_signed_up_guest_takes_over_its_node_once() {
    let server = server();
    let (_store, id, kind) = owned_node(&server, guest_of(1));
    let tx = id.time.0;

    // Another account's principal may not, and claims nothing by trying.
    assert!(!commit_as(&server, user(9), id.node, tx + 1, &kind));
    // The account's backend principal takes it over.
    assert!(commit_as(
        &server,
        signed_in_as(1, "urn:jazz:backend"),
        id.node,
        tx + 1,
        &kind
    ));
    // The guest's key (still unexpired, or leaked) no longer writes as it,
    // nor does a second backend principal of the account.
    assert!(!commit_as(&server, guest_of(1), id.node, tx + 2, &kind));
    assert!(!commit_as(
        &server,
        signed_in_as(1, "urn:jazz:other"),
        id.node,
        tx + 2,
        &kind
    ));
    assert!(commit_as(
        &server,
        signed_in_as(1, "urn:jazz:backend"),
        id.node,
        tx + 2,
        &kind
    ));
}

/// The hand-over runs one way only, from the account's local-first guest.
#[test]
fn a_node_of_a_signed_in_principal_is_not_handed_over() {
    let server = server();
    let owner = signed_in_as(1, "urn:jazz:backend");
    let (_store, id, kind) = owned_node(&server, owner);
    let tx = id.time.0;

    // Neither the account's guest key, another of its principals, nor a
    // principal with no account writes as the node; none of them claims it.
    assert!(!commit_as(&server, guest_of(1), id.node, tx + 1, &kind));
    assert!(!commit_as(
        &server,
        signed_in_as(1, "urn:jazz:other"),
        id.node,
        tx + 1,
        &kind
    ));
    let accountless = AuthorSubject::authenticated("urn:jazz:backend", "user-1").unwrap();
    assert!(!commit_as(&server, accountless, id.node, tx + 1, &kind));
    assert!(commit_as(&server, owner, id.node, tx + 1, &kind));
}

/// A base on another identity's commit tells its sender nothing: whether
/// the commit exists, was accepted or was rejected, the write is refused
/// alike.
#[test]
fn a_base_on_another_identitys_commit_is_refused_whether_or_not_it_exists() {
    let server = server();
    let write = |node: u8, tx: u64, op: WireWrite, bases: Vec<WireBase>| SyncMessage::Commit {
        id: TxId::new(TxTime(tx), NodeUuid::from_bytes([node; 16])),
        kind: WireKind::Mergeable,
        ops: vec![WireOp { bases, ..op.into() }],
        acting: Acting::Own,
    };
    let body = |text: &str| -> BTreeMap<String, Value> {
        [(
            stored(&server, "messages", "body"),
            Value::String(text.into()),
        )]
        .into()
    };
    let (_a_client, a_server, a_up, a_down) = pipe();
    server.accept_subscriber(a_server, user(1)).unwrap();
    a_up.borrow_mut().push_back(write(
        1,
        1,
        WireWrite::Insert {
            table: "messages".to_owned(),
            row: [7; 16],
            cells: body("alice's"),
            target: WireTarget::Root,
            at: None,
        },
        Vec::new(),
    ));
    block_on(server.tick()).unwrap();
    assert_eq!(fates(&a_down), vec![(1, true)]);

    let (_m_client, m_server, m_up, m_down) = pipe();
    server.accept_subscriber(m_server, user(9)).unwrap();
    for (tx, named) in [(1, 1), (2, 99)] {
        m_up.borrow_mut().push_back(write(
            9,
            tx,
            WireWrite::Update {
                table: "messages".to_owned(),
                row: [7; 16],
                cells: body("mallory's"),
                target: WireTarget::Root,
                at: None,
            },
            vec![WireBase {
                key: Vec::new(),
                on: BuiltOn::Commit {
                    node: [1; 16],
                    tx: named,
                },
            }],
        ));
    }
    block_on(server.tick()).unwrap();
    let refusals: Vec<String> = m_down
        .borrow()
        .iter()
        .filter_map(|m| match m {
            SyncMessage::Fate { outcome, .. } => Some(format!("{:?}", outcome.result())),
            _ => None,
        })
        .collect();
    assert_eq!(refusals.len(), 2, "{refusals:?}");
    assert_eq!(
        refusals[0], refusals[1],
        "the existing commit is not told apart"
    );
    assert!(refusals[0].contains("predecessor_rejected"), "{refusals:?}");
    assert!(rows(&server).contains("alice's"));
}

#[test]
fn an_upsert_over_a_row_deleted_in_the_same_commit_is_checked_as_an_insert() {
    let locked = JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("messages")
                    .column("body", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::False)
                            .with_update(Some(PolicyExpr::False), PolicyExpr::True)
                            .with_delete(PolicyExpr::True),
                    ),
            )
            .build(),
    )
    .unwrap();
    let cfg = |node: u8, author| {
        DbConfig::new(
            locked.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([node; 16]),
                author,
            },
        )
    };
    let server = block_on(Db::open_history_complete(cfg(0, AuthorSubject::SYSTEM))).unwrap();
    let row = RowUuid::from_bytes([7; 16]);
    server
        .seed_settled_mergeable_for_bootstrap(
            "messages",
            row,
            AuthorSubject::SYSTEM,
            [("body".to_owned(), Value::String("original".into()))].into(),
        )
        .unwrap();
    let (_m_client, m_server, m_up, m_down) = pipe();
    server.accept_subscriber(m_server, user(1)).unwrap();
    m_up.borrow_mut().push_back(SyncMessage::Commit {
        id: TxId::new(TxTime(1), NodeUuid::from_bytes([1; 16])),
        kind: WireKind::Mergeable,
        ops: vec![
            WireWrite::Delete {
                table: "messages".to_owned(),
                row: [7; 16],
                target: WireTarget::Root,
                at: None,
            }
            .into(),
            WireWrite::Upsert {
                table: "messages".to_owned(),
                row: [7; 16],
                cells: [(
                    stored(&server, "messages", "body"),
                    Value::String("rewritten".into()),
                )]
                .into(),
                created: Default::default(),
                target: WireTarget::Root,
                at: None,
            }
            .into(),
        ],
        acting: Acting::Own,
    });
    block_on(server.tick()).unwrap();
    assert_eq!(fates(&m_down), vec![(1, false)]);
    assert!(rows(&server).contains("original"), "{}", rows(&server));
}

#[test]
fn the_authority_refuses_cells_that_are_not_of_their_columns_type() {
    let server = server();
    let (_m_client, m_server, m_up, m_down) = pipe();
    server.accept_subscriber(m_server, user(2)).unwrap();
    for (i, value) in [Value::I64(5), Value::Nullable(None), Value::Bool(true)]
        .into_iter()
        .enumerate()
    {
        m_up.borrow_mut().push_back(SyncMessage::Commit {
            id: TxId::new(TxTime(1 + i as u64), NodeUuid::from_bytes([2; 16])),
            kind: WireKind::Mergeable,
            ops: vec![
                WireWrite::Insert {
                    table: "messages".to_owned(),
                    row: [10 + i as u8; 16],
                    cells: [(stored(&server, "messages", "body"), value)].into(),
                    target: WireTarget::Root,
                    at: None,
                }
                .into(),
            ],
            acting: Acting::Own,
        });
    }
    block_on(server.tick()).unwrap();
    assert_eq!(count(&server), 0);
    assert!(fates(&m_down).iter().all(|(_, ok)| !ok));
    assert_eq!(fates(&m_down).len(), 3);
}

#[test]
fn a_local_write_of_the_wrong_type_fails() {
    let alice = client(1, user(1));
    let cells: BTreeMap<String, Value> = [("body".to_owned(), Value::I64(5))].into();
    assert!(block_on(alice.insert("messages", cells, InsertOptions::default())).is_err());
}

#[test]
fn a_reopened_authority_continues_after_the_seq_of_a_rowless_commit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("server.db");
    let open = || {
        let db = block_on(Db::open_at(
            config(0, AuthorSubject::SYSTEM),
            &path,
            FileOptions::default(),
        ))
        .unwrap();
        db.set_authority();
        db
    };
    let server = open();
    let (_r, r_server, r_up, r_down) = pipe();
    server.accept_subscriber(r_server, user(2)).unwrap();
    r_up.borrow_mut().push_back(SyncMessage::Commit {
        id: TxId::new(TxTime(1), NodeUuid::from_bytes([2; 16])),
        kind: WireKind::Mergeable,
        ops: Vec::new(),
        acting: Acting::Own,
    });
    block_on(server.tick()).unwrap();
    assert_eq!(fates(&r_down), vec![(1, true)]);
    let before = server.seq_high_water();
    block_on(server.close()).unwrap();
    drop(server);
    assert_eq!(open().seq_high_water(), before);
}

#[test]
fn a_reopened_replica_continues_after_a_pending_commit_that_left_no_rows() {
    let server = server();
    let row = RowUuid::from_bytes([7; 16]);
    server
        .seed_settled_mergeable_for_bootstrap(
            "messages",
            row,
            AuthorSubject::SYSTEM,
            [("body".to_owned(), Value::String("held elsewhere".into()))].into(),
        )
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alice.db");
    let open_alice = || {
        block_on(Db::open_at(
            config(1, user(1)),
            &path,
            FileOptions::default(),
        ))
        .unwrap()
    };
    let alice = open_alice();
    let (a_client, a_server, _, _) = pipe();
    let up = block_on(alice.connect_upstream(a_client));
    let session = server.accept_subscriber(a_server, user(1)).unwrap();
    // A delete of a row this replica does not hold writes no local row.
    block_on(alice.delete("messages", row, DeleteOptions::default())).unwrap();
    let before = alice.seq_high_water();
    block_on(alice.tick()).unwrap();
    block_on(server.tick()).unwrap();
    // The replica goes away before the fate arrives.
    alice.detach_connection(&up);
    server.disconnect_subscriber(session);
    drop(alice);

    let alice = open_alice();
    assert!(alice.seq_high_water() >= before);
    post(&alice, "after reopen");
    let (a_client, a_server, _, _) = pipe();
    let _up = block_on(alice.connect_upstream(a_client));
    server.accept_subscriber(a_server, user(1)).unwrap();
    for _ in 0..6 {
        block_on(alice.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    assert!(rows(&server).contains("after reopen"), "{}", rows(&server));
}

/// A base travels on the commit's first op that writes its row, one per row
/// and branch key: anything else refuses the commit (SPEC 8.2).
#[test]
fn the_authority_refuses_misplaced_bases() {
    let server = server();
    let row = RowUuid::from_bytes([7; 16]);
    server
        .seed_settled_mergeable_for_bootstrap(
            "messages",
            row,
            AuthorSubject::SYSTEM,
            [("body".to_owned(), Value::String("original".into()))].into(),
        )
        .unwrap();
    let (_m_client, m_server, m_up, m_down) = pipe();
    server.accept_subscriber(m_server, user(1)).unwrap();
    let base = || WireBase {
        key: Vec::new(),
        on: BuiltOn::Seq(1),
    };
    let update = |body: &str, bases: Vec<WireBase>| WireOp {
        bases,
        ..WireWrite::Update {
            table: "messages".to_owned(),
            row: [7; 16],
            cells: [(
                stored(&server, "messages", "body"),
                Value::String(body.into()),
            )]
            .into(),
            target: WireTarget::Root,
            at: None,
        }
        .into()
    };
    let commit = |n: u64, ops: Vec<WireOp>| SyncMessage::Commit {
        id: TxId::new(TxTime(n), NodeUuid::from_bytes([1; 16])),
        kind: WireKind::Mergeable,
        ops,
        acting: Acting::Own,
    };
    // On the row's second op; twice on its first; then as SPEC 8.2 says.
    m_up.borrow_mut().push_back(commit(
        1,
        vec![update("a", Vec::new()), update("b", vec![base()])],
    ));
    m_up.borrow_mut()
        .push_back(commit(2, vec![update("c", vec![base(), base()])]));
    m_up.borrow_mut().push_back(commit(
        3,
        vec![update("d", vec![base()]), update("e", Vec::new())],
    ));
    block_on(server.tick()).unwrap();
    let refusals: Vec<Option<String>> = m_down
        .borrow()
        .iter()
        .filter_map(|m| match m {
            SyncMessage::Fate { outcome, .. } => {
                Some(outcome.result().err().map(|e| format!("{e:?}")))
            }
            _ => None,
        })
        .collect();
    assert_eq!(refusals.len(), 3, "{refusals:?}");
    assert!(
        refusals[0]
            .as_deref()
            .is_some_and(|e| e.contains("first op that writes its row")),
        "{refusals:?}"
    );
    assert!(
        refusals[1]
            .as_deref()
            .is_some_and(|e| e.contains("another base")),
        "{refusals:?}"
    );
    assert_eq!(refusals[2], None, "{refusals:?}");
    assert!(rows(&server).contains("\"e\""), "{}", rows(&server));
}

/// The subjects of who created each `messages` row, by body.
fn creators(db: &Db) -> Vec<(String, String)> {
    let prepared = db
        .prepare_query(&Query::from("messages").select(["body", "$createdBy.identity.subject"]))
        .unwrap();
    let text = |cell: Option<&Value>| match cell {
        Some(Value::String(v)) => v.clone(),
        Some(Value::Nullable(Some(v))) => match &**v {
            Value::String(v) => v.clone(),
            other => panic!("{other:?}"),
        },
        other => panic!("{other:?}"),
    };
    let mut rows: Vec<_> = block_on(db.all(&prepared, ReadOpts::default()))
        .unwrap()
        .iter()
        .map(|row| {
            let cell = |name: &str| row.cells().find(|(n, _)| *n == name).unwrap().1;
            (
                text(cell("body")),
                text(cell("$createdBy.identity.subject")),
            )
        })
        .collect();
    rows.sort();
    rows
}

#[test]
fn a_session_link_may_not_attribute_and_its_session_writes_are_its_own() {
    let server = server();
    let (_m_client, m_server, m_up, m_down) = pipe();
    server.accept_subscriber(m_server, user(2)).unwrap();
    let insert = |tx: u64, row: u8, body: &str, acting: Acting| SyncMessage::Commit {
        id: TxId::new(TxTime(tx), NodeUuid::from_bytes([2; 16])),
        kind: WireKind::Mergeable,
        ops: vec![
            WireWrite::Insert {
                table: "messages".to_owned(),
                row: [row; 16],
                cells: [(
                    stored(&server, "messages", "body"),
                    Value::String(body.to_owned()),
                )]
                .into(),
                target: WireTarget::Root,
                at: None,
            }
            .into(),
        ],
        acting,
    };
    let other = user(3).canonical().to_owned();
    m_up.borrow_mut().extend([
        insert(
            1,
            1,
            "attributed",
            Acting::Attributed {
                author: other.clone(),
            },
        ),
        insert(
            2,
            2,
            "as a session",
            Acting::Session {
                author: other,
                claims: BTreeMap::new(),
            },
        ),
    ]);
    block_on(server.tick()).unwrap();
    assert_eq!(fates(&m_down), vec![(1, false), (2, true)]);
    let (_, link) = user(2).principal_parts();
    assert_eq!(creators(&server), vec![("as a session".to_owned(), link)]);
}

/// A raw session's read of `messages`, over `span`.
fn read_messages(id: u64, span: ReadSpan, since: Vec<(Option<[u8; 16]>, u64)>) -> SyncMessage {
    SyncMessage::Read {
        id,
        query: Box::new(Query::from("messages")),
        binding: BTreeMap::new(),
        include_deleted: false,
        tier: ReadTier::Remote,
        wait: None,
        view: Box::new(ReadViewSourceSpec::Current),
        since,
        span,
    }
}

/// What the server sent read `id`: `Rows` it shipped (any read's) after
/// its first `Settled`/`Refused` for `id`, and that answer.
fn answers(down: &Q, id: u64) -> Vec<&'static str> {
    down.borrow()
        .iter()
        .filter_map(|m| match m {
            SyncMessage::Settled { id: at, .. } if *at == id => Some("settled"),
            SyncMessage::Refused { id: at, .. } if *at == id => Some("refused"),
            SyncMessage::Rows { images, .. } if !images.is_empty() => Some("rows"),
            _ => None,
        })
        .collect()
}

/// Positions across more than one sequencer are not supported yet: a read
/// naming them is refused before it is set up, so it holds nothing open
/// and later writes do not reach it.
#[test]
fn a_read_naming_several_sequencers_is_refused_and_holds_nothing_open() {
    let server = server();
    post(&server, "before");
    for (id, span, since) in [
        (1, ReadSpan::Live, vec![(None, 1), (Some([7; 16]), 1)]),
        (
            2,
            ReadSpan::Once {
                at: vec![(None, 1), (Some([7; 16]), 1)],
            },
            Vec::new(),
        ),
        (
            3,
            ReadSpan::Once {
                at: vec![(Some([7; 16]), 1)],
            },
            Vec::new(),
        ),
    ] {
        let (_client, session, up, down) = pipe();
        server.accept_subscriber(session, user(1)).unwrap();
        up.borrow_mut().push_back(read_messages(id, span, since));
        block_on(server.tick()).unwrap();
        assert_eq!(answers(&down, id), ["refused"], "read {id}");
        down.borrow_mut().clear();
        post(&server, "after");
        block_on(server.tick()).unwrap();
        assert_eq!(
            answers(&down, id),
            Vec::<&str>::new(),
            "read {id} stays closed"
        );
    }
}

/// A live read resuming from a position at another sequencer than the one
/// the server serves at opens fresh: its opening ships every row.
#[test]
fn a_read_resuming_at_another_sequencer_opens_fresh() {
    let server = server();
    post(&server, "one");
    post(&server, "two");
    let (_client, session, up, down) = pipe();
    server.accept_subscriber(session, user(1)).unwrap();
    up.borrow_mut().push_back(read_messages(
        1,
        ReadSpan::Live,
        vec![(Some([7; 16]), 1_000)],
    ));
    block_on(server.tick()).unwrap();
    let shipped: usize = down
        .borrow()
        .iter()
        .map(|m| match m {
            SyncMessage::Rows { images, .. } => images.len(),
            _ => 0,
        })
        .sum();
    assert_eq!(shipped, 2, "both rows ship: {:?}", down.borrow());
    assert_eq!(answers(&down, 1).last(), Some(&"settled"));
}
