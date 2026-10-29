//! What a server accepts from the clients it serves, and what a store
//! resumes after a reopen: a client is not trusted to name another's node,
//! to send only messages the server serves, to write under policies a
//! sequence of its own writes would skip, or to send cells of other types.

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use jazz::db::{
    Acting, Db, DbConfig, DbIdentity, DeleteOptions, FileOptions, InsertOptions, LOCAL_BASE,
    ReadOpts, ReadTier, SyncMessage, Transport, TransportError, WireKind, WireOp, WireTarget,
    block_on,
};
use jazz::ids::RowUuid;
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};

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
    block_on(Db::open_history_complete(config(0, AuthorSubject::SYSTEM))).unwrap()
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
            SyncMessage::Fate { tx, result, .. } => Some((*tx, result.is_ok())),
            _ => None,
        })
        .collect()
}

#[test]
fn a_message_the_server_does_not_serve_ends_only_that_session() {
    let server = server();
    let (_bad_client, bad_server, bad_up, _) = pipe();
    let bad = server.accept_subscriber(bad_server, user(9));
    let alice = client(1, user(1));
    let (a_client, a_server, _, _) = pipe();
    let _a = block_on(alice.connect_upstream(a_client));
    server.accept_subscriber(a_server, user(1));

    post(&alice, "hello");
    bad_up.borrow_mut().push_back(SyncMessage::Rows {
        images: Vec::new(),
        origins: Vec::new(),
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

#[test]
fn a_stale_subscriber_handle_does_not_close_the_session_reusing_its_slot() {
    let server = server();
    let (_old_client, old_server, _, _) = pipe();
    let old = server.accept_subscriber(old_server, user(1));
    server.disconnect_subscriber(old);
    let (_new_client, new_server, _, _) = pipe();
    let new = server.accept_subscriber(new_server, user(2));
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
    server.accept_subscriber(a_server, user(1));
    let (_m_client, m_server, m_up, m_down) = pipe();
    server.accept_subscriber(m_server, user(9));

    post(&alice, "first");
    block_on(alice.tick()).unwrap();
    let Some(SyncMessage::Commit { node, tx, kind, .. }) = a_up
        .borrow()
        .iter()
        .find(|m| matches!(m, SyncMessage::Commit { .. }))
        .cloned()
    else {
        panic!("alice's commit");
    };
    for _ in 0..4 {
        block_on(server.tick()).unwrap();
        block_on(alice.tick()).unwrap();
    }
    assert_eq!(count(&server), 1);

    // Mallory, another user, writes as alice's node at alice's next tx.
    m_up.borrow_mut().push_back(SyncMessage::Commit {
        node,
        tx: tx + 1,
        kind,
        ops: Vec::new(),
        acting: Acting::Own,
        expect: Vec::new(),
        deltas: Vec::new(),
        sources: Vec::new(),
        bases: Vec::new(),
        origin: None,
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
    server.accept_subscriber(m_server, user(1));
    m_up.borrow_mut().push_back(SyncMessage::Commit {
        node: [1; 16],
        tx: LOCAL_BASE + 1,
        kind: WireKind::Mergeable,
        ops: vec![
            WireOp::Delete {
                table: "messages".to_owned(),
                row: [7; 16],
                target: WireTarget::Root,
                at: None,
            },
            WireOp::Upsert {
                table: "messages".to_owned(),
                row: [7; 16],
                cells: [("body".to_owned(), Value::String("rewritten".into()))].into(),
                target: WireTarget::Root,
                at: None,
            },
        ],
        acting: Acting::Own,
        expect: Vec::new(),
        deltas: Vec::new(),
        sources: Vec::new(),
        bases: Vec::new(),
        origin: None,
    });
    block_on(server.tick()).unwrap();
    assert_eq!(fates(&m_down), vec![(LOCAL_BASE + 1, false)]);
    assert!(rows(&server).contains("original"), "{}", rows(&server));
}

#[test]
fn the_authority_refuses_cells_that_are_not_of_their_columns_type() {
    let server = server();
    let (_m_client, m_server, m_up, m_down) = pipe();
    server.accept_subscriber(m_server, user(2));
    for (i, value) in [Value::I64(5), Value::Nullable(None), Value::Bool(true)]
        .into_iter()
        .enumerate()
    {
        m_up.borrow_mut().push_back(SyncMessage::Commit {
            node: [2; 16],
            tx: LOCAL_BASE + 1 + i as u64,
            kind: WireKind::Mergeable,
            ops: vec![WireOp::Insert {
                table: "messages".to_owned(),
                row: [10 + i as u8; 16],
                cells: [("body".to_owned(), value)].into(),
                target: WireTarget::Root,
                at: None,
            }],
            acting: Acting::Own,
            expect: Vec::new(),
            deltas: Vec::new(),
            sources: Vec::new(),
            bases: Vec::new(),
            origin: None,
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
    server.accept_subscriber(r_server, user(2));
    r_up.borrow_mut().push_back(SyncMessage::Commit {
        node: [2; 16],
        tx: LOCAL_BASE + 1,
        kind: WireKind::Mergeable,
        ops: Vec::new(),
        acting: Acting::Own,
        expect: Vec::new(),
        deltas: Vec::new(),
        sources: Vec::new(),
        bases: Vec::new(),
        origin: None,
    });
    block_on(server.tick()).unwrap();
    assert_eq!(fates(&r_down), vec![(LOCAL_BASE + 1, true)]);
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
    let session = server.accept_subscriber(a_server, user(1));
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
    server.accept_subscriber(a_server, user(1));
    for _ in 0..6 {
        block_on(alice.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    assert!(rows(&server).contains("after reopen"), "{}", rows(&server));
}
