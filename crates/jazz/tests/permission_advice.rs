//! Permission advice: a replica asks its upstream authority whether an
//! operation would be allowed for its session. The authority dry-runs the
//! operation under the session's policies and rolls it back; without an
//! authority the answer is `Unknown`, never a local guess.

use crate::common;

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use jazz::db::duplex;
use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol::{PermissionAdvice, PermissionAdviceAction};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    CmpOp, ColumnType, PolicyExpr, PolicyValue, SchemaBuilder, TablePolicies, TableSchemaBuilder,
};

use common::compile_schema;

fn owned_by_session() -> PolicyExpr {
    PolicyExpr::Cmp {
        column: "owner".to_owned(),
        op: CmpOp::Eq,
        value: PolicyValue::SessionRef(vec![
            "user".to_owned(),
            "identity".to_owned(),
            "subject".to_owned(),
        ]),
    }
}

fn schema() -> JazzSchema {
    let owned = owned_by_session();
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("owner", ColumnType::Text)
                    .column("title", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(owned.clone())
                            .with_insert(owned.clone())
                            .with_update(Some(owned.clone()), owned.clone())
                            .with_delete(owned),
                    ),
            )
            .table(
                TableSchemaBuilder::new("pinned")
                    .column("title", ColumnType::Text)
                    .policies(TablePolicies::new().with_insert(PolicyExpr::Exists {
                        table: "notes".to_owned(),
                        condition: Box::new(owned_by_session()),
                    })),
            )
            .build(),
    )
}

fn open(node: u8, author: AuthorSubject) -> Db {
    block_on(Db::open(DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )))
    .unwrap()
}

fn subject(author: AuthorSubject) -> Value {
    Value::String(author.principal_parts().1)
}

fn note(owner: AuthorSubject, title: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("owner".to_owned(), subject(owner)),
        ("title".to_owned(), Value::String(title.to_owned())),
    ])
}

fn drive<T>(dbs: &[&Db], future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    for _ in 0..100 {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
    }
    panic!("did not complete");
}

fn ask(client: &Db, server: &Db, action: PermissionAdviceAction) -> PermissionAdvice {
    let id = client.request_permission_advice(action, None);
    drive(&[client, server, client], client.permission_advice(id))
}

/// A client transport that records the ids of the advice requests it
/// sends.
struct RecordingAdvice {
    inner: Box<dyn jazz::db::Transport>,
    sent: std::rc::Rc<std::cell::RefCell<Vec<u64>>>,
}

impl jazz::db::Transport for RecordingAdvice {
    fn send(&mut self, message: jazz::db::SyncMessage) -> Result<(), jazz::wire::TransportError> {
        if let jazz::db::SyncMessage::Advise { id, .. } = &message {
            self.sent.borrow_mut().push(*id);
        }
        self.inner.send(message)
    }

    fn try_recv(&mut self) -> Option<jazz::db::SyncMessage> {
        self.inner.try_recv()
    }
}

/// [`connect`], returning the ids of the advice requests the client sends.
fn connect_recording(
    client: &Db,
    server: &Db,
    author: AuthorSubject,
) -> std::rc::Rc<std::cell::RefCell<Vec<u64>>> {
    let (client_side, server_side) = duplex();
    let sent = std::rc::Rc::default();
    block_on(client.connect_upstream(Box::new(RecordingAdvice {
        inner: client_side,
        sent: std::rc::Rc::clone(&sent),
    })));
    server.accept_subscriber(server_side, author);
    sent
}

fn connect(client: &Db, server: &Db, author: AuthorSubject) {
    let (client_side, server_side) = duplex();
    block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, author);
}

fn count(db: &Db, table: &str) -> usize {
    let prepared = db.prepare_query(&Query::from(table)).unwrap();
    block_on(db.all(&prepared, ReadOpts::default()))
        .unwrap()
        .len()
}

#[test]
fn the_authority_advises_each_action_under_the_sessions_policies() {
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb0; 16]);
    let server = block_on(Db::open_history_complete(DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([0xee; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    let row = RowUuid::from_bytes([0x11; 16]);
    block_on(server.insert(
        "notes",
        note(alice, "mine"),
        InsertOptions {
            row_id: Some(row),
            ..Default::default()
        },
    ))
    .unwrap();
    let alice_db = open(0xa1, alice);
    let bob_db = open(0xb0, bob);
    connect(&alice_db, &server, alice);
    connect(&bob_db, &server, bob);

    use PermissionAdvice::{Allowed, Denied};
    let insert = |owner| PermissionAdviceAction::Insert {
        table: "notes".to_owned(),
        cells: note(owner, "new"),
    };
    let read = || PermissionAdviceAction::Read {
        table: "notes".to_owned(),
        row,
    };
    let update = || PermissionAdviceAction::Update {
        table: "notes".to_owned(),
        row,
        patch: BTreeMap::from([("title".to_owned(), Value::String("renamed".to_owned()))]),
    };
    let delete = || PermissionAdviceAction::Delete {
        table: "notes".to_owned(),
        row,
    };
    assert_eq!(ask(&alice_db, &server, insert(alice)), Allowed);
    assert_eq!(ask(&alice_db, &server, insert(bob)), Denied);
    assert_eq!(ask(&alice_db, &server, read()), Allowed);
    assert_eq!(ask(&alice_db, &server, update()), Allowed);
    assert_eq!(ask(&alice_db, &server, delete()), Allowed);
    assert_eq!(ask(&bob_db, &server, read()), Denied);
    assert_eq!(ask(&bob_db, &server, update()), Denied);
    assert_eq!(ask(&bob_db, &server, delete()), Denied);
    // An insert policy that reads other rows is decided for a fresh row:
    // alice owns a note, bob none.
    let pinned = PermissionAdviceAction::Insert {
        table: "pinned".to_owned(),
        cells: BTreeMap::from([("title".to_owned(), Value::String("p".to_owned()))]),
    };
    assert_eq!(ask(&alice_db, &server, pinned.clone()), Allowed);
    assert_eq!(ask(&bob_db, &server, pinned), Denied);

    // Advice never writes.
    assert_eq!(count(&server, "notes"), 1);
    let prepared = server.prepare_query(&Query::from("notes")).unwrap();
    let rows = block_on(server.all(&prepared, ReadOpts::default())).unwrap();
    assert_eq!(
        rows[0]
            .cells()
            .find(|(name, _)| *name == "title")
            .unwrap()
            .1,
        Some(&Value::String("mine".to_owned()))
    );
}

#[test]
fn without_an_authority_or_once_cancelled_the_advice_is_unknown() {
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let action = || PermissionAdviceAction::Insert {
        table: "notes".to_owned(),
        cells: note(alice, "new"),
    };
    // Never connected: no authority to ask, and no local guess.
    let alone = open(0xa1, alice);
    let id = alone.request_permission_advice(action(), None);
    assert_eq!(
        block_on(alone.permission_advice(id)),
        PermissionAdvice::Unknown
    );

    let server = block_on(Db::open_history_complete(DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([0xee; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    let client = open(0xa2, alice);
    connect(&client, &server, alice);
    let id = client.request_permission_advice(action(), None);
    client.cancel_permission_advice_request(id);
    // The late answer is dropped.
    assert_eq!(
        drive(&[&client, &server, &client], client.permission_advice(id)),
        PermissionAdvice::Unknown
    );
}

#[test]
fn a_trusted_backend_asks_for_the_sessions_it_acts_for() {
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb0; 16]);
    let server = block_on(Db::open_history_complete(DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([0xee; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    let row = RowUuid::from_bytes([0x11; 16]);
    block_on(server.insert(
        "notes",
        note(alice, "mine"),
        InsertOptions {
            row_id: Some(row),
            ..Default::default()
        },
    ))
    .unwrap();
    let backend = open(0xbe, AuthorSubject::SYSTEM);
    connect(&backend, &server, AuthorSubject::SYSTEM);
    let alice_db = open(0xa1, alice);
    connect(&alice_db, &server, alice);
    let update = || PermissionAdviceAction::Update {
        table: "notes".to_owned(),
        row,
        patch: BTreeMap::from([("title".to_owned(), Value::String("renamed".to_owned()))]),
    };
    let ask_as = |client: &Db, author: AuthorSubject| {
        let id = client.request_permission_advice(update(), Some((author, BTreeMap::new())));
        drive(&[client, &server, client], client.permission_advice(id))
    };
    assert_eq!(ask_as(&backend, alice), PermissionAdvice::Allowed);
    assert_eq!(ask_as(&backend, bob), PermissionAdvice::Denied);
    // A session link cannot ask as another session: it is answered as
    // itself.
    assert_eq!(ask_as(&alice_db, bob), PermissionAdvice::Allowed);
}

/// Each request has a fresh id, so an answer only ever resolves the request
/// it was asked for: a cancelled request's waiter is gone (it reads
/// `Unknown` at once), its late answer resolves nothing, and a reopened
/// database's ids never name another instance's requests.
///
/// ```text
/// alice ──advise#a(insert as bob)──► core      (sent, then cancelled)
/// alice ──advise#b(insert as alice)─► core ──► Allowed for #b only
/// ```
#[test]
fn an_answer_resolves_only_the_request_it_was_asked_for() {
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb0; 16]);
    let server = block_on(Db::open_history_complete(DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([0xee; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    let client = open(0xa1, alice);
    let sent = connect_recording(&client, &server, alice);
    let insert = |owner| PermissionAdviceAction::Insert {
        table: "notes".to_owned(),
        cells: note(owner, "new"),
    };

    let cancelled = client.request_permission_advice(insert(bob), None);
    block_on(client.tick()).unwrap();
    client.cancel_permission_advice_request(cancelled);
    assert_eq!(
        block_on(client.permission_advice(cancelled)),
        PermissionAdvice::Unknown,
        "a cancelled request's waiter is gone"
    );
    let asked = client.request_permission_advice(insert(alice), None);
    assert_ne!(asked, cancelled);
    assert_eq!(
        drive(
            &[&client, &server, &client],
            client.permission_advice(asked)
        ),
        PermissionAdvice::Allowed
    );
    assert_eq!(
        block_on(client.permission_advice(cancelled)),
        PermissionAdvice::Unknown,
        "the late answer resolved nothing"
    );

    // A request cancelled before it is sent is never asked: the core
    // answers only the request after it.
    let unsent = client.request_permission_advice(insert(bob), None);
    client.cancel_permission_advice_request(unsent);
    let next = client.request_permission_advice(insert(alice), None);
    assert_eq!(
        drive(&[&client, &server, &client], client.permission_advice(next)),
        PermissionAdvice::Allowed
    );
    assert!(sent.borrow().contains(&next));
    assert!(
        !sent.borrow().contains(&unsent),
        "a request cancelled before it was sent never goes out"
    );

    // A reopened database starts its ids elsewhere, so an id held over
    // from the earlier instance names nothing in the new one.
    let reopened = open(0xa1, alice);
    connect(&reopened, &server, alice);
    let fresh = reopened.request_permission_advice(insert(alice), None);
    assert!(![cancelled, asked, unsent, next].contains(&fresh));
    assert_eq!(
        block_on(reopened.permission_advice(asked)),
        PermissionAdvice::Unknown
    );
    assert_eq!(
        drive(
            &[&reopened, &server, &reopened],
            reopened.permission_advice(fresh)
        ),
        PermissionAdvice::Allowed
    );
}

/// Advice is a bare verdict: it discloses no rows, no policy reasons and
/// no hidden facts. Asking to change a row the session cannot see is
/// `Denied` exactly as for a row that does not exist.
///
/// ```text
/// bob ──advise(update alice's hidden note)──► core ──► Denied
/// bob ──advise(update a missing note)───────► core ──► Denied
/// ```
#[test]
fn advice_about_a_hidden_row_tells_nothing_about_it() {
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb0; 16]);
    let server = block_on(Db::open_history_complete(DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([0xee; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    let hidden = RowUuid::from_bytes([0x11; 16]);
    let missing = RowUuid::from_bytes([0x12; 16]);
    block_on(server.insert(
        "notes",
        note(alice, "mine"),
        InsertOptions {
            row_id: Some(hidden),
            ..Default::default()
        },
    ))
    .unwrap();
    let bob_db = open(0xb0, bob);
    connect(&bob_db, &server, bob);
    for row in [hidden, missing] {
        let update = PermissionAdviceAction::Update {
            table: "notes".to_owned(),
            row,
            patch: BTreeMap::from([("title".to_owned(), Value::String("x".to_owned()))]),
        };
        let delete = PermissionAdviceAction::Delete {
            table: "notes".to_owned(),
            row,
        };
        let read = PermissionAdviceAction::Read {
            table: "notes".to_owned(),
            row,
        };
        for action in [update, delete, read] {
            assert_eq!(ask(&bob_db, &server, action), PermissionAdvice::Denied);
        }
    }
    assert_eq!(count(&server, "notes"), 1);
}
