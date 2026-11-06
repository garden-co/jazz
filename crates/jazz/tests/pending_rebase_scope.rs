//! A replica shows its pending commits over its upstream's rows: when a
//! row's upstream version or the commits pending on it change, it restores
//! the row and replays those commits over it. It replays only the commits
//! that write a changed row, and with them every row they link: a commit
//! writing a changed row and an unchanged one replays on both, and so do
//! the commits on that other row. A row nothing changed comes out of a
//! replay as it went in, so it is left as it is: settling a commit costs
//! the commits sharing its rows, not every commit still pending.

use crate::common;

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, SyncMessage, Transport, TransportError, Upstream,
    block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::model::transaction::OpenTransactionId;
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

fn open(node: u8, author: AuthorSubject) -> Db {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("docs")
                    .column("title", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    let identity = DbIdentity {
        node: NodeUuid::from_bytes([node; 16]),
        author,
    };
    block_on(Db::open(DbConfig::new(schema, identity))).unwrap()
}

fn user(n: u8) -> AuthorSubject {
    AuthorSubject::for_test_bytes([n; 16])
}

fn row(n: u16) -> RowUuid {
    let mut bytes = [0xab; 16];
    bytes[..2].copy_from_slice(&n.to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn title(title: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("title".to_owned(), Value::String(title.to_owned()))])
}

fn insert(db: &Db, n: u16, text: &str) {
    block_on(db.insert(
        "docs",
        title(text),
        InsertOptions {
            row_id: Some(row(n)),
            ..Default::default()
        },
    ))
    .unwrap();
}

fn update(db: &Db, n: u16, text: &str) {
    block_on(db.update("docs", row(n), title(text), Default::default())).unwrap();
}

/// One commit: each insert, then each update.
fn commit(db: &Db, inserts: &[(u16, &str)], updates: &[(u16, &str)]) {
    let tx = OpenTransactionId::new();
    block_on(db.begin_mergeable(tx)).unwrap();
    for &(n, text) in inserts {
        block_on(db.tx_ref(tx).insert(
            "docs",
            title(text),
            InsertOptions {
                row_id: Some(row(n)),
                ..Default::default()
            },
        ))
        .unwrap();
    }
    for &(n, text) in updates {
        block_on(
            db.tx_ref(tx)
                .update("docs", row(n), title(text), Default::default()),
        )
        .unwrap();
    }
    block_on(db.commit_mergeable_handle(tx)).unwrap();
}

/// Every row's title, as `db` shows it now.
fn titles(db: &Db) -> BTreeMap<RowUuid, String> {
    let prepared = db.prepare_query(&Query::from("docs")).unwrap();
    db.read(&prepared)
        .unwrap()
        .iter()
        .map(|r| match r.cell("title") {
            Some(Value::String(title)) => (r.row_uuid(), title),
            other => panic!("title {other:?}"),
        })
        .collect()
}

fn expect(pairs: &[(u16, &str)]) -> BTreeMap<RowUuid, String> {
    pairs
        .iter()
        .map(|&(n, text)| (row(n), text.to_owned()))
        .collect()
}

type Queue = Rc<RefCell<VecDeque<SyncMessage>>>;

/// The server's side of a link: fates wait in `held` while held.
struct Down {
    to_client: Queue,
    from_client: Queue,
    holding: Rc<RefCell<bool>>,
    held: Queue,
}

impl Transport for Down {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        if *self.holding.borrow() && matches!(message, SyncMessage::Fate { .. }) {
            self.held.borrow_mut().push_back(message);
        } else {
            self.to_client.borrow_mut().push_back(message);
        }
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.from_client.borrow_mut().pop_front()
    }
}

struct Up {
    to_server: Queue,
    from_server: Queue,
}

impl Transport for Up {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.to_server.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.from_server.borrow_mut().pop_front()
    }
}

struct Link {
    _upstream: Upstream,
    to_client: Queue,
    holding: Rc<RefCell<bool>>,
    held: Queue,
}

impl Link {
    fn hold_fates(&self) {
        *self.holding.borrow_mut() = true;
    }

    fn held(&self) -> usize {
        self.held.borrow().len()
    }

    /// Let the oldest held fate through, and have the client take it.
    fn release_one(&self, client: &Db) {
        let fate = self.held.borrow_mut().pop_front().expect("a held fate");
        self.to_client.borrow_mut().push_back(fate);
        block_on(client.tick()).unwrap();
    }
}

fn connect(server: &Db, client: &Db, identity: AuthorSubject) -> Link {
    let up: Queue = Rc::default();
    let down: Queue = Rc::default();
    let holding = Rc::new(RefCell::new(false));
    let held: Queue = Rc::default();
    let upstream = block_on(client.connect_upstream(Box::new(Up {
        to_server: up.clone(),
        from_server: down.clone(),
    })));
    server
        .accept_subscriber(
            Box::new(Down {
                to_client: down.clone(),
                from_client: up,
                holding: holding.clone(),
                held: held.clone(),
            }),
            identity,
        )
        .unwrap();
    Link {
        _upstream: upstream,
        to_client: down,
        holding,
        held,
    }
}

fn pump(server: &Db, clients: &[&Db]) {
    for _ in 0..6 {
        for client in clients {
            block_on(client.tick()).unwrap();
        }
        block_on(server.tick()).unwrap();
    }
    for client in clients {
        block_on(client.tick()).unwrap();
    }
}

fn subscribe(db: &Db) -> jazz::db::SubscriptionStream {
    let prepared = db.prepare_query(&Query::from("docs")).unwrap();
    block_on(db.subscribe(&prepared, Default::default())).unwrap()
}

/// Writes on rows no other pending commit writes settle one fate at a
/// time without replaying the rest: before, each fate replayed every
/// commit still pending, so settling a bulk insert took time quadratic in
/// its size (a backend's 20k inserts outlasted a read's wait for them).
#[test]
fn settling_a_commit_replays_only_the_commits_sharing_its_rows() {
    const WRITES: u16 = 200;
    let server = open(0, AuthorSubject::SYSTEM);
    let client = open(1, user(1));
    let link = connect(&server, &client, user(1));
    pump(&server, &[&client]);
    link.hold_fates();
    for n in 0..WRITES {
        insert(&client, n, "mine");
    }
    pump(&server, &[&client]);
    assert_eq!(link.held(), WRITES as usize);
    while link.held() > 0 {
        link.release_one(&client);
    }
    let replayed = client.replayed_commits();
    assert!(
        replayed < u64::from(WRITES),
        "settling {WRITES} unrelated commits replayed {replayed} commits"
    );
    let mine: Vec<(u16, &str)> = (0..WRITES).map(|n| (n, "mine")).collect();
    assert_eq!(titles(&client), expect(&mine));
    assert_eq!(titles(&server), expect(&mine));
}

/// A commit writing two rows links them: when one of them changes, the
/// commits on the other replay too, and each row keeps showing this
/// replica's newest pending write until its last pending commit settles.
#[test]
fn a_change_under_one_row_replays_the_commits_linked_to_it() {
    let server = open(0, AuthorSubject::SYSTEM);
    let a = open(1, user(1));
    let b = open(2, user(2));
    let a_link = connect(&server, &a, user(1));
    let _b_link = connect(&server, &b, user(2));
    let (_a_live, _b_live) = (subscribe(&a), subscribe(&b));
    for n in 1..=4 {
        insert(&server, n, &format!("s{n}"));
    }
    pump(&server, &[&a, &b]);

    a_link.hold_fates();
    // tx1 on row 1; tx2 links rows 1 and 2; tx3 on row 3 alone.
    update(&a, 1, "a1");
    commit(&a, &[], &[(1, "a2"), (2, "a2")]);
    update(&a, 3, "a3");
    pump(&server, &[&a, &b]);
    assert_eq!(a_link.held(), 3);
    // b writes row 2 after a's commits landed: its version arrives under
    // a's pending tx2, which replays over it, with tx1 on the row it links.
    update(&b, 2, "b2");
    pump(&server, &[&a, &b]);
    assert_eq!(
        titles(&a),
        expect(&[(1, "a2"), (2, "a2"), (3, "a3"), (4, "s4")]),
        "every pending write shows over the upstream's rows"
    );

    // tx1 settles: row 1 still shows tx2, which also keeps row 2.
    a_link.release_one(&a);
    assert_eq!(
        titles(&a),
        expect(&[(1, "a2"), (2, "a2"), (3, "a3"), (4, "s4")])
    );
    // tx2 settles: row 2 shows b's later write.
    a_link.release_one(&a);
    assert_eq!(
        titles(&a),
        expect(&[(1, "a2"), (2, "b2"), (3, "a3"), (4, "s4")])
    );
    a_link.release_one(&a);
    pump(&server, &[&a, &b]);
    assert_eq!(titles(&a), titles(&server));
    assert_eq!(titles(&b), titles(&server));
    assert_eq!(
        titles(&server),
        expect(&[(1, "a2"), (2, "b2"), (3, "a3"), (4, "s4")])
    );
}

/// A refused commit drops whole, on every row it wrote, and the later
/// pending commit on one of those rows still shows over what is left.
#[test]
fn a_refused_commit_drops_from_every_row_it_linked() {
    let server = open(0, AuthorSubject::SYSTEM);
    let a = open(1, user(1));
    let a_link = connect(&server, &a, user(1));
    let _live = subscribe(&a);
    insert(&server, 4, "s4");
    insert(&server, 6, "s6");
    pump(&server, &[&a]);

    a_link.hold_fates();
    // The server holds a row 5 of its own before a's insert of it lands:
    // the commit inserting it, and writing rows 4 and 6, is refused.
    commit(&a, &[(5, "mine")], &[(4, "a4"), (6, "a6")]);
    insert(&server, 5, "theirs");
    update(&a, 4, "a4 again");
    pump(&server, &[&a]);
    assert_eq!(a_link.held(), 2);
    assert_eq!(
        titles(&a),
        expect(&[(4, "a4 again"), (5, "mine"), (6, "a6")]),
        "until its fate"
    );

    a_link.release_one(&a);
    assert_eq!(
        titles(&a),
        expect(&[(4, "a4 again"), (5, "theirs"), (6, "s6")]),
        "refused, it drops from rows 5 and 6; row 4 shows the later commit"
    );
    // The later commit was built on the refused one, so the authority
    // refuses it as well: row 4 settles back to the server's version.
    a_link.release_one(&a);
    pump(&server, &[&a]);
    assert_eq!(titles(&a), titles(&server));
    assert_eq!(
        titles(&server),
        expect(&[(4, "s4"), (5, "theirs"), (6, "s6")])
    );
}
