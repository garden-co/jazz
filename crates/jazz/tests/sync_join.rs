//! The version handshake is one exchange: a replica's `Join` (its schema
//! version and the catalogue it keeps) opens every connection, and the
//! server answers it with one `Welcome` (admission and catalogue) ahead of
//! any row (SPEC 8.9, 8.10).

use crate::common::{allow_all_policies, compile_schema};
use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, SyncMessage, Transport, block_on};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::protocol::{LensOp, MigrationLens, TableLens};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::wire::TransportError;
use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

/// `docs(body)`; `draft` adds `note`, a version the server has not admitted.
fn schema(draft: bool) -> JazzSchema {
    let mut docs = TableSchemaBuilder::new("docs").column("body", ColumnType::Text);
    if draft {
        docs = docs.column("note", ColumnType::Text);
    }
    compile_schema(
        &SchemaBuilder::new()
            .table(docs.policies(allow_all_policies()))
            .build(),
    )
}

fn open(node: u8, draft: bool) -> Db {
    block_on(Db::open(DbConfig::new(
        schema(draft),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap()
}

type Log = Rc<RefCell<Vec<&'static str>>>;

/// One side of a duplex link that logs the kind of every message it sends.
struct Logged {
    outbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    inbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    log: Log,
}

impl Transport for Logged {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.log.borrow_mut().push(message.kind());
        self.outbound.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }
}

/// What the client and the server sent.
fn connect(client: &Db, server: &Db) -> (Log, Log) {
    let up = Rc::new(RefCell::new(VecDeque::new()));
    let down = Rc::new(RefCell::new(VecDeque::new()));
    let (sent_up, sent_down) = (Log::default(), Log::default());
    block_on(client.connect_upstream(Box::new(Logged {
        outbound: up.clone(),
        inbound: down.clone(),
        log: sent_up.clone(),
    })));
    server
        .accept_subscriber(
            Box::new(Logged {
                outbound: down,
                inbound: up,
                log: sent_down.clone(),
            }),
            AuthorSubject::SYSTEM,
        )
        .unwrap();
    (sent_up, sent_down)
}

fn pump(dbs: &[&Db]) {
    for _ in 0..8 {
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
    }
}

#[test]
fn a_connection_opens_with_one_join_answered_by_one_welcome_ahead_of_rows() {
    let server = open(1, false);
    server.set_authority();
    block_on(server.insert(
        "docs",
        BTreeMap::from([("body".to_owned(), Value::String("hi".to_owned()))]),
        Default::default(),
    ))
    .unwrap();
    let client = open(2, false);
    let (up, down) = connect(&client, &server);
    let docs = client.prepare_query(&Query::from("docs")).unwrap();
    let _live = block_on(client.subscribe(&docs, ReadOpts::default())).unwrap();
    pump(&[&client, &server, &client]);

    assert_eq!(up.borrow().first(), Some(&"Join"));
    assert!(
        !up.borrow().iter().any(|kind| *kind == "Schema"),
        "no switch: {:?}",
        up.borrow()
    );
    let down = down.borrow();
    assert_eq!(down.first(), Some(&"Welcome"), "{down:?}");
    assert!(down.contains(&"Rows"), "{down:?}");
    assert!(
        !down
            .iter()
            .any(|kind| matches!(*kind, "Catalogue" | "SchemaAdmission")),
        "the Welcome alone carries admission and catalogue: {down:?}"
    );
    assert_eq!(down.iter().filter(|kind| **kind == "Welcome").count(), 1);
}

#[test]
fn an_unadmitted_client_is_welcomed_unadmitted_and_told_once_it_is_admitted() {
    let server = open(1, false);
    server.set_authority();
    let client = open(2, true);
    let (_, down) = connect(&client, &server);
    pump(&[&client, &server, &client]);
    assert_eq!(*down.borrow(), ["Welcome"]);

    let lens = MigrationLens::new(
        schema(false).version_id(),
        schema(true).version_id(),
        vec![TableLens {
            source_table: "docs".into(),
            target_table: "docs".into(),
            ops: vec![LensOp::AddColumn {
                column: "note".into(),
                default: Value::String(String::new()),
            }],
        }],
    )
    .unwrap();
    block_on(server.admit_schema_with_lenses(schema(true), &[lens])).unwrap();
    pump(&[&client, &server, &client]);
    let down = down.borrow();
    assert_eq!(down[0], "Welcome");
    assert!(down[1..].contains(&"SchemaAdmission"), "{down:?}");
    assert!(down[1..].contains(&"Catalogue"), "{down:?}");
}

#[test]
fn a_reconnect_joins_again_and_is_welcomed_again() {
    let server = open(1, false);
    server.set_authority();
    let client = open(2, false);
    let (first_up, first_down) = connect(&client, &server);
    pump(&[&client, &server, &client]);
    assert_eq!(*first_up.borrow(), ["Join"]);
    assert_eq!(*first_down.borrow(), ["Welcome"]);

    let (up, down) = connect(&client, &server);
    pump(&[&client, &server, &client]);
    assert_eq!(*up.borrow(), ["Join"]);
    assert_eq!(*down.borrow(), ["Welcome"]);
}

#[test]
fn a_tab_joins_its_worker_as_the_worker_joins_the_server() {
    let server = open(1, false);
    server.set_authority();
    let worker = open(2, false);
    let (worker_up, worker_down) = connect(&worker, &server);
    pump(&[&worker, &server, &worker]);
    let tab = open(3, false);
    tab.set_non_durable_client();
    let (tab_up, tab_down) = connect(&tab, &worker);
    pump(&[&tab, &worker, &server, &worker, &tab]);

    for (up, down) in [(worker_up, worker_down), (tab_up, tab_down)] {
        assert_eq!(up.borrow().first(), Some(&"Join"), "{:?}", up.borrow());
        let down = down.borrow();
        assert_eq!(down.first(), Some(&"Welcome"), "{down:?}");
        assert_eq!(down.iter().filter(|kind| **kind == "Welcome").count(), 1);
        assert!(!down.contains(&"SchemaAdmission"), "{down:?}");
    }
}
