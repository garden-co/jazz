//! A worker and its tabs each number their own unsettled versions. A tab's
//! replica holds the worker's unsettled versions (other tabs' writes the
//! worker has not settled yet) next to its own, so the two numberings must
//! never collide in a tab's history: the worker's start at `LOCAL_BASE`,
//! a tab's at `NON_DURABLE_BASE`.

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;

use duplex_transport::duplex;
use jazz::db::{
    Db, DbConfig, DbIdentity, LOCAL_BASE, NON_DURABLE_BASE, ReadOpts, RemoteLinkHint, Upstream,
    block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

use crate::common::{allow_all_policies, compile_schema};

const WORKER: u8 = 0xa0;

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("body", ColumnType::Text)
                    .policies(allow_all_policies()),
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

fn author(node: u8) -> AuthorSubject {
    AuthorSubject::for_test_bytes([node; 16])
}

fn body(text: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("body".to_owned(), Value::String(text.to_owned()))])
}

/// The authority, one worker, and its tabs (each connected as the worker's
/// identity, as a browser's tabs are).
struct Net {
    core: Db,
    worker: Db,
    worker_up: Option<Upstream>,
    tabs: Vec<Db>,
}

impl Net {
    fn new(tabs: usize) -> Self {
        let mut net = Net {
            core: open(0xc0, AuthorSubject::SYSTEM),
            worker: open(WORKER, author(WORKER)),
            worker_up: None,
            tabs: Vec::new(),
        };
        net.worker_online();
        for i in 0..tabs {
            let tab = open(WORKER + 1 + i as u8, author(WORKER));
            tab.set_non_durable_client();
            tab.set_remote_link_hint(RemoteLinkHint::Live);
            let (client_side, server_side) = duplex();
            block_on(tab.connect_upstream(client_side));
            net.worker.accept_subscriber(server_side, author(WORKER));
            net.tabs.push(tab);
        }
        net
    }

    fn worker_online(&mut self) {
        let (client_side, server_side) = duplex();
        self.worker_up = Some(block_on(self.worker.connect_upstream(client_side)));
        self.core.accept_subscriber(server_side, author(WORKER));
        self.worker.set_remote_link_hint(RemoteLinkHint::Live);
    }

    fn worker_offline(&mut self) {
        let upstream = self.worker_up.take().expect("online");
        assert!(self.worker.detach_connection(&upstream));
        self.worker.set_remote_link_hint(RemoteLinkHint::Failed);
    }

    fn tab(&self, i: usize) -> &Db {
        &self.tabs[i]
    }

    fn pump(&self) {
        for _ in 0..4 {
            for tab in &self.tabs {
                block_on(tab.tick()).unwrap();
            }
            block_on(self.worker.tick()).unwrap();
            if self.worker_up.is_some() {
                block_on(self.core.tick()).unwrap();
                block_on(self.worker.tick()).unwrap();
            }
            for tab in &self.tabs {
                block_on(tab.tick()).unwrap();
            }
        }
    }

    fn insert(&self, db: &Db, text: &str) -> RowUuid {
        block_on(db.insert("notes", body(text), Default::default()))
            .unwrap()
            .row_uuid()
    }

    fn update(&self, db: &Db, row: RowUuid, text: &str) -> u64 {
        block_on(db.update("notes", row, body(text), Default::default()))
            .unwrap_or_else(|error| panic!("writing {text:?}: {error}"))
            .mergeable_tx_id()
    }

    /// `db` subscribes to every note, so its replica holds the rows.
    fn watch(&self, db: &Db) -> jazz::db::SubscriptionStream {
        let prepared = db.prepare_query(&Query::from("notes")).unwrap();
        let stream = block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap();
        self.pump();
        stream
    }

    fn bodies(&self, db: &Db) -> Vec<String> {
        let prepared = db.prepare_query(&Query::from("notes")).unwrap();
        block_on(db.all(&prepared, ReadOpts::default()))
            .unwrap()
            .iter()
            .filter_map(|row| match row.cells().find(|(name, _)| *name == "body") {
                Some((_, Some(Value::String(text)))) => Some(text.clone()),
                _ => None,
            })
            .collect()
    }

    fn settle(&self, db: &Db, tx: u64) {
        use std::future::Future;
        use std::pin::pin;
        use std::task::{Context, Poll, Waker};
        let mut wait = pin!(db.wait_for_transaction(tx, DurabilityTier::Global));
        let mut context = Context::from_waker(Waker::noop());
        for _ in 0..50 {
            if let Poll::Ready(result) = wait.as_mut().poll(&mut context) {
                result.expect("the write settles");
                return;
            }
            self.pump();
        }
        panic!("tx {tx} did not settle");
    }
}

/// A tab with an unsettled write on a row receives another tab's unsettled
/// version of it from the worker, then writes the row again. Numbered from
/// one base, the worker's version and the tab's write had the same seq, and
/// the tab's next write failed to keep its version as history.
#[test]
fn a_tab_writes_again_over_another_tabs_unsettled_version_from_its_worker() {
    let mut net = Net::new(2);
    net.worker_offline();
    // The tab's first commit is also the worker's first.
    let row = net.insert(net.tab(0), "seed");
    let _watch = [net.watch(net.tab(0)), net.watch(net.tab(1))];
    net.pump();

    // The worker orders the other tab's write second, and the tab's own
    // second write third.
    let other = net.update(net.tab(1), row, "b1");
    let own = net.update(net.tab(0), row, "a1");
    net.pump();
    let last = net.update(net.tab(0), row, "a2");
    net.pump();
    for tx in [other, own, last] {
        assert!(
            tx > NON_DURABLE_BASE,
            "a tab numbers its writes in its own range"
        );
    }
    assert!(NON_DURABLE_BASE > LOCAL_BASE);

    net.worker_online();
    net.settle(net.tab(0), last);
    let expected = net.bodies(&net.core);
    assert_eq!(expected, ["a2"]);
    for db in [&net.worker, net.tab(0), net.tab(1)] {
        assert_eq!(net.bodies(db), expected, "replicas converge");
    }
}

/// A tab's unsettled insert stays its own after the worker stored it at a
/// seq of its own (here not the tab's: the worker wrote first, unseen by
/// the tab): a serializable snapshot on the tab still reads it as the
/// tab's write.
#[test]
fn a_tabs_unsettled_insert_stays_its_own_once_its_worker_stored_it() {
    let mut net = Net::new(1);
    net.worker_offline();
    net.insert(&net.worker, "worker");
    let row = net.insert(net.tab(0), "own");
    net.pump();

    let tab = net.tab(0);
    let open = jazz::tools::OpenTransactionId::new();
    block_on(tab.begin_serializable(open)).unwrap();
    let prepared = tab.prepare_query(&Query::from("notes")).unwrap();
    let tx = tab.tx_ref(open);
    let mut read = std::pin::pin!(tx.all_prepared(&prepared));
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    let rows = loop {
        if let std::task::Poll::Ready(rows) = read.as_mut().poll(&mut context) {
            break rows.unwrap();
        }
        net.pump();
    };
    assert!(
        rows.iter().any(|r| r.row_uuid() == row),
        "the snapshot holds the tab's own unsettled insert"
    );
}
