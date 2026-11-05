//! State-space audit (storage-values): text-merging columns whose value
//! crosses the 64 KiB inline/chunked threshold (SPEC 19.1) while concurrent
//! offline edits merge at the authority (SPEC 4 §4.3), including an offline
//! chain across an authority restart. Harness copied from `text_merge.rs`.
#![allow(dead_code)]

use crate::common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use duplex_transport::duplex;
use jazz::db::{
    Db, DbConfig, DbIdentity, ReadOpts, RemoteLinkHint, SubscriptionStream, Upstream, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    ColumnDescriptor, ColumnMergeStrategy, ColumnType, RowDescriptor, Schema, TableName,
    TableSchema, Value as PublicValue,
};
use jazz::tx::DurabilityTier;

use common::{allow_all_policies, compile_schema};

const MAX_TURNS: usize = 400;
const CORE: u8 = 0xc0;

fn schema() -> JazzSchema {
    let column = |name: &str| {
        ColumnDescriptor::new(name, ColumnType::Text).default(PublicValue::Text(String::new()))
    };
    let merging = |name: &str, strategy| column(name).merge_strategy(strategy);
    compile_schema(&Schema::from([(
        TableName::new("docs"),
        TableSchema {
            columns: RowDescriptor::new(vec![
                merging("body", ColumnMergeStrategy::Text),
                merging("notes", ColumnMergeStrategy::Markdown),
                merging("page", ColumnMergeStrategy::Html),
                column("title"),
                ColumnDescriptor::new("summary", ColumnType::Text)
                    .nullable()
                    .merge_strategy(ColumnMergeStrategy::Text),
            ]),
            indexed_columns: None,
            composite_indexes: Vec::new(),
            policies: allow_all_policies(),
            branch_by: Vec::new(),
        },
    )]))
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

fn cells(pairs: &[(&str, &str)]) -> BTreeMap<String, Value> {
    pairs
        .iter()
        .map(|(column, value)| ((*column).to_owned(), Value::String((*value).to_owned())))
        .collect()
}

/// Column `summary`, optional: `None` writes NULL.
fn summary(value: Option<&str>) -> BTreeMap<String, Value> {
    BTreeMap::from([(
        "summary".to_owned(),
        Value::Nullable(value.map(|text| Box::new(Value::String(text.to_owned())))),
    )])
}

struct Client {
    node: u8,
    db: Db,
    up: Option<Upstream>,
    _docs: Option<SubscriptionStream>,
}

/// The authority and clients connected to it directly.
struct Net {
    core: Db,
    clients: Vec<Client>,
    /// Where the authority's file lives, when it has one.
    dir: Option<tempfile::TempDir>,
}

fn open_core(dir: &tempfile::TempDir) -> Db {
    block_on(Db::open_at(
        DbConfig::new(
            schema(),
            DbIdentity {
                node: NodeUuid::from_bytes([CORE; 16]),
                author: AuthorSubject::SYSTEM,
            },
        ),
        &dir.path().join("core.sqlite"),
        jazz::db::FileOptions::default(),
    ))
    .unwrap()
}

impl Net {
    fn new(clients: usize) -> Self {
        Self::with_core(clients, None)
    }

    /// [`Net::new`] with the authority on a file, so it can restart.
    fn on_file(clients: usize) -> Self {
        Self::with_core(clients, Some(tempfile::tempdir().unwrap()))
    }

    fn with_core(clients: usize, dir: Option<tempfile::TempDir>) -> Self {
        let core = match &dir {
            Some(dir) => open_core(dir),
            None => open(CORE, AuthorSubject::SYSTEM),
        };
        let mut net = Net {
            core,
            clients: Vec::new(),
            dir,
        };
        for _ in 0..clients {
            net.join();
        }
        net
    }

    /// A new client, connected and subscribed to every doc; its index.
    fn join(&mut self) -> usize {
        let i = self.clients.len();
        let node = 0x50 + i as u8;
        let db = open(node, author(node));
        db.set_remote_link_hint(RemoteLinkHint::Live);
        self.clients.push(Client {
            node,
            db,
            up: None,
            _docs: None,
        });
        self.connect(i);
        let prepared = self.clients[i]
            .db
            .prepare_query(&Query::from("docs"))
            .unwrap();
        let stream = self.drive(self.clients[i].db.subscribe(&prepared, ReadOpts::default()));
        self.clients[i]._docs = Some(stream.unwrap());
        self.pump();
        i
    }

    fn connect(&mut self, i: usize) {
        let (client_side, server_side) = duplex();
        let client = &mut self.clients[i];
        client.up = Some(block_on(client.db.connect_upstream(client_side)));
        self.core
            .accept_subscriber(server_side, author(client.node))
            .unwrap();
    }

    fn offline(&mut self, i: usize) {
        let client = &mut self.clients[i];
        let up = client.up.take().expect("online");
        assert!(client.db.detach_connection(&up));
        client.db.set_remote_link_hint(RemoteLinkHint::Failed);
    }

    fn online(&mut self, i: usize) {
        self.connect(i);
        self.clients[i]
            .db
            .set_remote_link_hint(RemoteLinkHint::Live);
        self.pump();
    }

    fn db(&self, i: usize) -> &Db {
        &self.clients[i].db
    }

    /// Close the authority and open it again from its file; the clients
    /// that were online reconnect.
    fn restart_core(&mut self) {
        let online: Vec<usize> = (0..self.clients.len())
            .filter(|&i| self.clients[i].up.is_some())
            .collect();
        for &i in &online {
            let client = &mut self.clients[i];
            let up = client.up.take().expect("online");
            assert!(client.db.detach_connection(&up));
        }
        drop(std::mem::replace(
            &mut self.core,
            open(CORE, AuthorSubject::SYSTEM),
        ));
        self.core = open_core(self.dir.as_ref().expect("an authority on a file"));
        for i in online {
            self.connect(i);
        }
    }

    /// The bodies `db`'s history read lists for `row`, oldest first.
    fn history(db: &Db, row: RowUuid) -> Vec<String> {
        let query = Query::from("history.docs")
            .filter(jazz::query::eq(
                jazz::query::col("id"),
                jazz::query::lit(Value::Uuid(row.0)),
            ))
            .order_by("$acceptedAt", jazz::query::OrderDirection::Asc);
        block_on(db.all(&db.prepare_query(&query).unwrap(), ReadOpts::default()))
            .unwrap()
            .into_iter()
            .map(|version| match version.cell("body") {
                Some(Value::String(text)) => text,
                other => panic!("`body` is {other:?}"),
            })
            .collect()
    }

    fn pump(&self) {
        for _ in 0..3 {
            for client in &self.clients {
                block_on(client.db.tick()).unwrap();
            }
            block_on(self.core.tick()).unwrap();
            for client in &self.clients {
                block_on(client.db.tick()).unwrap();
            }
        }
    }

    fn drive<T>(&self, future: impl Future<Output = T>) -> T {
        let mut future = pin!(future);
        let mut context = Context::from_waker(Waker::noop());
        for _ in 0..MAX_TURNS {
            if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
                return output;
            }
            self.pump();
        }
        panic!("operation did not complete within {MAX_TURNS} turns");
    }

    fn settle(&self, i: usize, tx: u64) {
        self.drive(self.db(i).wait_for_transaction(tx, DurabilityTier::Global))
            .expect("the write settles");
        self.pump();
    }

    /// A doc client `i` inserts, settled and held by every client.
    fn seed(&self, i: usize, pairs: &[(&str, &str)]) -> RowUuid {
        let write = block_on(self.db(i).insert("docs", cells(pairs), Default::default())).unwrap();
        self.settle(i, write.mergeable_tx_id());
        write.row_uuid()
    }

    /// Client `i` edits `row`; returns the edit's tx. It does not wait.
    fn edit(&self, i: usize, row: RowUuid, pairs: &[(&str, &str)]) -> u64 {
        block_on(
            self.db(i)
                .update("docs", row, cells(pairs), Default::default()),
        )
        .unwrap()
        .mergeable_tx_id()
    }

    /// The seq of `row`'s current version at the authority.
    fn seq_of(&self, row: RowUuid) -> u64 {
        self.core
            .write_conflicts("docs", row, None)
            .unwrap()
            .last()
            .expect("an accepted version")
            .seq
    }

    /// Column `column` of `row` as `db` holds it.
    fn read(db: &Db, row: RowUuid, column: &str) -> String {
        let prepared = db.prepare_query(&Query::from("docs")).unwrap();
        match block_on(db.all(&prepared, ReadOpts::default()))
            .unwrap()
            .into_iter()
            .find(|current| current.row_uuid() == row)
            .and_then(|current| current.cell(column))
        {
            Some(Value::String(text)) => text,
            other => panic!("`{column}` is {other:?}"),
        }
    }

    /// Optional column `summary` of `row` as `db` holds it.
    fn read_summary(db: &Db, row: RowUuid) -> Option<String> {
        let prepared = db.prepare_query(&Query::from("docs")).unwrap();
        match block_on(db.all(&prepared, ReadOpts::default()))
            .unwrap()
            .into_iter()
            .find(|current| current.row_uuid() == row)
            .and_then(|current| current.cell("summary"))
        {
            Some(Value::Nullable(None)) | None => None,
            Some(Value::Nullable(Some(text))) => match *text {
                Value::String(text) => Some(text),
                other => panic!("`summary` holds {other:?}"),
            },
            Some(Value::String(text)) => Some(text),
            other => panic!("`summary` is {other:?}"),
        }
    }

    /// Client `i` writes `summary` of `row`; returns the write's tx.
    fn write_summary(&self, i: usize, row: RowUuid, value: Option<&str>) -> u64 {
        block_on(
            self.db(i)
                .update("docs", row, summary(value), Default::default()),
        )
        .unwrap()
        .mergeable_tx_id()
    }

    /// Every replica holds `expected` in optional column `summary` of `row`.
    fn assert_summary_everywhere(&self, row: RowUuid, expected: Option<&str>) {
        assert_eq!(
            Self::read_summary(&self.core, row).as_deref(),
            expected,
            "the authority"
        );
        for (i, client) in self.clients.iter().enumerate() {
            assert_eq!(
                Self::read_summary(&client.db, row).as_deref(),
                expected,
                "client {i}"
            );
        }
    }

    /// Every replica holds `expected` in `column` of `row`.
    fn assert_everywhere(&self, row: RowUuid, column: &str, expected: &str) {
        assert_eq!(
            Self::read(&self.core, row, column),
            expected,
            "the authority"
        );
        for (i, client) in self.clients.iter().enumerate() {
            assert_eq!(Self::read(&client.db, row, column), expected, "client {i}");
        }
    }
}

fn words(n: usize, seed: usize) -> String {
    let words = ["lorem", "ipsum", "dolor", "sit", "amet", "consectetur"];
    (0..n)
        .map(|i| words[(i * 7 + i / 5 + seed) % words.len()])
        .collect::<Vec<_>>()
        .join(" ")
}

/// alice and bob both hold an inline body just under 64 KiB. Offline,
/// alice grows it past the threshold (it becomes chunked on her replica
/// and as sent), bob edits its start. Both edits survive everywhere.
#[test]
fn audit_storage_values_text_merge_where_one_side_crosses_into_chunked() {
    let mut net = Net::new(2);
    let base = words(11_500, 0);
    assert!(
        base.len() < 64 * 1024 && base.len() > 55 * 1024,
        "{}",
        base.len()
    );
    let row = net.seed(0, &[("body", &base)]);
    net.offline(0);
    net.offline(1);
    let tail = words(2_000, 3);
    let alice = net.edit(0, row, &[("body", &format!("{base} {tail}"))]);
    let bob = net.edit(1, row, &[("body", &format!("Preface. {base}"))]);
    net.online(0);
    net.settle(0, alice);
    net.online(1);
    net.settle(1, bob);
    net.assert_everywhere(row, "body", &format!("Preface. {base} {tail}"));
}

/// The reverse: a chunked body; alice trims it back under 64 KiB (inline),
/// bob prepends. Sequenced alice first, then bob.
#[test]
fn audit_storage_values_text_merge_where_one_side_drops_back_inline() {
    let mut net = Net::new(2);
    let keep = words(9_000, 0);
    let cut = words(3_000, 1);
    let base = format!("{keep} {cut}");
    assert!(base.len() > 64 * 1024 && keep.len() < 64 * 1024);
    let row = net.seed(0, &[("body", &base)]);
    net.offline(0);
    net.offline(1);
    let alice = net.edit(0, row, &[("body", &keep)]);
    let bob = net.edit(1, row, &[("body", &format!("Preface. {base}"))]);
    net.online(0);
    net.settle(0, alice);
    net.online(1);
    net.settle(1, bob);
    net.assert_everywhere(row, "body", &format!("Preface. {keep}"));
}

/// bob's offline chain crosses the threshold in its first link and edits
/// again in its second; alice's concurrent edit lands first; the authority
/// restarts between bob's two links. Every edit survives.
#[test]
fn audit_storage_values_text_merge_chain_across_threshold_and_restart() {
    let mut net = Net::on_file(2);
    let base = words(11_500, 0);
    assert!(base.len() < 64 * 1024);
    let row = net.seed(0, &[("body", &base)]);
    net.offline(1);
    let alice = net.edit(0, row, &[("body", &format!("Preface. {base}"))]);
    net.settle(0, alice);
    let tail = words(2_000, 3);
    let first = format!("{base} {tail}");
    assert!(first.len() > 64 * 1024);
    let bob1 = net.edit(1, row, &[("body", &first)]);
    let bob2 = net.edit(1, row, &[("body", &format!("{first} The end."))]);
    net.online(1);
    net.settle(1, bob1);
    net.restart_core();
    net.pump();
    net.settle(1, bob2);
    net.assert_everywhere(row, "body", &format!("Preface. {base} {tail} The end."));
}
