//! Text-merging string columns (SPEC 4 §4.3): a column declared `text`,
//! `markdown` or `html` stores a plain string, and concurrent writes to it
//! merge. Writes travel as plain values; the authority merges a write that
//! missed a version authoring the column, over the version it was built on
//! (SPEC 4 §4.6), and every replica converges to its image. Disjoint edits
//! all survive, for inline and large values alike; overlapping
//! replacements go to the later write; a write without anything to merge
//! takes the fast path.

use crate::common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use duplex_transport::duplex;
use jazz::db::{
    Db, DbConfig, DbIdentity, LargeValueUpdateSplice, ReadOpts, RemoteLinkHint, SubscriptionStream,
    Upstream, block_on,
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
}

impl Net {
    fn new(clients: usize) -> Self {
        let mut net = Net {
            core: open(CORE, AuthorSubject::SYSTEM),
            clients: Vec::new(),
        };
        for i in 0..clients {
            let node = 0x50 + i as u8;
            let db = open(node, author(node));
            db.set_remote_link_hint(RemoteLinkHint::Live);
            net.clients.push(Client {
                node,
                db,
                up: None,
                _docs: None,
            });
            net.connect(i);
            let prepared = net.clients[i]
                .db
                .prepare_query(&Query::from("docs"))
                .unwrap();
            let stream = net.drive(net.clients[i].db.subscribe(&prepared, ReadOpts::default()));
            net.clients[i]._docs = Some(stream.unwrap());
            net.pump();
        }
        net
    }

    fn connect(&mut self, i: usize) {
        let (client_side, server_side) = duplex();
        let client = &mut self.clients[i];
        client.up = Some(block_on(client.db.connect_upstream(client_side)));
        self.core
            .accept_subscriber(server_side, author(client.node));
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

#[test]
fn concurrent_disjoint_edits_both_survive_on_every_replica() {
    let mut net = Net::new(2);
    let row = net.seed(0, &[("body", "The cat sat.")]);
    net.offline(0);
    net.offline(1);
    let alice = net.edit(0, row, &[("body", "The black cat sat.")]);
    let bob = net.edit(1, row, &[("body", "The cat slept.")]);
    net.online(0);
    net.settle(0, alice);
    net.online(1);
    net.settle(1, bob);
    net.assert_everywhere(row, "body", "The black cat slept.");
}

#[test]
fn a_last_writer_wins_column_beside_it_still_takes_the_later_write() {
    let mut net = Net::new(2);
    let row = net.seed(0, &[("body", "one"), ("title", "seed")]);
    net.offline(0);
    net.offline(1);
    let alice = net.edit(0, row, &[("body", "one two"), ("title", "alice")]);
    let bob = net.edit(1, row, &[("body", "zero one"), ("title", "bob")]);
    net.online(0);
    net.settle(0, alice);
    net.online(1);
    net.settle(1, bob);
    net.assert_everywhere(row, "body", "zero one two");
    net.assert_everywhere(row, "title", "bob");
}

#[test]
fn overlapping_replacements_go_to_the_later_write() {
    let mut net = Net::new(2);
    let row = net.seed(0, &[("body", "the quick fox")]);
    net.offline(0);
    net.offline(1);
    let alice = net.edit(0, row, &[("body", "the slow fox")]);
    let bob = net.edit(1, row, &[("body", "the fast fox")]);
    net.online(0);
    net.settle(0, alice);
    net.online(1);
    net.settle(1, bob);
    net.assert_everywhere(row, "body", "the fast fox");
}

/// The chain case a stored version cannot serve as base for: an offline
/// writer's second edit was made over its first, which the authority
/// merged with writes the writer never saw before the second arrived.
#[test]
fn an_offline_chain_merges_each_write_over_what_its_writer_saw() {
    let mut net = Net::new(2);
    let row = net.seed(0, &[("body", "Hello")]);
    net.offline(0);
    let first = net.edit(0, row, &[("body", "Hello world")]);
    let second = net.edit(0, row, &[("body", "Hello big world")]);
    let carol = net.edit(1, row, &[("body", "Hello!")]);
    net.settle(1, carol);
    let dave = net.edit(1, row, &[("body", "Hi! Hello!")]);
    net.settle(1, dave);
    net.online(0);
    net.settle(0, first);
    net.settle(0, second);
    // Carol's "!" and alice's " world" both landed after "Hello": the
    // held value's insert goes first.
    net.assert_everywhere(row, "body", "Hi! Hello! big world");
}

#[test]
fn only_a_write_that_missed_an_edit_to_the_column_merges() {
    let mut net = Net::new(2);
    let row = net.seed(0, &[("body", "alpha beta gamma"), ("title", "seed")]);
    // Nothing missed: the fast path, no base read.
    let first = net.edit(0, row, &[("body", "alpha beta gamma delta")]);
    net.settle(0, first);
    let first_seq = net.seq_of(row);
    assert_eq!(
        net.core.merge_base("docs", row, first_seq, "body").unwrap(),
        None
    );
    // A missed write that set only the title: still nothing to merge.
    net.offline(0);
    let second = net.edit(0, row, &[("body", "alpha beta gamma delta epsilon")]);
    let bob = net.edit(1, row, &[("title", "bob")]);
    net.settle(1, bob);
    net.online(0);
    net.settle(0, second);
    let second_seq = net.seq_of(row);
    assert_eq!(
        net.core
            .merge_base("docs", row, second_seq, "body")
            .unwrap(),
        None
    );
    // A missed write to the body: merged over the version it was built on.
    net.offline(0);
    let third = net.edit(0, row, &[("body", "alpha beta gamma delta epsilon zeta")]);
    let carol = net.edit(1, row, &[("body", "ALPHA beta gamma delta epsilon")]);
    net.settle(1, carol);
    net.online(0);
    net.settle(0, third);
    let third_seq = net.seq_of(row);
    assert_eq!(
        net.core.merge_base("docs", row, third_seq, "body").unwrap(),
        Some(Value::String("alpha beta gamma delta epsilon".to_owned()))
    );
    net.assert_everywhere(row, "body", "ALPHA beta gamma delta epsilon zeta");
}

#[test]
fn markdown_and_html_edits_keep_their_markup_whole() {
    let mut net = Net::new(2);
    let row = net.seed(
        0,
        &[("notes", "make this bold"), ("page", "<p>one two</p>")],
    );
    net.offline(0);
    net.offline(1);
    let alice = net.edit(
        0,
        row,
        &[
            ("notes", "make **this** bold"),
            ("page", "<p>one <b>two</b></p>"),
        ],
    );
    let bob = net.edit(
        1,
        row,
        &[
            ("notes", "make this bold!"),
            ("page", "<p>zero one two</p>"),
        ],
    );
    net.online(0);
    net.settle(0, alice);
    net.online(1);
    net.settle(1, bob);
    net.assert_everywhere(row, "notes", "make **this** bold!");
    net.assert_everywhere(row, "page", "<p>zero one <b>two</b></p>");
}

#[test]
fn large_values_merge_edits_at_both_ends() {
    let mut net = Net::new(2);
    let words = ["lorem", "ipsum", "dolor", "sit", "amet", "consectetur"];
    let base: String = (0..40_000)
        .map(|i| words[(i * 7 + i / 5) % words.len()])
        .collect::<Vec<_>>()
        .join(" ");
    assert!(base.len() > 200_000, "well past the inline threshold");
    let row = net.seed(0, &[("body", &base)]);
    net.offline(0);
    net.offline(1);
    let alice = net.edit(0, row, &[("body", &format!("Preface. {base}"))]);
    let bob = net.edit(1, row, &[("body", &format!("{base} The end."))]);
    net.online(0);
    net.settle(0, alice);
    net.online(1);
    net.settle(1, bob);
    net.assert_everywhere(row, "body", &format!("Preface. {base} The end."));
}

#[test]
fn large_value_splices_and_appends_merge_too() {
    let mut net = Net::new(2);
    let base: String = (0..30_000).map(|i| format!("w{} ", i % 97)).collect();
    let row = net.seed(0, &[("body", &base)]);
    net.offline(0);
    net.offline(1);
    let alice = block_on(net.db(0).splice_value(
        "docs",
        row,
        "body",
        LargeValueUpdateSplice {
            at: 0,
            delete: 0,
            insert: b"Preface. ".to_vec(),
        },
        Default::default(),
    ))
    .unwrap()
    .mergeable_tx_id();
    let bob = block_on(
        net.db(1)
            .append_value("docs", row, "body", b"The end.".to_vec()),
    )
    .unwrap()
    .mergeable_tx_id();
    net.online(0);
    net.settle(0, alice);
    net.online(1);
    net.settle(1, bob);
    net.assert_everywhere(row, "body", &format!("Preface. {base}The end."));
}

#[test]
fn transaction_writes_merge_over_what_the_transaction_saw() {
    let mut net = Net::new(2);
    let row = net.seed(0, &[("body", "one two")]);
    net.offline(0);
    let tx = block_on(net.db(0).mergeable_tx()).unwrap();
    block_on(tx.update(
        "docs",
        row,
        cells(&[("body", "one two three")]),
        Default::default(),
    ))
    .unwrap();
    // A second write in the same transaction, over its first.
    block_on(tx.update(
        "docs",
        row,
        cells(&[("body", "one two three four")]),
        Default::default(),
    ))
    .unwrap();
    let alice = block_on(tx.commit()).unwrap().0;
    let bob = net.edit(1, row, &[("body", "zero one two")]);
    net.settle(1, bob);
    net.online(0);
    net.settle(0, alice);
    net.assert_everywhere(row, "body", "zero one two three four");
}
