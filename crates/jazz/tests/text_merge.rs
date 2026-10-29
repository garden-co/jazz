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
    // Both merges kept what alice's chain sent, as versions of their own.
    assert_eq!(net.core.as_sent_versions_for_test().unwrap(), 2);
    // No read lists them: history holds the accepted versions only.
    let accepted = [
        "Hello",
        "Hello!",
        "Hi! Hello!",
        "Hi! Hello! world",
        "Hi! Hello! big world",
    ];
    assert_eq!(Net::history(&net.core, row), accepted);
    // A client lists the versions it holds, never one as sent.
    for i in 0..2 {
        let held = Net::history(net.db(i), row);
        assert!(
            held.iter().all(|body| accepted.contains(&body.as_str())),
            "client {i}: {held:?}"
        );
    }
    // Conflict analysis lists the accepted writes only.
    let seqs: Vec<u64> = net
        .core
        .write_conflicts("docs", row, None)
        .unwrap()
        .iter()
        .map(|write| write.seq)
        .collect();
    assert_eq!(seqs.len(), accepted.len(), "{seqs:?}");
    assert!(seqs.windows(2).all(|pair| pair[0] < pair[1]), "{seqs:?}");
    // A client that joins now catches up on the merged value only.
    let carol = net.join();
    assert_eq!(
        Net::read(net.db(carol), row, "body"),
        "Hi! Hello! big world"
    );
    let held = Net::history(net.db(carol), row);
    assert!(
        held.iter().all(|body| accepted.contains(&body.as_str())),
        "{held:?}"
    );
    // Alice's next write is built on a settled version: nothing can build
    // on her old chain any more, and its as-sent versions go.
    let next = net.edit(0, row, &[("body", "Hi! Hello! big world.")]);
    net.settle(0, next);
    net.assert_everywhere(row, "body", "Hi! Hello! big world.");
    assert_eq!(net.core.as_sent_versions_for_test().unwrap(), 0);
}

/// A read through a cut never takes an as-sent version for a version of
/// its row: not before the row existed, nor between its versions.
#[test]
fn a_read_at_a_snapshot_never_sees_a_value_as_sent() {
    let mut net = Net::new(2);
    let before = block_on(net.core.capture_snapshot()).unwrap();
    let row = net.seed(0, &[("body", "The cat sat.")]);
    let seeded = block_on(net.core.capture_snapshot()).unwrap();
    net.offline(0);
    net.offline(1);
    let alice = net.edit(0, row, &[("body", "The black cat sat.")]);
    let bob = net.edit(1, row, &[("body", "The cat slept.")]);
    net.online(0);
    net.settle(0, alice);
    net.online(1);
    net.settle(1, bob);
    assert_eq!(net.core.as_sent_versions_for_test().unwrap(), 1);
    let at = |snapshot: &jazz::protocol::SnapshotRef| -> Vec<String> {
        let opts = ReadOpts {
            read_view: jazz::protocol::ReadViewSpec {
                source: jazz::protocol::ReadViewSourceSpec::Snapshot {
                    snapshot: snapshot.clone(),
                },
                include_deleted: false,
            },
            ..Default::default()
        };
        let prepared = net.core.prepare_query(&Query::from("docs")).unwrap();
        block_on(net.core.all(&prepared, opts))
            .unwrap()
            .into_iter()
            .map(|current| match current.cell("body") {
                Some(Value::String(text)) => text,
                other => panic!("`body` is {other:?}"),
            })
            .collect()
    };
    assert!(at(&before).is_empty(), "{:?}", at(&before));
    assert_eq!(at(&seeded), ["The cat sat."]);
    net.assert_everywhere(row, "body", "The black cat slept.");
}

/// The as-sent version a chained write merges over is stored, not held in
/// memory: the authority restarts between the merged link and the next.
#[test]
fn a_chain_merges_over_its_value_as_sent_after_the_authority_restarts() {
    let mut net = Net::on_file(2);
    let row = net.seed(0, &[("body", "Hello")]);
    net.offline(0);
    let first = net.edit(0, row, &[("body", "Hello world")]);
    let bob = net.edit(1, row, &[("body", "Hello!")]);
    net.settle(1, bob);
    // Alice's first edit reaches the authority, which merges it, but she
    // writes again before she hears back: her second is built on her first
    // as she sent it.
    net.connect(0);
    net.db(0).set_remote_link_hint(RemoteLinkHint::Live);
    block_on(net.db(0).tick()).unwrap();
    block_on(net.core.tick()).unwrap();
    assert_eq!(Net::read(&net.core, row, "body"), "Hello! world");
    let second = net.edit(0, row, &[("body", "Hello big world")]);
    net.restart_core();
    net.settle(0, first);
    net.settle(0, second);
    // Over the merged value instead, bob's "!" would be lost.
    net.assert_everywhere(row, "body", "Hello! big world");
    assert_eq!(
        Net::history(&net.core, row),
        ["Hello", "Hello!", "Hello! world", "Hello! big world"]
    );
}

/// A chain link that left the column alone stores the value the authority
/// held, edits its writer never saw included: the next link's base passes
/// it by, to what the writer last saw of the column.
#[test]
fn a_chain_link_that_left_the_column_alone_hides_no_missed_edit() {
    let mut net = Net::new(2);
    let row = net.seed(0, &[("body", "Hello"), ("title", "seed")]);
    net.offline(0);
    let title = net.edit(0, row, &[("title", "greeting")]);
    let body = net.edit(0, row, &[("body", "Hello world")]);
    let bob = net.edit(1, row, &[("body", "Oh Hello")]);
    net.settle(1, bob);
    net.online(0);
    net.settle(0, title);
    net.settle(0, body);
    net.assert_everywhere(row, "body", "Oh Hello world");
    net.assert_everywhere(row, "title", "greeting");
}

/// The same chain where the writer went on to merge: the second link
/// authored the column, so the third merges over it as sent.
#[test]
fn a_chain_merges_over_the_last_link_that_authored_the_column() {
    let mut net = Net::new(2);
    let row = net.seed(0, &[("body", "one"), ("title", "seed")]);
    net.offline(0);
    let first = net.edit(0, row, &[("body", "one two")]);
    let title = net.edit(0, row, &[("title", "numbers")]);
    let third = net.edit(0, row, &[("body", "one two three")]);
    let bob = net.edit(1, row, &[("body", "zero one")]);
    net.settle(1, bob);
    net.online(0);
    for tx in [first, title, third] {
        net.settle(0, tx);
    }
    net.assert_everywhere(row, "body", "zero one two three");
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
    // The merged value is stored as a local write's would be: chunked.
    let stored = net.core.stored_bytes_for_test("docs", row, "body").unwrap();
    assert!(stored < 64 * 1024, "{stored} bytes stored inline");
    // Bob's write as sent is kept, chunked too: the seed, alice's version,
    // the merge and bob's value as sent each hold a tree.
    assert_eq!(net.core.as_sent_versions_for_test().unwrap(), 1);
    let (_, roots) = net.core.large_value_nodes_for_test().unwrap();
    assert_eq!(roots, 4);
    // Bob's next write is built on the merge, settled: his value as sent
    // goes, and with it its tree's reference.
    let next = net.edit(1, row, &[("body", "short")]);
    net.settle(1, next);
    assert_eq!(net.core.as_sent_versions_for_test().unwrap(), 0);
    let (_, roots) = net.core.large_value_nodes_for_test().unwrap();
    assert_eq!(roots, 3);
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
