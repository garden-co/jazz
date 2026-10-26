//! Gateway spike: a storeless, per-connection proxy between Jazz clients
//! and several shard authorities, in local simulation.
//!
//! The gateway holds nothing durable. Per client connection it keeps the
//! client's identity and claims, its declared schema, which shard each of
//! its subscriptions, reads and commits went to, and one lazily opened link
//! per shard. Shards check every read and write as the client's session.
//!
//! Partitions: the first 4 bytes of a row id are its partition tag; rows of
//! one partition (a room and everything in it) share it, and the tag picks
//! the shard. Server seqs cross the gateway tagged with their shard in bits
//! 44..52, so a client's per-row versions and a single-shard subscription's
//! `since` stay meaningful without gateway state.

mod common;

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::rc::Rc;

use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, SubscriberId, SyncMessage, Transport,
    WireKind, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::wire::TransportError;

use common::{compile_schema, exists, outer_eq, read_and_allow_all_writes, session_eq};

// ---- transport ---------------------------------------------------------------

type Queue = Rc<RefCell<VecDeque<SyncMessage>>>;

struct Duplex {
    outbound: Queue,
    inbound: Queue,
    closed: Rc<RefCell<bool>>,
}

impl Transport for Duplex {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        if *self.closed.borrow() {
            return Err(TransportError::Failed("closed".into()));
        }
        self.outbound.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }

    fn take_failure(&mut self) -> Option<TransportError> {
        if *self.closed.borrow() {
            return Some(TransportError::Failed("closed".into()));
        }
        None
    }
}

/// A socket: two ends and a switch that closes both.
struct Socket {
    closed: Rc<RefCell<bool>>,
}

fn socket() -> (Box<dyn Transport>, Box<dyn Transport>, Socket) {
    let a_to_b: Queue = Rc::default();
    let b_to_a: Queue = Rc::default();
    let closed = Rc::new(RefCell::new(false));
    let a = Duplex {
        outbound: a_to_b.clone(),
        inbound: b_to_a.clone(),
        closed: closed.clone(),
    };
    let b = Duplex {
        outbound: b_to_a.clone(),
        inbound: a_to_b.clone(),
        closed: closed.clone(),
    };
    (Box::new(a), Box::new(b), Socket { closed })
}

// ---- partitions and seq tags -------------------------------------------------

const TAG_SHIFT: u32 = 44;
const TAG_MASK: u64 = 0xff << TAG_SHIFT;
const LOCAL_BASE: u64 = 1 << 52;

fn tag_of(id: &[u8; 16]) -> u32 {
    u32::from_be_bytes([id[0], id[1], id[2], id[3]])
}

fn shard_of_tag(tag: u32, shards: usize) -> usize {
    tag as usize % shards
}

fn tag_seq(shard: usize, seq: u64) -> u64 {
    if seq >= LOCAL_BASE || seq == 0 {
        return seq;
    }
    assert!(seq < 1 << TAG_SHIFT, "server seq past the tag bits");
    ((shard as u64) << TAG_SHIFT) | seq
}

/// The shard a tagged seq came from, and the shard's own seq.
fn untag_seq(seq: u64) -> (usize, u64) {
    if seq >= LOCAL_BASE || seq == 0 {
        return (usize::MAX, seq);
    }
    (((seq & TAG_MASK) >> TAG_SHIFT) as usize, seq & !TAG_MASK)
}

// ---- the gateway ---------------------------------------------------------------

/// Why a client's message did not go anywhere.
#[derive(Debug, Default, Clone)]
struct GatewayStats {
    up: usize,
    down: usize,
    links_opened: usize,
    refused: Vec<String>,
}

struct ShardLink {
    transport: Box<dyn Transport>,
    subscriber: SubscriberId,
    socket: Socket,
}

/// One client connection.
struct Conn {
    client: Box<dyn Transport>,
    identity: AuthorSubject,
    claims: Option<BTreeMap<String, Value>>,
    schema: Option<SyncMessage>,
    links: HashMap<usize, ShardLink>,
    /// Where each subscription, read and commit of this client went.
    subs: HashMap<u64, usize>,
    fetches: HashMap<u64, usize>,
    txs: HashMap<u64, usize>,
}

struct Gateway {
    shards: Rc<Vec<Db>>,
    conns: Vec<Conn>,
    stats: GatewayStats,
}

impl Gateway {
    fn new(shards: Rc<Vec<Db>>) -> Self {
        Self {
            shards,
            conns: Vec::new(),
            stats: GatewayStats::default(),
        }
    }

    /// A client connected and authenticated as `identity`.
    fn accept(&mut self, client: Box<dyn Transport>, identity: AuthorSubject) {
        self.conns.push(Conn {
            client,
            identity,
            claims: None,
            schema: None,
            links: HashMap::new(),
            subs: HashMap::new(),
            fetches: HashMap::new(),
            txs: HashMap::new(),
        });
    }

    /// The gateway instance goes away (a function recycled): every shard
    /// session it opened ends, as a closed socket would.
    fn shutdown(self) {
        for conn in self.conns {
            for (shard, link) in conn.links {
                *link.socket.closed.borrow_mut() = true;
                self.shards[shard].disconnect_subscriber(link.subscriber);
            }
        }
    }

    fn link<'a>(
        shards: &[Db],
        stats: &mut GatewayStats,
        conn: &'a mut Conn,
        shard: usize,
    ) -> &'a mut ShardLink {
        if !conn.links.contains_key(&shard) {
            let (mut ours, theirs, socket) = socket();
            let subscriber = shards[shard].accept_subscriber_with_claims(
                theirs,
                conn.identity,
                conn.claims.clone(),
            );
            if let Some(schema) = &conn.schema {
                ours.send(schema.clone()).unwrap();
            }
            stats.links_opened += 1;
            conn.links.insert(
                shard,
                ShardLink {
                    transport: ours,
                    subscriber,
                    socket,
                },
            );
        }
        conn.links.get_mut(&shard).unwrap()
    }

    fn tick(&mut self) {
        let n = self.shards.len();
        let shards = self.shards.clone();
        for conn in &mut self.conns {
            // Client → shards.
            while let Some(message) = conn.client.try_recv() {
                self.stats.up += 1;
                for (shard, message) in route_up(conn, message, n, &mut self.stats) {
                    Self::link(&shards, &mut self.stats, conn, shard)
                        .transport
                        .send(message)
                        .unwrap();
                }
            }
            // Shards → client.
            let mut down = Vec::new();
            for (&shard, link) in conn.links.iter_mut() {
                while let Some(message) = link.transport.try_recv() {
                    down.push(retag_down(shard, message));
                }
            }
            for message in down {
                self.stats.down += 1;
                let _ = conn.client.send(message);
            }
        }
    }
}

/// The shard a query is pinned to: an equality on a uuid column (the row id
/// or a reference) names a partition.
fn pinned_shard(query: &Query, binding: &BTreeMap<String, Value>, shards: usize) -> Option<usize> {
    use jazz::query::{Operand, Predicate};
    let uuid = |operand: &Operand| match operand {
        Operand::Literal(Value::Uuid(u)) => Some(*u.as_bytes()),
        Operand::Param(name) => match binding.get(name) {
            Some(Value::Uuid(u)) => Some(*u.as_bytes()),
            _ => None,
        },
        _ => None,
    };
    query.filters.iter().find_map(|predicate| match predicate {
        Predicate::Eq(Operand::Column(_), other) | Predicate::Eq(other, Operand::Column(_)) => {
            uuid(other).map(|id| shard_of_tag(tag_of(&id), shards))
        }
        _ => None,
    })
}

/// Where a client message goes, rewritten for that shard.
fn route_up(
    conn: &mut Conn,
    message: SyncMessage,
    shards: usize,
    stats: &mut GatewayStats,
) -> Vec<(usize, SyncMessage)> {
    match message {
        SyncMessage::Schema { .. } => {
            conn.schema = Some(message.clone());
            conn.links.keys().map(|&s| (s, message.clone())).collect()
        }
        SyncMessage::Hold { rows } => {
            // Each held row goes to the shard its id names; a version from
            // another shard (the row moved) is not sent, so the shard ships
            // the row afresh.
            let mut per: BTreeMap<usize, Vec<_>> = BTreeMap::new();
            for (table, id, key, seq) in rows {
                let shard = shard_of_tag(tag_of(&id), shards);
                let (from, seq) = untag_seq(seq);
                if from == shard {
                    per.entry(shard).or_default().push((table, id, key, seq));
                }
            }
            per.into_iter()
                .map(|(s, rows)| (s, SyncMessage::Hold { rows }))
                .collect()
        }
        SyncMessage::Subscribe {
            id,
            query,
            binding,
            since,
            include_deleted,
            tier,
            wait,
        } => {
            let Some(shard) = pinned_shard(&query, &binding, shards) else {
                stats.refused.push(format!(
                    "subscription {id} on {} names no partition",
                    query.table
                ));
                let _ = conn.client.send(SyncMessage::Refused {
                    id,
                    code: jazz::db::ErrorCode::Query,
                    message: "a query across partitions is not supported by this gateway yet"
                        .into(),
                });
                return Vec::new();
            };
            conn.subs.insert(id, shard);
            let since = since.and_then(|s| {
                let (from, seq) = untag_seq(s);
                (from == shard).then_some(seq)
            });
            vec![(
                shard,
                SyncMessage::Subscribe {
                    id,
                    query,
                    binding,
                    since,
                    include_deleted,
                    tier,
                    wait,
                },
            )]
        }
        SyncMessage::Unsubscribe { id } => match conn.subs.remove(&id) {
            Some(shard) => vec![(shard, SyncMessage::Unsubscribe { id })],
            None => Vec::new(),
        },
        SyncMessage::Fetch {
            id,
            query,
            binding,
            at,
            include_deleted,
            tier,
            wait,
        } => {
            let Some(shard) = pinned_shard(&query, &binding, shards) else {
                stats
                    .refused
                    .push(format!("read {id} on {} names no partition", query.table));
                let _ = conn.client.send(SyncMessage::Refused {
                    id,
                    code: jazz::db::ErrorCode::Query,
                    message: "a read across partitions is not supported by this gateway yet".into(),
                });
                return Vec::new();
            };
            conn.fetches.insert(id, shard);
            let at = at.map(|s| untag_seq(s).1);
            vec![(
                shard,
                SyncMessage::Fetch {
                    id,
                    query,
                    binding,
                    at,
                    include_deleted,
                    tier,
                    wait,
                },
            )]
        }
        SyncMessage::Commit {
            node,
            tx,
            kind,
            ops,
            author,
            session,
            expect,
        } => {
            let touched: HashSet<usize> = ops
                .iter()
                .map(|op| shard_of_tag(tag_of(op_row(op)), shards))
                .collect();
            if touched.len() != 1 {
                stats
                    .refused
                    .push(format!("commit {tx} touches {} shards", touched.len()));
                let _ = conn.client.send(SyncMessage::Fate {
                    tx,
                    through: 0,
                    result: Err(jazz::db::WireError::Rejected(
                        "a transaction across partitions is not supported by this gateway yet"
                            .into(),
                    )),
                    held: Vec::new(),
                    global: true,
                });
                return Vec::new();
            }
            let shard = *touched.iter().next().unwrap();
            conn.txs.insert(tx, shard);
            let kind = match kind {
                WireKind::Mergeable => WireKind::Mergeable,
                WireKind::Serializable {
                    snapshot,
                    reads,
                    own,
                } => WireKind::Serializable {
                    snapshot: untag_seq(snapshot).1,
                    reads: reads
                        .into_iter()
                        .map(|mut read| {
                            for (_, seq) in read.rows.iter_mut() {
                                *seq = untag_seq(*seq).1;
                            }
                            read
                        })
                        .collect(),
                    own,
                },
            };
            vec![(
                shard,
                SyncMessage::Commit {
                    node,
                    tx,
                    kind,
                    ops,
                    author,
                    session,
                    expect,
                },
            )]
        }
        other => {
            stats
                .refused
                .push(format!("not routed: {}", variant(&other)));
            Vec::new()
        }
    }
}

fn op_row(op: &jazz::db::WireOp) -> &[u8; 16] {
    use jazz::db::WireOp::*;
    match op {
        Insert { row, .. }
        | Update { row, .. }
        | Upsert { row, .. }
        | Delete { row, .. }
        | Restore { row, .. }
        | Move { row, .. } => row,
    }
}

fn variant(message: &SyncMessage) -> String {
    format!("{message:?}")
        .split([' ', '{', '('])
        .next()
        .unwrap_or_default()
        .to_owned()
}

/// A shard's message, with its seqs tagged by the shard.
fn retag_down(shard: usize, message: SyncMessage) -> SyncMessage {
    let images = |images: Vec<jazz::db::Image>| -> Vec<jazz::db::Image> {
        images
            .into_iter()
            .map(|mut image| {
                image.seq = tag_seq(shard, image.seq);
                image
            })
            .collect()
    };
    match message {
        SyncMessage::Rows { images: i } => SyncMessage::Rows { images: images(i) },
        SyncMessage::RowsPart { images: i } => SyncMessage::RowsPart { images: images(i) },
        SyncMessage::Settled {
            id,
            through,
            window,
            global,
        } => SyncMessage::Settled {
            id,
            through: tag_seq(shard, through),
            window,
            global,
        },
        SyncMessage::Answer { id, through, rows } => SyncMessage::Answer {
            id,
            through: tag_seq(shard, through),
            rows,
        },
        SyncMessage::Fate {
            tx,
            through,
            result,
            held,
            global,
        } => SyncMessage::Fate {
            tx,
            through: tag_seq(shard, through),
            result,
            held,
            global,
        },
        SyncMessage::Origins { commits } => SyncMessage::Origins {
            commits: commits
                .into_iter()
                .map(|(seq, node, tx)| (tag_seq(shard, seq), node, tx))
                .collect(),
        },
        other => other,
    }
}

// ---- the app -----------------------------------------------------------------------

const SHARDS: usize = 3;

fn schema() -> JazzSchema {
    let me = ["user", "identity", "subject"];
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("rooms")
                    .column("name", ColumnType::Text)
                    .policies(read_and_allow_all_writes(exists(
                        "memberships",
                        vec![outer_eq("room", "id"), session_eq("member", &me)],
                    ))),
            )
            .table(
                TableSchemaBuilder::new("memberships")
                    .fk_column("room", "rooms")
                    .column("member", ColumnType::Text)
                    .index_only(["member"])
                    .policies(read_and_allow_all_writes(session_eq("member", &me))),
            )
            .table(
                TableSchemaBuilder::new("messages")
                    .fk_column("room", "rooms")
                    .column("body", ColumnType::Text)
                    .column("sent_at", ColumnType::BigInt)
                    .composite_index(["room", "sent_at"])
                    .policies(read_and_allow_all_writes(exists(
                        "memberships",
                        vec![outer_eq("room", "room"), session_eq("member", &me)],
                    ))),
            )
            .build(),
    )
}

fn user(n: u8) -> AuthorSubject {
    AuthorSubject::for_test_bytes([n; 16])
}

fn subject(n: u8) -> String {
    user(n).principal_parts().1
}

/// A row id in partition `tag`.
fn row(tag: u32, n: u64) -> RowUuid {
    let mut bytes = [0u8; 16];
    bytes[..4].copy_from_slice(&tag.to_be_bytes());
    bytes[6] = 0x70; // uuid version nibble, so it reads as a v7-shaped id
    bytes[8..].copy_from_slice(&n.to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn text(s: &str) -> Value {
    Value::String(s.to_owned())
}

fn cells(pairs: &[(&str, Value)]) -> BTreeMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect()
}

struct World {
    shards: Rc<Vec<Db>>,
    gateway: Gateway,
    clients: Vec<(Db, AuthorSubject)>,
}

impl World {
    fn new() -> Self {
        let shards: Vec<Db> = (0..SHARDS)
            .map(|s| {
                let db = block_on(Db::open_history_complete(DbConfig::new(
                    schema(),
                    DbIdentity {
                        node: NodeUuid::from_bytes([0xa0 + s as u8; 16]),
                        author: AuthorSubject::SYSTEM,
                    },
                )))
                .unwrap();
                db.set_authority();
                db
            })
            .collect();
        let shards = Rc::new(shards);
        Self {
            gateway: Gateway::new(shards.clone()),
            shards,
            clients: Vec::new(),
        }
    }

    fn client(&mut self, n: u8) -> usize {
        let db = block_on(Db::open(DbConfig::new(
            schema(),
            DbIdentity {
                node: NodeUuid::from_bytes([n; 16]),
                author: user(n),
            },
        )))
        .unwrap();
        self.clients.push((db, user(n)));
        let index = self.clients.len() - 1;
        self.connect(index);
        index
    }

    fn connect(&mut self, index: usize) {
        let (client_side, gateway_side, _socket) = socket();
        let (db, identity) = &self.clients[index];
        let _ = block_on(db.connect_upstream(client_side));
        self.gateway.accept(gateway_side, *identity);
    }

    fn pump(&mut self) {
        for _ in 0..12 {
            for (db, _) in &self.clients {
                block_on(db.tick()).unwrap();
            }
            self.gateway.tick();
            for shard in self.shards.iter() {
                block_on(shard.tick()).unwrap();
            }
            self.gateway.tick();
        }
    }

    /// Seed a room with members directly on its shard.
    fn room(&self, tag: u32, members: &[u8]) {
        let shard = &self.shards[shard_of_tag(tag, SHARDS)];
        shard
            .seed_settled_mergeable_for_bootstrap(
                "rooms",
                row(tag, 0),
                AuthorSubject::SYSTEM,
                cells(&[("name", text(&format!("room {tag}")))]),
            )
            .unwrap();
        for (i, m) in members.iter().enumerate() {
            shard
                .seed_settled_mergeable_for_bootstrap(
                    "memberships",
                    row(tag, 1 + i as u64),
                    AuthorSubject::SYSTEM,
                    cells(&[
                        ("room", Value::Uuid(row(tag, 0).0)),
                        ("member", text(&subject(*m))),
                    ]),
                )
                .unwrap();
        }
        block_on(shard.tick()).unwrap();
    }
}

fn room_messages(tag: u32) -> Query {
    Query::from("messages")
        .filter(eq(col("room"), lit(Value::Uuid(row(tag, 0).0))))
        .order_by("sent_at", OrderDirection::Desc)
        .limit(50)
}

fn bodies(db: &Db, tag: u32) -> Vec<String> {
    let prepared = db.prepare_query(&room_messages(tag)).unwrap();
    let mut out: Vec<String> = block_on(db.all(&prepared, ReadOpts::default()))
        .unwrap()
        .iter()
        .filter_map(|r| {
            r.cells()
                .find(|(n, _)| *n == "body")
                .and_then(|(_, v)| match v {
                    Some(Value::String(s)) => Some(s.clone()),
                    _ => None,
                })
        })
        .collect();
    out.sort();
    out
}

fn post(db: &Db, tag: u32, n: u64, body: &str) {
    block_on(db.insert(
        "messages",
        cells(&[
            ("room", Value::Uuid(row(tag, 0).0)),
            ("body", text(body)),
            ("sent_at", Value::I64(n as i64)),
        ]),
        InsertOptions {
            row_id: Some(row(tag, 1000 + n)),
            ..Default::default()
        },
    ))
    .unwrap();
}

// Rooms with tags 0, 1, 2 land on shards 0, 1, 2.
const ROOM_A: u32 = 3; // shard 0
const ROOM_B: u32 = 4; // shard 1
const ROOM_C: u32 = 8; // shard 2

#[test]
fn routes_pinned_subscriptions_and_commits_to_their_shards() {
    let mut w = World::new();
    w.room(ROOM_A, &[1, 2]);
    w.room(ROOM_B, &[1, 2]);
    w.room(ROOM_C, &[2]);
    let alice = w.client(1);
    let bob = w.client(2);
    let mut subs = Vec::new();
    for (c, tag) in [
        (alice, ROOM_A),
        (alice, ROOM_B),
        (bob, ROOM_A),
        (bob, ROOM_B),
        (bob, ROOM_C),
    ] {
        let db = &w.clients[c].0;
        let prepared = db.prepare_query(&room_messages(tag)).unwrap();
        subs.push(block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap());
    }
    w.pump();
    post(&w.clients[alice].0, ROOM_A, 1, "a1 from alice");
    post(&w.clients[alice].0, ROOM_B, 2, "b1 from alice");
    post(&w.clients[bob].0, ROOM_C, 3, "c1 from bob");
    w.pump();
    assert_eq!(bodies(&w.clients[bob].0, ROOM_A), vec!["a1 from alice"]);
    assert_eq!(bodies(&w.clients[bob].0, ROOM_B), vec!["b1 from alice"]);
    assert_eq!(bodies(&w.clients[bob].0, ROOM_C), vec!["c1 from bob"]);
    // Each write landed on its room's shard only.
    let on = |shard: usize| {
        let p = w.shards[shard]
            .prepare_query(&Query::from("messages"))
            .unwrap();
        block_on(w.shards[shard].all(&p, ReadOpts::default()))
            .unwrap()
            .len()
    };
    assert_eq!((on(0), on(1), on(2)), (1, 1, 1));
    // Alice never touched room C's shard.
    assert_eq!(w.gateway.conns[0].links.len(), 2);
    assert!(
        w.gateway.stats.refused.is_empty(),
        "{:?}",
        w.gateway.stats.refused
    );
    println!("{:?}", w.gateway.stats);
}

#[test]
fn shards_enforce_read_policies_for_the_session() {
    let mut w = World::new();
    w.room(ROOM_C, &[2]); // alice (1) is not a member
    w.room(ROOM_A, &[1]);
    let alice = w.client(1);
    let bob = w.client(2);
    let db = &w.clients[alice].0;
    let prepared = db.prepare_query(&room_messages(ROOM_C)).unwrap();
    let _sub = block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap();
    w.pump();
    post(&w.clients[bob].0, ROOM_C, 1, "secret");
    w.pump();
    assert!(bodies(&w.clients[alice].0, ROOM_C).is_empty());
    assert_eq!(bodies(&w.clients[bob].0, ROOM_C), vec!["secret"]);
}

#[test]
fn a_cross_partition_commit_is_refused_whole() {
    let mut w = World::new();
    w.room(ROOM_A, &[1]);
    w.room(ROOM_B, &[1]);
    let alice = w.client(1);
    let db = &w.clients[alice].0;
    let tx = jazz::tools::OpenTransactionId::new();
    block_on(db.begin_mergeable(tx)).unwrap();
    for (tag, n) in [(ROOM_A, 1), (ROOM_B, 2)] {
        block_on(db.mergeable_tx_ref(tx).insert(
            "messages",
            cells(&[
                ("room", Value::Uuid(row(tag, 0).0)),
                ("body", text("both")),
                ("sent_at", Value::I64(n)),
            ]),
            InsertOptions {
                row_id: Some(row(tag, 1000 + n as u64)),
                ..Default::default()
            },
        ))
        .unwrap();
    }
    block_on(db.commit_mergeable_handle(tx)).unwrap();
    w.pump();
    let total: usize = w
        .shards
        .iter()
        .map(|s| {
            let p = s.prepare_query(&Query::from("messages")).unwrap();
            block_on(s.all(&p, ReadOpts::default())).unwrap().len()
        })
        .sum();
    assert_eq!(total, 0);
    assert_eq!(
        w.gateway.stats.refused.len(),
        1,
        "{:?}",
        w.gateway.stats.refused
    );
}

#[test]
fn a_fresh_gateway_instance_resumes_every_client() {
    let mut w = World::new();
    w.room(ROOM_A, &[1, 2]);
    w.room(ROOM_B, &[1, 2]);
    let alice = w.client(1);
    let bob = w.client(2);
    let mut subs = Vec::new();
    for c in [alice, bob] {
        for tag in [ROOM_A, ROOM_B] {
            let db = &w.clients[c].0;
            let prepared = db.prepare_query(&room_messages(tag)).unwrap();
            subs.push(block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap());
        }
    }
    w.pump();
    post(&w.clients[alice].0, ROOM_A, 1, "before");
    w.pump();
    assert_eq!(bodies(&w.clients[bob].0, ROOM_A), vec!["before"]);

    // The function instance is recycled: every socket drops, a new
    // instance with no memory of the old one takes the reconnects.
    let old = std::mem::replace(&mut w.gateway, Gateway::new(w.shards.clone()));
    old.shutdown();
    // While nobody is connected, alice writes offline in both rooms.
    post(&w.clients[alice].0, ROOM_A, 2, "offline a");
    post(&w.clients[alice].0, ROOM_B, 3, "offline b");
    for c in [alice, bob] {
        w.connect(c);
    }
    w.pump();
    assert_eq!(
        bodies(&w.clients[bob].0, ROOM_A),
        vec!["before", "offline a"]
    );
    assert_eq!(bodies(&w.clients[bob].0, ROOM_B), vec!["offline b"]);
    // Nothing was written twice (the shards dedupe resent commits).
    let p = w.shards[0].prepare_query(&Query::from("messages")).unwrap();
    assert_eq!(
        block_on(w.shards[0].all(&p, ReadOpts::default()))
            .unwrap()
            .len(),
        2
    );
    assert!(
        w.gateway.stats.refused.is_empty(),
        "{:?}",
        w.gateway.stats.refused
    );
    println!("after resume: {:?}", w.gateway.stats);
}

#[test]
fn an_unpinned_subscription_is_refused_not_silently_partial() {
    let mut w = World::new();
    w.room(ROOM_A, &[1]);
    let alice = w.client(1);
    let db = &w.clients[alice].0;
    let prepared = db.prepare_query(&Query::from("memberships")).unwrap();
    let _sub = block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap();
    w.pump();
    assert_eq!(
        w.gateway.stats.refused.len(),
        1,
        "{:?}",
        w.gateway.stats.refused
    );
}
