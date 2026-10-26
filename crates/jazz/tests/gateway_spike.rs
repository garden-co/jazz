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
    /// Subscriptions and reads fanned out to several shards.
    multi: HashMap<u64, Multi>,
    multi_fetches: HashMap<u64, Multi>,
    /// Cross-shard commits in flight, by the client's tx, and the advice
    /// ids the gateway asked for them.
    cross: HashMap<u64, Cross>,
    advice: HashMap<u64, u64>,
    next_advice: u64,
    cross_committed: usize,
    cross_aborted: usize,
    /// Test fault: stop after the commit point (the decider's part and
    /// tx record), as an instance that dies there would.
    crash_after_commit_point: bool,
}

/// A commit across partitions: admission (permission advice for every
/// write on its shard), then the commit point (the decider's part plus a
/// tx record naming every other part, in one commit on the decider), then
/// the other parts as ordinary commits. Every step after the commit point
/// is an idempotent resend, so any instance (or a sweeper) can finish it.
struct Cross {
    node: [u8; 16],
    author: Option<String>,
    session: Option<BTreeMap<String, Value>>,
    parts: BTreeMap<usize, Vec<jazz::db::WireOp>>,
    decider: usize,
    advice_pending: HashSet<u64>,
    denied: bool,
    phase: CrossPhase,
    /// Participants whose part is applied.
    applied: HashSet<usize>,
    decider_through: u64,
}

#[derive(PartialEq, Debug)]
enum CrossPhase {
    Admission,
    CommitPoint,
    Parts,
}

/// The tx record the decider stores with its part: the other parts and
/// who wrote them, so any instance can resend them.
#[derive(serde::Serialize, serde::Deserialize)]
struct TxRecord {
    node: [u8; 16],
    tx: u64,
    author: Option<String>,
    identity: String,
    session: Option<BTreeMap<String, Value>>,
    parts: Vec<(usize, Vec<jazz::db::WireOp>)>,
}

fn advise_action(op: &jazz::db::WireOp) -> Option<jazz::protocol::PermissionAdviceAction> {
    use jazz::db::WireOp::*;
    use jazz::protocol::PermissionAdviceAction as A;
    Some(match op {
        Insert { table, cells, .. } | Upsert { table, cells, .. } => A::Insert {
            table: table.clone(),
            cells: cells.clone(),
        },
        Update {
            table, row, cells, ..
        } => A::Update {
            table: table.clone(),
            row: RowUuid::from_bytes(*row),
            patch: cells.clone(),
        },
        Delete { table, row, .. } => A::Delete {
            table: table.clone(),
            row: RowUuid::from_bytes(*row),
        },
        _ => return None,
    })
}

/// A query fanned out to several shards: each shard's latest answer, and
/// the sort keys of the rows in them, to merge into the client's result.
struct Multi {
    table: String,
    order: Option<(String, bool)>,
    limit: Option<usize>,
    /// Per shard: (rows of its latest window or answer, through, global);
    /// `None` until it first answered.
    answers: BTreeMap<usize, Option<(Vec<[u8; 16]>, u64, bool)>>,
    keys: HashMap<[u8; 16], jazz::db::SyncCell>,
    /// A limited, ordered subscription asks each shard for its quota of
    /// rows rather than the whole limit, and raises a shard's quota when
    /// that shard may hold rows the merged window needs.
    quota: BTreeMap<usize, usize>,
    /// The client's `Subscribe`, to ask a shard again with a new quota.
    template: Option<SyncMessage>,
    /// Times a shard was asked again for more rows.
    raised: usize,
}

thread_local! {
    /// Whether fanned-out windows use quotas (off: every shard ships the
    /// whole limit, the naive merge).
    static QUOTAS: RefCell<bool> = const { RefCell::new(true) };
}

/// Quota per shard for `k` rows over `n` shards: the mean share plus two
/// standard deviations (B2's adaptive quota with no history yet).
fn initial_quota(k: usize, n: usize) -> usize {
    if !QUOTAS.with(|q| *q.borrow()) {
        return k;
    }
    let mean = k as f64 / n as f64;
    ((mean + 2.0 * mean.sqrt()).ceil() as usize).clamp(1, k)
}

impl Multi {
    fn new(query: &Query, shards: &[usize]) -> Self {
        Self {
            table: query.table.clone(),
            order: query.order_by.first().map(|o| {
                (
                    o.column.clone(),
                    matches!(o.direction, OrderDirection::Desc),
                )
            }),
            limit: query.limit,
            answers: shards.iter().map(|&s| (s, None)).collect(),
            keys: HashMap::new(),
            quota: BTreeMap::new(),
            template: None,
            raised: 0,
        }
    }

    /// A fanned-out subscription: quotas when it is limited and ordered.
    fn subscription(template: SyncMessage, shards: &[usize]) -> Self {
        let SyncMessage::Subscribe { query, .. } = &template else {
            unreachable!()
        };
        let mut multi = Self::new(query, shards);
        if let (Some(k), Some(_)) = (multi.limit, &multi.order) {
            let q = initial_quota(k, shards.len());
            multi.quota = shards.iter().map(|&s| (s, q)).collect();
        }
        multi.template = Some(template);
        multi
    }

    /// The `Subscribe` shard `shard` gets: the client's, at its quota.
    fn ask(&self, shard: usize) -> SyncMessage {
        let mut message = self.template.clone().unwrap();
        if let SyncMessage::Subscribe { query, since, .. } = &mut message {
            *since = None;
            if let Some(q) = self.quota.get(&shard) {
                query.limit = Some(*q);
            }
        }
        message
    }

    fn sort(&self, rows: &mut [[u8; 16]]) {
        if let Some((_, desc)) = &self.order {
            let keys = &self.keys;
            rows.sort_by(|a, b| {
                let ord = cmp_cells(keys.get(a), keys.get(b)).then(a.cmp(b));
                if *desc { ord.reverse() } else { ord }
            });
        }
    }

    /// Shards whose full quota window ends at or before the merged
    /// window's last row (or whose rows the merged window runs short of):
    /// they may hold more rows that belong in it.
    fn short_shards(&self, merged: &[[u8; 16]]) -> Vec<usize> {
        let Some(k) = self.limit else {
            return Vec::new();
        };
        let mut short = Vec::new();
        for (&shard, answer) in &self.answers {
            let (Some((rows, _, _)), Some(&q)) = (answer, self.quota.get(&shard)) else {
                continue;
            };
            if q >= k || rows.len() < q {
                continue;
            }
            let mut own = rows.clone();
            self.sort(&mut own);
            let last = own.last().unwrap();
            let needed = merged.len() < k || merged.contains(last);
            if needed {
                short.push(shard);
            }
        }
        short
    }

    fn record(&mut self, image: &jazz::db::Image) {
        if image.table != self.table {
            return;
        }
        if let Some((column, _)) = &self.order
            && let Some((_, cell)) = image.cells.iter().find(|(n, _)| n == column)
        {
            self.keys.insert(image.id, cell.clone());
        }
    }

    /// Every shard answered: the merged rows (ordered and cut to the
    /// limit when the query has them), the highest tagged through, and
    /// whether every answer was global.
    fn merged(&mut self) -> Option<(Vec<[u8; 16]>, u64, bool)> {
        let mut rows = Vec::new();
        let mut through = 0;
        let mut global = true;
        for answer in self.answers.values() {
            let (r, t, g) = answer.as_ref()?;
            rows.extend_from_slice(r);
            through = through.max(*t);
            global &= *g;
        }
        self.sort(&mut rows);
        if let Some(limit) = self.limit {
            rows.truncate(limit);
        }
        // Keys of rows no shard window holds any more are dropped.
        let live: HashSet<[u8; 16]> = self
            .answers
            .values()
            .flatten()
            .flat_map(|(r, _, _)| r.iter().copied())
            .collect();
        self.keys.retain(|id, _| live.contains(id));
        Some((rows, through, global))
    }
}

fn cmp_cells(a: Option<&jazz::db::SyncCell>, b: Option<&jazz::db::SyncCell>) -> std::cmp::Ordering {
    use jazz::db::SyncCell::*;
    use std::cmp::Ordering;
    let rank = |c: Option<&jazz::db::SyncCell>| match c {
        None | Some(Null) => 0,
        Some(Int(_)) | Some(Real(_)) => 1,
        Some(Text(_)) => 2,
        Some(Blob(_)) => 3,
    };
    match (a, b) {
        (Some(Int(x)), Some(Int(y))) => x.cmp(y),
        (Some(Real(x)), Some(Real(y))) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (Some(Int(x)), Some(Real(y))) => (*x as f64).partial_cmp(y).unwrap_or(Ordering::Equal),
        (Some(Real(x)), Some(Int(y))) => x.partial_cmp(&(*y as f64)).unwrap_or(Ordering::Equal),
        (Some(Text(x)), Some(Text(y))) | (Some(Blob(x)), Some(Blob(y))) => x.cmp(y),
        _ => rank(a).cmp(&rank(b)),
    }
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
            multi: HashMap::new(),
            multi_fetches: HashMap::new(),
            cross: HashMap::new(),
            advice: HashMap::new(),
            next_advice: 1 << 60,
            cross_committed: 0,
            cross_aborted: 0,
            crash_after_commit_point: false,
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
            let mut arrived = Vec::new();
            for (&shard, link) in conn.links.iter_mut() {
                while let Some(message) = link.transport.try_recv() {
                    arrived.push((shard, retag_down(shard, message)));
                }
            }
            let mut again = Vec::new();
            for (shard, message) in arrived {
                down.extend(merge_down(conn, shard, message, &mut again));
            }
            for (shard, message) in again {
                Self::link(&shards, &mut self.stats, conn, shard)
                    .transport
                    .send(message)
                    .unwrap();
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
                if let Some(why) = unmergeable(&query) {
                    stats.refused.push(format!("subscription {id}: {why}"));
                    let _ = conn.client.send(SyncMessage::Refused {
                        id,
                        code: jazz::db::ErrorCode::Query,
                        message: why,
                    });
                    return Vec::new();
                }
                // Fan out to every shard the query may reach. A multi-shard
                // `since` is a vector the client cannot hold yet, so the
                // shards ship the result afresh.
                let reach = reach(&query, shards);
                let template = SyncMessage::Subscribe {
                    id,
                    query,
                    binding,
                    since: None,
                    include_deleted,
                    tier,
                    wait,
                };
                let multi = Multi::subscription(template, &reach);
                let asks = reach.iter().map(|&s| (s, multi.ask(s))).collect();
                conn.multi.insert(id, multi);
                return asks;
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
        SyncMessage::Unsubscribe { id } => {
            if let Some(multi) = conn.multi.remove(&id) {
                return multi
                    .answers
                    .keys()
                    .map(|&s| (s, SyncMessage::Unsubscribe { id }))
                    .collect();
            }
            match conn.subs.remove(&id) {
                Some(shard) => vec![(shard, SyncMessage::Unsubscribe { id })],
                None => Vec::new(),
            }
        }
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
                let why = unmergeable(&query).or_else(|| {
                    at.map(|_| "a historical read across partitions is not supported yet".into())
                });
                if let Some(why) = why {
                    stats.refused.push(format!("read {id}: {why}"));
                    let _ = conn.client.send(SyncMessage::Refused {
                        id,
                        code: jazz::db::ErrorCode::Query,
                        message: why,
                    });
                    return Vec::new();
                }
                let reach = reach(&query, shards);
                conn.multi_fetches.insert(id, Multi::new(&query, &reach));
                return reach
                    .into_iter()
                    .map(|s| {
                        let message = SyncMessage::Fetch {
                            id,
                            query: query.clone(),
                            binding: binding.clone(),
                            at: None,
                            include_deleted,
                            tier,
                            wait,
                        };
                        (s, message)
                    })
                    .collect();
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
            if touched.len() > 1 {
                if !matches!(kind, WireKind::Mergeable) || std::env::var("GATEWAY_NO_CROSS").is_ok()
                {
                    stats
                        .refused
                        .push(format!("commit {tx} touches {} shards", touched.len()));
                    let _ = conn.client.send(SyncMessage::Fate {
                        tx,
                        through: 0,
                        result: Err(jazz::db::WireError::Rejected(
                            "a serializable transaction across partitions is not supported by this gateway yet"
                                .into(),
                        )),
                        held: Vec::new(),
                        global: true,
                    });
                    return Vec::new();
                }
                // A resend of a cross commit already in flight changes nothing.
                if conn.cross.contains_key(&tx) {
                    return Vec::new();
                }
                let mut parts: BTreeMap<usize, Vec<jazz::db::WireOp>> = BTreeMap::new();
                for op in ops {
                    parts
                        .entry(shard_of_tag(tag_of(op_row(&op)), shards))
                        .or_default()
                        .push(op);
                }
                let decider =
                    shard_of_tag(tag_of(op_row(&parts.values().next().unwrap()[0])), shards);
                let mut asks = Vec::new();
                let mut pending = HashSet::new();
                for (&s, ops) in &parts {
                    for op in ops {
                        if let Some(action) = advise_action(op) {
                            let id = conn.next_advice;
                            conn.next_advice += 1;
                            conn.advice.insert(id, tx);
                            pending.insert(id);
                            asks.push((
                                s,
                                SyncMessage::Advise {
                                    id,
                                    action,
                                    author: author.clone(),
                                    session: session.clone(),
                                },
                            ));
                        }
                    }
                }
                conn.cross.insert(
                    tx,
                    Cross {
                        node,
                        author,
                        session,
                        parts,
                        decider,
                        advice_pending: pending,
                        denied: false,
                        phase: CrossPhase::Admission,
                        applied: HashSet::new(),
                        decider_through: 0,
                    },
                );
                return asks;
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

/// The commit point: the decider's part plus the tx record, one commit.
fn commit_point(identity: AuthorSubject, tx: u64, cross: &Cross) -> SyncMessage {
    let record = TxRecord {
        node: cross.node,
        tx,
        author: cross.author.clone(),
        identity: identity.canonical().to_owned(),
        session: cross.session.clone(),
        parts: cross
            .parts
            .iter()
            .filter(|(s, _)| **s != cross.decider)
            .map(|(s, ops)| (*s, ops.clone()))
            .collect(),
    };
    let mut ops = cross.parts[&cross.decider].clone();
    let decider_tag = tag_of(op_row(&ops[0]));
    let target = match &ops[0] {
        jazz::db::WireOp::Insert { target, .. }
        | jazz::db::WireOp::Update { target, .. }
        | jazz::db::WireOp::Upsert { target, .. }
        | jazz::db::WireOp::Delete { target, .. }
        | jazz::db::WireOp::Restore { target, .. } => target.clone(),
        jazz::db::WireOp::Move { .. } => panic!("a move in a cross-partition commit"),
    };
    let mut id = row(decider_tag, 0).0.into_bytes();
    id[8..].copy_from_slice(&tx.to_be_bytes());
    id[4..6].copy_from_slice(&cross.node[..2]);
    ops.push(jazz::db::WireOp::Insert {
        table: "tx_records".into(),
        row: id,
        cells: cells(&[
            (
                "record",
                Value::String(serde_json::to_string(&record).unwrap()),
            ),
            ("state", text("committed")),
        ]),
        target,
        at: None,
    });
    part_commit(cross.node, tx, ops, &cross.author, &cross.session)
}

fn part_commit(
    node: [u8; 16],
    tx: u64,
    ops: Vec<jazz::db::WireOp>,
    author: &Option<String>,
    session: &Option<BTreeMap<String, Value>>,
) -> SyncMessage {
    SyncMessage::Commit {
        node,
        tx,
        kind: WireKind::Mergeable,
        ops,
        author: author.clone(),
        session: session.clone(),
        expect: Vec::new(),
    }
}

/// Why a query cannot be merged from per-shard answers, if it cannot.
fn unmergeable(query: &Query) -> Option<String> {
    if query.offset > 0 {
        return Some(
            "offset on a query across partitions is not supported; page by key instead".into(),
        );
    }
    if query.aggregate.is_some() {
        return Some("an aggregate across partitions is not supported yet".into());
    }
    if query.order_by.len() > 1 {
        return Some(
            "more than one sort key across partitions is not supported by this gateway yet".into(),
        );
    }
    None
}

/// The shards an unpinned query may reach. Every shard, for now; a
/// session's reach set from directories narrows it.
fn reach(_query: &Query, shards: usize) -> Vec<usize> {
    (0..shards).collect()
}

/// A shard's message for a fanned-out query is folded into the merged
/// result; anything else passes through.
fn merge_down(
    conn: &mut Conn,
    shard: usize,
    message: SyncMessage,
    up: &mut Vec<(usize, SyncMessage)>,
) -> Vec<SyncMessage> {
    match message {
        SyncMessage::Rows { ref images } | SyncMessage::RowsPart { ref images } => {
            for multi in conn
                .multi
                .values_mut()
                .chain(conn.multi_fetches.values_mut())
            {
                for image in images {
                    multi.record(image);
                }
            }
            vec![message]
        }
        SyncMessage::Settled {
            id,
            through,
            window,
            global,
        } if conn.multi.contains_key(&id) => {
            let multi = conn.multi.get_mut(&id).unwrap();
            // An unlimited query's Settled lists no window: its rows are
            // whatever the shard shipped, so nothing to merge.
            let rows = window.clone().unwrap_or_default();
            multi.answers.insert(shard, Some((rows, through, global)));
            match multi.merged() {
                Some((rows, _, _)) if !multi.short_shards(&rows).is_empty() => {
                    // Ask the short shards again for twice their quota; the
                    // client's result waits for their answers.
                    for s in multi.short_shards(&rows) {
                        let k = multi.limit.unwrap();
                        let q = multi.quota.get_mut(&s).unwrap();
                        *q = (*q * 2).min(k);
                        multi.answers.insert(s, None);
                        multi.raised += 1;
                        up.push((s, SyncMessage::Unsubscribe { id }));
                        up.push((s, multi.ask(s)));
                    }
                    Vec::new()
                }
                Some((rows, through, global)) => vec![SyncMessage::Settled {
                    id,
                    through,
                    window: window.is_some().then_some(rows),
                    global,
                }],
                None => Vec::new(),
            }
        }
        SyncMessage::Answer { id, through, rows } if conn.multi_fetches.contains_key(&id) => {
            let multi = conn.multi_fetches.get_mut(&id).unwrap();
            multi.answers.insert(shard, Some((rows, through, true)));
            match multi.merged() {
                Some((rows, through, _)) => {
                    conn.multi_fetches.remove(&id);
                    vec![SyncMessage::Answer { id, through, rows }]
                }
                None => Vec::new(),
            }
        }
        SyncMessage::Advice { id, advice } if conn.advice.contains_key(&id) => {
            let tx = conn.advice.remove(&id).unwrap();
            if let Some(cross) = conn.cross.get_mut(&tx) {
                cross.advice_pending.remove(&id);
                cross.denied |= !matches!(advice, jazz::protocol::PermissionAdvice::Allowed);
                if cross.advice_pending.is_empty() && cross.phase == CrossPhase::Admission {
                    if cross.denied {
                        // Refused at admission: nothing was written anywhere.
                        conn.cross.remove(&tx);
                        conn.cross_aborted += 1;
                        return vec![SyncMessage::Fate {
                            tx,
                            through: 0,
                            result: Err(jazz::db::WireError::Rejected(
                                "a write of this cross-partition transaction is not allowed".into(),
                            )),
                            held: Vec::new(),
                            global: true,
                        }];
                    }
                    cross.phase = CrossPhase::CommitPoint;
                    up.push((cross.decider, commit_point(conn.identity, tx, cross)));
                }
            }
            Vec::new()
        }
        SyncMessage::Fate {
            tx,
            through,
            ref result,
            ..
        } if conn.cross.contains_key(&tx) => {
            let cross = conn.cross.get_mut(&tx).unwrap();
            match (&cross.phase, result) {
                (CrossPhase::CommitPoint, Ok(())) if shard == cross.decider => {
                    cross.decider_through = through;
                    cross.applied.insert(shard);
                    cross.phase = CrossPhase::Parts;
                    if !conn.crash_after_commit_point {
                        for (&s, ops) in &cross.parts {
                            if s != cross.decider {
                                up.push((
                                    s,
                                    part_commit(
                                        cross.node,
                                        tx,
                                        ops.clone(),
                                        &cross.author,
                                        &cross.session,
                                    ),
                                ));
                            }
                        }
                    }
                }
                (CrossPhase::CommitPoint, Err(_)) => {
                    // The decider refused its part: nothing committed.
                    conn.cross.remove(&tx);
                    conn.cross_aborted += 1;
                    return vec![message];
                }
                (CrossPhase::Parts, Ok(())) => {
                    cross.applied.insert(shard);
                }
                (CrossPhase::Parts, Err(error)) => {
                    // After the commit point a part must apply; a refusal
                    // here is the gap admission leaves (policy changed in
                    // between). Reported, not retried.
                    panic!("part on shard {shard} of committed tx {tx} refused: {error:?}");
                }
                _ => {}
            }
            let cross = conn.cross.get(&tx).unwrap();
            if cross.applied.len() == cross.parts.len() {
                let through = cross.decider_through;
                conn.cross.remove(&tx);
                conn.cross_committed += 1;
                return vec![SyncMessage::Fate {
                    tx,
                    through,
                    result: Ok(()),
                    held: Vec::new(),
                    global: true,
                }];
            }
            Vec::new()
        }
        SyncMessage::Refused { id, .. }
            if conn.multi.contains_key(&id) || conn.multi_fetches.contains_key(&id) =>
        {
            // The first refusal ends the read; the other shards' answers
            // are dropped with it.
            conn.multi.remove(&id);
            conn.multi_fetches.remove(&id);
            vec![message]
        }
        other => vec![other],
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
            // The sharding layer's tx records (a system table in a product).
            .table(
                TableSchemaBuilder::new("tx_records")
                    .column("record", ColumnType::Text)
                    .column("state", ColumnType::Text)
                    .policies(common::allow_all_policies()),
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

fn messages_on(w: &World, shard: usize) -> usize {
    let p = w.shards[shard]
        .prepare_query(&Query::from("messages"))
        .unwrap();
    block_on(w.shards[shard].all(&p, ReadOpts::default()))
        .unwrap()
        .len()
}

/// Alice writes one message in room A (shard 0) and one in room B (shard
/// 1) in one mergeable transaction.
fn cross_write(w: &World, alice: usize) {
    let db = &w.clients[alice].0;
    let tx = jazz::tools::OpenTransactionId::new();
    block_on(db.begin_mergeable(tx)).unwrap();
    for (tag, n) in [(ROOM_A, 1), (ROOM_B, 2)] {
        block_on(db.mergeable_tx_ref(tx).insert(
            "messages",
            cells(&[
                ("room", Value::Uuid(row(tag, 0).0)),
                ("body", text(&format!("both {n}"))),
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
}

#[test]
fn a_cross_partition_commit_lands_whole() {
    let mut w = World::new();
    w.room(ROOM_A, &[1, 2]);
    w.room(ROOM_B, &[1, 2]);
    let alice = w.client(1);
    let bob = w.client(2);
    for tag in [ROOM_A, ROOM_B] {
        let db = &w.clients[bob].0;
        let prepared = db.prepare_query(&room_messages(tag)).unwrap();
        std::mem::forget(block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap());
    }
    w.pump();
    cross_write(&w, alice);
    w.pump();
    assert_eq!((messages_on(&w, 0), messages_on(&w, 1)), (1, 1));
    assert_eq!(bodies(&w.clients[bob].0, ROOM_A), vec!["both 1"]);
    assert_eq!(bodies(&w.clients[bob].0, ROOM_B), vec!["both 2"]);
    assert_eq!(w.gateway.conns[alice].cross_committed, 1);
    assert!(w.gateway.conns[alice].cross.is_empty());
}

/// Finish every committed cross-partition transaction the decider's tx
/// records name: resend each other part as its writer (idempotent on the
/// participant), then mark the record done. Any gateway instance can run
/// this; it needs no memory of the instance that started the commit.
fn sweep(shards: &[Db], decider: usize) -> usize {
    let records = shards[decider]
        .prepare_query(&Query::from("tx_records"))
        .unwrap();
    let mut resent = 0;
    for r in block_on(shards[decider].all(&records, ReadOpts::default())).unwrap() {
        let cell = |name: &str| match r
            .cells()
            .find(|(n, _)| *n == name)
            .and_then(|(_, v)| v.cloned())
        {
            Some(Value::String(s)) => s,
            _ => String::new(),
        };
        if cell("state") != "committed" {
            continue;
        }
        let record: TxRecord = serde_json::from_str(&cell("record")).unwrap();
        let identity = AuthorSubject::from_canonical(&record.identity).unwrap();
        for (shard, ops) in record.parts {
            let (mut ours, theirs, _socket) = socket();
            let subscriber = shards[shard].accept_subscriber_with_claims(
                theirs,
                identity,
                record.session.clone(),
            );
            ours.send(part_commit(
                record.node,
                record.tx,
                ops,
                &record.author,
                &record.session,
            ))
            .unwrap();
            let mut fate = None;
            for _ in 0..4 {
                block_on(shards[shard].tick()).unwrap();
                while let Some(m) = ours.try_recv() {
                    if let SyncMessage::Fate { result, .. } = m {
                        fate = Some(result);
                    }
                }
            }
            assert!(matches!(fate, Some(Ok(()))), "{fate:?}");
            shards[shard].disconnect_subscriber(subscriber);
            resent += 1;
        }
        block_on(shards[decider].update(
            "tx_records",
            r.row_uuid(),
            cells(&[("state", text("done"))]),
            Default::default(),
        ))
        .unwrap();
    }
    resent
}

#[test]
fn a_gateway_dying_after_the_commit_point_leaves_nothing_half_done() {
    let mut w = World::new();
    w.room(ROOM_A, &[1, 2]);
    w.room(ROOM_B, &[1, 2]);
    let alice = w.client(1);
    w.pump();
    w.gateway.conns[alice].crash_after_commit_point = true;
    cross_write(&w, alice);
    w.pump();
    // The commit point happened (decider's part and record), the other
    // part did not.
    assert_eq!((messages_on(&w, 0), messages_on(&w, 1)), (1, 0));
    let old = std::mem::replace(&mut w.gateway, Gateway::new(w.shards.clone()));
    old.shutdown();
    // A sweeper finishes it, and alice's reconnect resends the commit; both
    // land exactly once.
    assert_eq!(sweep(&w.shards, 0), 1);
    w.connect(alice);
    w.pump();
    assert_eq!((messages_on(&w, 0), messages_on(&w, 1)), (1, 1));
    // A second sweep finds nothing left to do.
    assert_eq!(sweep(&w.shards, 0), 0);
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

fn count(db: &Db, query: &Query) -> Vec<RowUuid> {
    let prepared = db.prepare_query(query).unwrap();
    block_on(db.all(&prepared, ReadOpts::default()))
        .unwrap()
        .iter()
        .map(|r| r.row_uuid())
        .collect()
}

fn feed(limit: usize) -> Query {
    Query::from("messages")
        .order_by("sent_at", OrderDirection::Desc)
        .limit(limit)
}

#[test]
fn an_unpinned_subscription_reaches_every_shard() {
    let mut w = World::new();
    w.room(ROOM_A, &[1]);
    w.room(ROOM_B, &[1]);
    w.room(ROOM_C, &[1, 2]);
    let alice = w.client(1);
    let db = &w.clients[alice].0;
    let prepared = db.prepare_query(&Query::from("memberships")).unwrap();
    let _sub = block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap();
    w.pump();
    // Her three memberships, one per shard; bob's stays on its shard.
    assert_eq!(
        count(&w.clients[alice].0, &Query::from("memberships")).len(),
        3
    );
    assert!(
        w.gateway.stats.refused.is_empty(),
        "{:?}",
        w.gateway.stats.refused
    );
}

#[test]
fn a_feed_across_shards_is_the_global_top_k_and_stays_live() {
    let mut w = World::new();
    w.room(ROOM_A, &[1, 2]);
    w.room(ROOM_B, &[1, 2]);
    w.room(ROOM_C, &[1, 2]);
    let alice = w.client(1);
    let bob = w.client(2);
    // Bob posts 30 messages round-robin over the three rooms, newest last.
    let tags = [ROOM_A, ROOM_B, ROOM_C];
    for n in 0..30u64 {
        post(
            &w.clients[bob].0,
            tags[n as usize % 3],
            n,
            &format!("m{n:02}"),
        );
    }
    w.pump();
    let db = &w.clients[alice].0;
    let prepared = db.prepare_query(&feed(5)).unwrap();
    let mut sub = block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap();
    let down_before = w.gateway.stats.down;
    w.pump();
    let top = |w: &World| -> Vec<String> {
        let db = &w.clients[alice].0;
        let prepared = db.prepare_query(&feed(5)).unwrap();
        block_on(db.all(&prepared, ReadOpts::default()))
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
            .collect()
    };
    assert_eq!(top(&w), vec!["m29", "m28", "m27", "m26", "m25"]);
    let held = count(&w.clients[alice].0, &Query::from("messages")).len();
    println!(
        "feed k=5 over 3 shards: alice holds {held} message rows; {} messages down",
        w.gateway.stats.down - down_before
    );
    // Live: a new message in one room enters the window.
    post(&w.clients[bob].0, ROOM_B, 30, "m30");
    w.pump();
    assert_eq!(top(&w), vec!["m30", "m29", "m28", "m27", "m26"]);
    while sub.try_next_event().is_some() {}
    assert!(
        w.gateway.stats.refused.is_empty(),
        "{:?}",
        w.gateway.stats.refused
    );
}

#[test]
fn offset_across_shards_reads_the_merged_prefix() {
    // The client asks upstream for `offset + limit` rows and skips the
    // offset itself, so the gateway's merged top (offset + limit) is exact,
    // at the cost of shipping the skipped rows from every shard.
    let mut w = World::new();
    w.room(ROOM_A, &[1, 2]);
    w.room(ROOM_B, &[1, 2]);
    w.room(ROOM_C, &[1, 2]);
    let alice = w.client(1);
    let bob = w.client(2);
    let tags = [ROOM_A, ROOM_B, ROOM_C];
    for n in 0..30u64 {
        post(
            &w.clients[bob].0,
            tags[n as usize % 3],
            n,
            &format!("m{n:02}"),
        );
    }
    w.pump();
    let mut query = feed(5);
    query.offset = 5;
    let db = &w.clients[alice].0;
    let prepared = db.prepare_query(&query).unwrap();
    let _sub = block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap();
    w.pump();
    let db = &w.clients[alice].0;
    let rows = block_on(db.all(&prepared, ReadOpts::default())).unwrap();
    let got: Vec<String> = rows
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
    assert_eq!(got, vec!["m24", "m23", "m22", "m21", "m20"]);
    let held = count(db, &Query::from("messages")).len();
    println!("offset 5 limit 5 over 3 shards: alice holds {held} message rows");
}

/// Rows the reader holds and shard re-asks, for a feed of `k` over `n`
/// shards, with and without quotas, on interleaved and skewed data.
#[test]
fn quotas_cut_feed_overfill() {
    for skewed in [false, true] {
        for quotas in [false, true] {
            QUOTAS.with(|q| *q.borrow_mut() = quotas);
            let mut w = World::new();
            let tags = [ROOM_A, ROOM_B, ROOM_C];
            for tag in tags {
                w.room(tag, &[1, 2]);
            }
            let alice = w.client(1);
            let bob = w.client(2);
            for n in 0..300u64 {
                // Skewed: the newest 100 messages are all in one room.
                let room = if skewed && n >= 200 {
                    ROOM_B
                } else {
                    tags[n as usize % 3]
                };
                post(&w.clients[bob].0, room, n, &format!("m{n:03}"));
            }
            w.pump();
            let db = &w.clients[alice].0;
            let prepared = db.prepare_query(&feed(60)).unwrap();
            let _sub = block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap();
            w.pump();
            let rows = block_on(w.clients[alice].0.all(&prepared, ReadOpts::default())).unwrap();
            assert_eq!(rows.len(), 60);
            let held = count(&w.clients[alice].0, &Query::from("messages")).len();
            let raised: usize = w.gateway.conns[0].multi.values().map(|m| m.raised).sum();
            println!(
                "feed k=60 over 3 shards, {}, quotas {}: reader holds {held} rows, {raised} re-asks",
                if skewed { "skewed" } else { "interleaved" },
                if quotas { "on " } else { "off" },
            );
        }
    }
    QUOTAS.with(|q| *q.borrow_mut() = true);
}
