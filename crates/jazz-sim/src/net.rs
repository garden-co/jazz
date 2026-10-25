//! Deterministic network of jazz-db nodes.
//!
//! Every node is a real [`Db`]. A link joins a client to its server (an
//! authority or a relay) with one ordered queue per direction; a message
//! sent at virtual time `t` becomes receivable at `t + latency`, never
//! before an earlier message on the same queue (a socket is ordered).
//! [`Sim::settle`] ticks every node in declaration order and advances the
//! clock to the next delivery until nothing is in flight.

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use jazz::db::{
    Db, RemoteLinkHint, SubscriberId, SyncMessage, Transport, TransportError, Upstream, block_on,
    encode_frames,
};
use jazz::ids::AuthorSubject;
use serde_json::{Map, Value as JsonValue, json};

use crate::PeerProfile;
use crate::distributions::Lcg;

/// Rounds [`Sim::settle`] may take before it reports a livelock.
const MAX_SETTLE_ROUNDS: usize = 100_000;

/// What crosses a simulated link.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SimulatorTransportCodec {
    /// In-memory sync messages.
    Native,
    /// Every message is encoded to the wire frames a socket carries and
    /// decoded on arrival, so the run proves the wire codec round-trips.
    #[default]
    WireFrames,
}

impl SimulatorTransportCodec {
    /// Parse `native` or `wire_frames`.
    pub fn parse(name: &str, value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "native" => Self::Native,
            "wire_frames" | "wire-frames" | "wireframes" | "frames" => Self::WireFrames,
            other => panic!("{name} must be native or wire_frames; got {other:?}"),
        }
    }

    /// The codec named by `name`, else `JAZZ_TRANSPORT_CODEC`, else wire frames.
    pub fn from_env(name: &str) -> Self {
        std::env::var(name)
            .map(|value| Self::parse(name, &value))
            .or_else(|_| {
                std::env::var("JAZZ_TRANSPORT_CODEC")
                    .map(|value| Self::parse("JAZZ_TRANSPORT_CODEC", &value))
            })
            .unwrap_or_default()
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::WireFrames => "wire_frames",
        }
    }
}

/// How a paused link treats traffic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PauseMode {
    /// Hold messages in the queue until the link resumes (a stalled socket).
    Hold,
    /// Cut the connection: both ends see it drop, queued traffic is lost,
    /// and resuming connects again as a new session.
    Disconnect,
}

/// Traffic counters for one direction of one link.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LinkStats {
    pub messages: u64,
    pub bytes: u64,
    pub frames: u64,
    /// Row images carried by `Rows` messages.
    pub row_images: u64,
    /// Messages by `SyncMessage` variant.
    pub by_kind: BTreeMap<String, u64>,
}

impl LinkStats {
    fn add(&mut self, other: &LinkStats) {
        self.messages += other.messages;
        self.bytes += other.bytes;
        self.frames += other.frames;
        self.row_images += other.row_images;
        for (kind, count) in &other.by_kind {
            *self.by_kind.entry(kind.clone()).or_default() += count;
        }
    }

    pub fn to_json(&self) -> JsonValue {
        json!({
            "messages": self.messages,
            "bytes": self.bytes,
            "frames": self.frames,
            "row_images": self.row_images,
            "by_kind": self.by_kind,
        })
    }
}

/// A message in flight: native, or the frames that encode it.
enum Payload {
    Native(Box<SyncMessage>),
    Frames(Vec<Vec<u8>>),
}

struct Queue {
    items: VecDeque<(u64, Payload)>,
    /// Delivery time of the newest item: later sends never overtake it.
    tail_ms: u64,
    stats: LinkStats,
    /// Every message sent, when the scenario inspects the traffic.
    log: Option<Vec<SyncMessage>>,
}

impl Queue {
    fn new() -> Self {
        Self {
            items: VecDeque::new(),
            tail_ms: 0,
            stats: LinkStats::default(),
            log: None,
        }
    }
}

struct LinkState {
    profile: PeerProfile,
    /// Client to server.
    up: Queue,
    /// Server to client.
    down: Queue,
    held: bool,
    /// Bumped on every reconnect: transports of an older session go dead.
    generation: u64,
}

struct NetState {
    now_ms: u64,
    rng: Lcg,
    codec: SimulatorTransportCodec,
    links: BTreeMap<String, LinkState>,
}

impl NetState {
    fn queue(&mut self, client: &str, up: bool) -> Option<(&mut Queue, &PeerProfile, bool, u64)> {
        let link = self.links.get_mut(client)?;
        let queue = if up { &mut link.up } else { &mut link.down };
        Some((queue, &link.profile, link.held, link.generation))
    }
}

/// One end of a link, handed to a `Db` as its transport.
struct SimTransport {
    net: Rc<RefCell<NetState>>,
    client: String,
    /// Sends go up (client to server) or down.
    up: bool,
    generation: u64,
}

impl Transport for SimTransport {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        let mut net = self.net.borrow_mut();
        let NetState {
            now_ms,
            rng,
            codec,
            links,
        } = &mut *net;
        let link = links
            .get_mut(&self.client)
            .expect("transport of a declared link");
        if link.generation != self.generation {
            return Err(TransportError::Failed("connection closed".to_owned()));
        }
        let latency = link.profile.latency_ms(rng);
        let queue = if self.up {
            &mut link.up
        } else {
            &mut link.down
        };
        #[cfg(feature = "cold-settle-attribution")]
        let encoding = std::time::Instant::now();
        let frames = encode_frames(message.clone()).expect("encode sync message");
        #[cfg(feature = "cold-settle-attribution")]
        crate::phase_attribution::record("codec_encode", encoding.elapsed());
        queue.stats.messages += 1;
        queue.stats.frames += frames.len() as u64;
        queue.stats.bytes += frames.iter().map(|frame| frame.len() as u64).sum::<u64>();
        if let SyncMessage::Rows { images } = &message {
            queue.stats.row_images += images.len() as u64;
        }
        *queue.stats.by_kind.entry(kind(&message)).or_default() += 1;
        if let Some(log) = &mut queue.log {
            log.push(message.clone());
        }
        let at = (*now_ms + latency).max(queue.tail_ms);
        queue.tail_ms = at;
        let payload = match codec {
            SimulatorTransportCodec::Native => Payload::Native(Box::new(message)),
            SimulatorTransportCodec::WireFrames => Payload::Frames(frames),
        };
        queue.items.push_back((at, payload));
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        let mut net = self.net.borrow_mut();
        let now = net.now_ms;
        // The receiver reads the queue the other side sends on.
        let (queue, _, held, generation) = net.queue(&self.client, !self.up)?;
        if held || generation != self.generation {
            return None;
        }
        let (at, _) = queue.items.front()?;
        if *at > now {
            return None;
        }
        let (_, payload) = queue.items.pop_front().expect("front item");
        Some(match payload {
            Payload::Native(message) => *message,
            Payload::Frames(frames) => {
                #[cfg(feature = "cold-settle-attribution")]
                let decoding = std::time::Instant::now();
                let message = decode_frames(frames);
                #[cfg(feature = "cold-settle-attribution")]
                crate::phase_attribution::record("codec_decode", decoding.elapsed());
                message
            }
        })
    }
}

/// Decode the frames of one message. A message split across several
/// frames is a `Rows` message whose images were halved; join them again.
fn decode_frames(frames: Vec<Vec<u8>>) -> SyncMessage {
    let mut decoded = frames
        .iter()
        .map(|frame| postcard::from_bytes::<SyncMessage>(frame).expect("decode sync frame"));
    let first = decoded.next().expect("at least one frame");
    decoded.fold(first, |joined, next| match (joined, next) {
        (SyncMessage::Rows { mut images }, SyncMessage::Rows { images: more }) => {
            images.extend(more);
            SyncMessage::Rows { images }
        }
        (joined, next) => panic!("only Rows messages span frames: {joined:?} then {next:?}"),
    })
}

fn kind(message: &SyncMessage) -> String {
    let debug = format!("{message:?}");
    debug
        .split(|c: char| !c.is_alphanumeric() && c != '_')
        .next()
        .unwrap_or_default()
        .to_owned()
}

struct Node {
    name: String,
    db: Db,
}

struct Connection {
    server: String,
    identity: AuthorSubject,
    upstream: Option<Upstream>,
    subscriber: Option<SubscriberId>,
}

/// A deterministic simulated network of `Db` nodes.
pub struct Sim {
    net: Rc<RefCell<NetState>>,
    nodes: Vec<Node>,
    /// By client name.
    connections: BTreeMap<String, Connection>,
    /// Settle rounds run so far.
    rounds: std::cell::Cell<u64>,
}

impl Sim {
    /// An empty network with seeded link jitter.
    pub fn new(seed: u64, codec: SimulatorTransportCodec) -> Self {
        Self {
            net: Rc::new(RefCell::new(NetState {
                now_ms: 0,
                rng: Lcg::new(seed),
                codec,
                links: BTreeMap::new(),
            })),
            nodes: Vec::new(),
            connections: BTreeMap::new(),
            rounds: std::cell::Cell::new(0),
        }
    }

    /// Add a node. Nodes tick in the order they were added.
    pub fn add(&mut self, name: impl Into<String>, db: Db) -> &Db {
        let name = name.into();
        assert!(
            self.nodes.iter().all(|node| node.name != name),
            "duplicate node {name}"
        );
        self.nodes.push(Node { name, db });
        &self.nodes.last().expect("just added").db
    }

    pub fn db(&self, name: &str) -> &Db {
        &self
            .nodes
            .iter()
            .find(|node| node.name == name)
            .unwrap_or_else(|| panic!("unknown node {name}"))
            .db
    }

    /// Remove a node: its connections close and the `Db` is handed back
    /// (to reopen its file, or to drop it as a crash would).
    pub fn remove(&mut self, name: &str) -> Db {
        let clients: Vec<String> = self
            .connections
            .iter()
            .filter(|(client, conn)| *client == name || conn.server == name)
            .map(|(client, _)| client.clone())
            .collect();
        for client in clients {
            self.cut(&client);
            self.connections.remove(&client);
            self.net.borrow_mut().links.remove(&client);
        }
        let index = self
            .nodes
            .iter()
            .position(|node| node.name == name)
            .unwrap_or_else(|| panic!("unknown node {name}"));
        self.nodes.remove(index).db
    }

    pub fn now_ms(&self) -> u64 {
        self.net.borrow().now_ms
    }

    pub fn codec(&self) -> SimulatorTransportCodec {
        self.net.borrow().codec
    }

    /// Connect `client` to `server`, which serves it as `identity`.
    pub fn connect(
        &mut self,
        client: &str,
        server: &str,
        identity: AuthorSubject,
        profile: PeerProfile,
    ) {
        assert!(
            !self.connections.contains_key(client),
            "{client} is already connected"
        );
        self.net.borrow_mut().links.insert(
            client.to_owned(),
            LinkState {
                profile,
                up: Queue::new(),
                down: Queue::new(),
                held: false,
                generation: 0,
            },
        );
        self.connections.insert(
            client.to_owned(),
            Connection {
                server: server.to_owned(),
                identity,
                upstream: None,
                subscriber: None,
            },
        );
        self.open(client);
    }

    fn open(&mut self, client: &str) {
        let generation = {
            let mut net = self.net.borrow_mut();
            let link = net.links.get_mut(client).expect("declared link");
            link.generation += 1;
            link.up.items.clear();
            link.down.items.clear();
            link.held = false;
            link.generation
        };
        let transport = |up| {
            Box::new(SimTransport {
                net: self.net.clone(),
                client: client.to_owned(),
                up,
                generation,
            })
        };
        let (client_side, server_side) = (transport(true), transport(false));
        let conn = &self.connections[client];
        let server = conn.server.clone();
        let identity = conn.identity;
        let upstream = block_on(self.db(client).connect_upstream(client_side));
        self.db(client).set_remote_link_hint(RemoteLinkHint::Live);
        let subscriber = self.db(&server).accept_subscriber(server_side, identity);
        let conn = self.connections.get_mut(client).expect("connection");
        conn.upstream = Some(upstream);
        conn.subscriber = Some(subscriber);
    }

    /// Both ends see the connection drop; traffic in flight is lost.
    fn cut(&mut self, client: &str) {
        let conn = self.connections.get_mut(client).expect("connection");
        let upstream = conn.upstream.take();
        let subscriber = conn.subscriber.take();
        let server = conn.server.clone();
        if let Some(upstream) = upstream {
            self.db(client).detach_connection(&upstream);
            self.db(client).set_remote_link_hint(RemoteLinkHint::Failed);
        }
        if let Some(subscriber) = subscriber {
            self.db(&server).disconnect_subscriber(subscriber);
        }
        let mut net = self.net.borrow_mut();
        let link = net.links.get_mut(client).expect("declared link");
        link.generation += 1;
        link.up.items.clear();
        link.down.items.clear();
    }

    /// Pause `client`'s link.
    pub fn pause(&mut self, client: &str, mode: PauseMode) {
        match mode {
            PauseMode::Hold => {
                self.net
                    .borrow_mut()
                    .links
                    .get_mut(client)
                    .expect("declared link")
                    .held = true;
            }
            PauseMode::Disconnect => self.cut(client),
        }
    }

    /// Resume a held link, or connect a cut one again as a new session.
    pub fn resume(&mut self, client: &str) {
        if self.connections[client].upstream.is_some() {
            let mut net = self.net.borrow_mut();
            let now = net.now_ms;
            let link = net.links.get_mut(client).expect("declared link");
            link.held = false;
            // Held traffic leaves now, not when it was first sent.
            for queue in [&mut link.up, &mut link.down] {
                for (at, _) in &mut queue.items {
                    *at = (*at).max(now);
                }
            }
        } else {
            self.open(client);
        }
    }

    pub fn is_connected(&self, client: &str) -> bool {
        self.connections
            .get(client)
            .is_some_and(|conn| conn.upstream.is_some())
    }

    /// Record every message sent down to `client` from now on.
    pub fn record_downstream(&mut self, client: &str) {
        self.net
            .borrow_mut()
            .links
            .get_mut(client)
            .expect("declared link")
            .down
            .log = Some(Vec::new());
    }

    /// Messages sent down to `client` since [`Self::record_downstream`].
    pub fn downstream_log(&self, client: &str) -> Vec<SyncMessage> {
        self.net.borrow().links[client]
            .down
            .log
            .clone()
            .unwrap_or_default()
    }

    /// Traffic on `client`'s link: (up, down).
    pub fn link_stats(&self, client: &str) -> (LinkStats, LinkStats) {
        let net = self.net.borrow();
        let link = &net.links[client];
        (link.up.stats.clone(), link.down.stats.clone())
    }

    /// Traffic over every link: (up, down).
    pub fn total_stats(&self) -> (LinkStats, LinkStats) {
        let net = self.net.borrow();
        let mut up = LinkStats::default();
        let mut down = LinkStats::default();
        for link in net.links.values() {
            up.add(&link.up.stats);
            down.add(&link.down.stats);
        }
        (up, down)
    }

    /// Zero every link's counters.
    pub fn reset_stats(&mut self) {
        for link in self.net.borrow_mut().links.values_mut() {
            link.up.stats = LinkStats::default();
            link.down.stats = LinkStats::default();
        }
    }

    /// Tick every node once, in declaration order.
    pub fn tick(&self) {
        for node in &self.nodes {
            #[cfg(feature = "cold-settle-attribution")]
            let (ticking, codec) = (
                std::time::Instant::now(),
                crate::phase_attribution::codec_so_far(),
            );
            block_on(node.db.tick()).unwrap_or_else(|error| {
                panic!("tick {} failed: {error}", node.name);
            });
            #[cfg(feature = "cold-settle-attribution")]
            crate::phase_attribution::record(
                &format!("tick_{}", node.name),
                ticking
                    .elapsed()
                    .saturating_sub(crate::phase_attribution::codec_so_far() - codec),
            );
        }
    }

    /// The earliest delivery time of traffic on a live, unheld link.
    fn next_delivery(&self) -> Option<u64> {
        let net = self.net.borrow();
        net.links
            .values()
            .filter(|link| !link.held)
            .flat_map(|link| {
                link.up
                    .items
                    .front()
                    .into_iter()
                    .chain(link.down.items.front())
            })
            .map(|(at, _)| *at)
            .min()
    }

    /// One round: tick every node, then move the clock to the next
    /// delivery if nothing is receivable now. False once nothing is in
    /// flight.
    pub fn step(&self) -> bool {
        self.rounds.set(self.rounds.get() + 1);
        self.tick();
        match self.next_delivery() {
            None => false,
            Some(at) => {
                let mut net = self.net.borrow_mut();
                net.now_ms = net.now_ms.max(at);
                true
            }
        }
    }

    /// Run until no traffic is in flight on any live link (held links keep
    /// theirs). Two quiet rounds in a row: a tick may answer what the
    /// previous one delivered.
    pub fn settle(&self) {
        let mut quiet = 0;
        for _ in 0..MAX_SETTLE_ROUNDS {
            if self.step() {
                quiet = 0;
            } else {
                quiet += 1;
                if quiet == 2 {
                    return;
                }
            }
        }
        panic!("network did not settle within {MAX_SETTLE_ROUNDS} rounds");
    }

    /// Advance the virtual clock by `ms`, delivering what falls due.
    pub fn advance(&self, ms: u64) {
        let until = self.now_ms() + ms;
        loop {
            self.rounds.set(self.rounds.get() + 1);
            self.tick();
            match self.next_delivery() {
                Some(at) if at <= until => {
                    let mut net = self.net.borrow_mut();
                    net.now_ms = net.now_ms.max(at);
                }
                _ => break,
            }
        }
        self.net.borrow_mut().now_ms = until;
        self.tick();
    }

    /// Poll `future` to completion, stepping the network between polls.
    pub fn drive<T>(&self, future: impl Future<Output = T>) -> T {
        let mut future = pin!(future);
        let mut context = Context::from_waker(Waker::noop());
        for _ in 0..MAX_SETTLE_ROUNDS {
            if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
                return output;
            }
            if !self.step() {
                // Nothing in flight: one more tick may still flush an outbox.
                self.tick();
            }
        }
        panic!("operation did not complete within {MAX_SETTLE_ROUNDS} rounds");
    }

    /// Settle rounds run so far.
    pub fn rounds(&self) -> u64 {
        self.rounds.get()
    }

    /// Transport and clock fields for a JSON summary.
    pub fn json_fields(&self) -> Map<String, JsonValue> {
        let (up, down) = self.total_stats();
        let mut fields = Map::new();
        fields.insert("transport_codec".to_owned(), json!(self.codec().as_str()));
        fields.insert("virtual_ms".to_owned(), json!(self.now_ms()));
        fields.insert("rounds".to_owned(), json!(self.rounds.get()));
        fields.insert("up".to_owned(), up.to_json());
        fields.insert("down".to_owned(), down.to_json());
        fields
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jazz::db::{DbConfig, DbIdentity, InsertOptions, ReadOpts};
    use jazz::ids::{NodeUuid, RowUuid};
    use jazz::query::Query;
    use jazz::records::Value;
    use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};

    fn open(node: u8, author: AuthorSubject) -> Db {
        let allow = TablePolicies::new()
            .with_select(PolicyExpr::True)
            .with_insert(PolicyExpr::True)
            .with_update(Some(PolicyExpr::True), PolicyExpr::True)
            .with_delete(PolicyExpr::True);
        let schema = jazz::schema::JazzSchema::new(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("notes")
                        .column("body", ColumnType::Text)
                        .policies(allow),
                )
                .build(),
        )
        .unwrap();
        block_on(Db::open(DbConfig::new(
            schema,
            DbIdentity {
                node: NodeUuid::from_bytes([node; 16]),
                author,
            },
        )))
        .unwrap()
    }

    fn user(n: u8) -> AuthorSubject {
        AuthorSubject::for_test_bytes([n; 16])
    }

    fn sim(seed: u64, codec: SimulatorTransportCodec, latency: u64) -> Sim {
        let mut sim = Sim::new(seed, codec);
        sim.add("core", open(0, AuthorSubject::SYSTEM));
        sim.add("a", open(1, user(1)));
        sim.add("b", open(2, user(2)));
        let profile = PeerProfile::new("test", latency, 3, 0);
        sim.connect("a", "core", user(1), profile.clone());
        sim.connect("b", "core", user(2), profile);
        sim
    }

    fn bodies(db: &Db) -> Vec<Value> {
        let query = db.prepare_query(&Query::from("notes")).unwrap();
        block_on(db.all(&query, ReadOpts::default()))
            .unwrap()
            .iter()
            .flat_map(|row| {
                row.cells()
                    .filter_map(|(_, v)| v.cloned())
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn write(sim: &Sim, node: &str, n: u8) {
        let handle = block_on(sim.db(node).insert(
            "notes",
            BTreeMap::from([("body".to_owned(), Value::String(format!("n{n}")))]),
            InsertOptions {
                row_id: Some(RowUuid::from_bytes([n; 16])),
                ..Default::default()
            },
        ))
        .unwrap();
        let tx = handle.mergeable_tx_id();
        sim.drive(
            sim.db(node)
                .wait_for_transaction(tx, jazz::tx::DurabilityTier::Global),
        )
        .unwrap();
    }

    #[test]
    fn writes_reach_every_replica_over_both_codecs() {
        for codec in [
            SimulatorTransportCodec::Native,
            SimulatorTransportCodec::WireFrames,
        ] {
            let sim = sim(7, codec, 5);
            let query = sim.db("b").prepare_query(&Query::from("notes")).unwrap();
            let _live = block_on(sim.db("b").subscribe(&query, ReadOpts::default())).unwrap();
            write(&sim, "a", 1);
            sim.settle();
            assert_eq!(bodies(sim.db("b")), vec![Value::String("n1".to_owned())]);
            assert!(
                sim.now_ms() >= 10,
                "a round trip takes two one-way latencies"
            );
            let (up, down) = sim.total_stats();
            assert!(up.messages > 0 && down.messages > 0 && down.bytes > 0);
        }
    }

    #[test]
    fn the_schedule_repeats_for_one_seed() {
        let run = || {
            let sim = sim(42, SimulatorTransportCodec::WireFrames, 4);
            write(&sim, "a", 1);
            write(&sim, "b", 2);
            sim.settle();
            (sim.now_ms(), sim.total_stats())
        };
        assert_eq!(run(), run());
    }

    #[test]
    fn a_held_link_delivers_only_after_it_resumes() {
        let mut sim = sim(1, SimulatorTransportCodec::Native, 1);
        let query = sim.db("b").prepare_query(&Query::from("notes")).unwrap();
        let _live = block_on(sim.db("b").subscribe(&query, ReadOpts::default())).unwrap();
        sim.settle();
        sim.pause("b", PauseMode::Hold);
        write(&sim, "a", 1);
        sim.settle();
        assert!(bodies(sim.db("b")).is_empty());
        sim.resume("b");
        sim.settle();
        assert_eq!(bodies(sim.db("b")).len(), 1);
    }

    #[test]
    fn a_cut_link_catches_up_as_a_new_session() {
        let mut sim = sim(1, SimulatorTransportCodec::WireFrames, 1);
        let query = sim.db("b").prepare_query(&Query::from("notes")).unwrap();
        let _live = block_on(sim.db("b").subscribe(&query, ReadOpts::default())).unwrap();
        sim.settle();
        sim.pause("b", PauseMode::Disconnect);
        assert!(!sim.is_connected("b"));
        write(&sim, "a", 1);
        sim.settle();
        assert!(bodies(sim.db("b")).is_empty());
        sim.resume("b");
        sim.settle();
        assert_eq!(bodies(sim.db("b")).len(), 1);
    }
}
