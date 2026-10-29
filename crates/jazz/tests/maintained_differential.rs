//! Maintained-vs-one-shot differential oracle: random writes from the
//! authority and from synced clients, under every reader's subscriptions on
//! both; after every round each maintained result must equal a fresh read of
//! the same query by the same reader on the authority.
//!
//! The matrix is deliberately review-visible. A row missing from it is a
//! known coverage gap, not implicit confidence.
//!
//! | Atom / shape family                               | Covered by shape(s)        |
//! |---------------------------------------------------|----------------------------|
//! | plain table                                       | `messages`                 |
//! | filtered, ordered first page (DESC, LIMIT)        | `room_page`                |
//! | first page ordered by an unselected column        | `bodies_page`              |
//! | read policy: correlated EXISTS over memberships   | every `messages` shape     |
//! | read policy: session equality                     | `memberships`              |
//! | provenance-scoped `$createdBy`                    | `mine`                     |
//! | forward include                                   | `with_room`                |
//! | ordered, limited parent page                      | `rooms_page`               |
//! | aggregates count/min/max/sum/avg, grouped          | `stats_by_room`            |
//! | synced replica: maintained vs the authority's     | every shape, on a client   |
//! |   one-shot read for the same reader               |                            |
//! | aggregate cancellation and sustained churn        | churn phase, per depth     |
//!
//! `maintained_one_shot_differential_oracle` is the randomized soak; its
//! inputs come from the environment:
//!
//! - `JAZZ_SEED`: one seed (default: a fixed list);
//! - `JAZZ_DIFFERENTIAL_STEP_COUNT`: random rounds per seed (default 300);
//! - `JAZZ_DIFFERENTIAL_CHURN_DEPTHS`: rows inserted then deleted in one
//!   room per churn phase, comma-separated (default `10,1000`).
//!
//! CI runs it bounded (seed 11; see the `rust-differential` partition in
//! `dev/gates/local-ci-equivalent.mjs`).

mod common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::{BTreeMap, BTreeSet};

use duplex_transport::duplex;
use jazz::db::{
    CurrentRow, Db, DbConfig, DbIdentity, InsertOptions, PreparedQuery, ReadOpts,
    SubscriptionEvent, SubscriptionStream, UpdateOptions, WriteIdentity, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{Aggregate, OrderDirection, Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{
    allow_all_policies, compile_schema, exists, outer_eq, read_and_allow_all_writes, session_eq,
};

const USERS: usize = 4;
const ROOMS: usize = 3;
const ME: [&str; 3] = ["user", "identity", "subject"];

fn user(n: usize) -> AuthorSubject {
    AuthorSubject::for_test_bytes([0x40 + n as u8; 16])
}

fn subject(n: usize) -> Value {
    Value::String(user(n).principal_parts().1)
}

fn room_id(n: usize) -> RowUuid {
    RowUuid::from_bytes([0x10 + n as u8; 16])
}

fn room(n: usize) -> Value {
    Value::Uuid(room_id(n).0)
}

fn row(kind: u8, n: usize) -> RowUuid {
    let mut bytes = [kind; 16];
    bytes[8..].copy_from_slice(&(n as u64).to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn cells(pairs: &[(&str, Value)]) -> BTreeMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect()
}

struct Rng(u64);

impl Rng {
    fn next(&mut self, n: usize) -> usize {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((self.0 >> 33) as usize) % n
    }
}

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("rooms")
                    .column("name", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("memberships")
                    .fk_column("room", "rooms")
                    .column("member", ColumnType::Text)
                    .policies(read_and_allow_all_writes(session_eq("member", &ME))),
            )
            .table(
                TableSchemaBuilder::new("messages")
                    .fk_column("room", "rooms")
                    .column("body", ColumnType::Text)
                    .column("sent_at", ColumnType::BigInt)
                    .composite_index(["room", "sent_at"])
                    .policies(read_and_allow_all_writes(exists(
                        "memberships",
                        vec![outer_eq("room", "room"), session_eq("member", &ME)],
                    ))),
            )
            .build(),
    )
}

/// Every shape a reader subscribes to, by name.
fn shapes(reader: usize) -> Vec<(&'static str, Query)> {
    vec![
        ("messages", Query::from("messages")),
        (
            "room_page",
            Query::from("messages")
                .filter(eq(col("room"), lit(room(reader % ROOMS))))
                .order_by("sent_at", OrderDirection::Desc)
                .limit(3),
        ),
        (
            "bodies_page",
            Query::from("messages")
                .select(["body"])
                .order_by("sent_at", OrderDirection::Asc)
                .limit(4),
        ),
        ("memberships", Query::from("memberships")),
        (
            "mine",
            Query::from("messages")
                .filter(eq(col("$createdBy.identity.subject"), lit(subject(reader)))),
        ),
        ("with_room", Query::from("messages").include("room")),
        (
            "rooms_page",
            Query::from("rooms")
                .order_by("name", OrderDirection::Asc)
                .limit(2),
        ),
        (
            "stats_by_room",
            Query::from("messages")
                .aggregate([
                    Aggregate::count(),
                    Aggregate::min("sent_at"),
                    Aggregate::max("sent_at"),
                    Aggregate::sum("sent_at"),
                    Aggregate::avg("sent_at"),
                ])
                .group_by("room")
                .order_by("room", OrderDirection::Asc),
        ),
    ]
}

type Rows = Vec<(RowUuid, Vec<(String, Option<Value>)>)>;

fn owned(row: &CurrentRow) -> Vec<(String, Option<Value>)> {
    row.cells()
        .map(|(name, value)| (name.to_owned(), value.cloned()))
        .collect()
}

fn snapshot(rows: &[CurrentRow]) -> Rows {
    rows.iter().map(|r| (r.row_uuid(), owned(r))).collect()
}

/// One maintained result.
struct Live {
    shape: &'static str,
    reader: usize,
    /// The client replica it lives on, or `None` for the authority.
    client: Option<usize>,
    /// The query, prepared on the authority: what the reader may read there
    /// is the truth both its results must match.
    truth: PreparedQuery,
    stream: SubscriptionStream,
    rows: Rows,
}

impl Live {
    /// Apply queued deltas.
    fn apply(&mut self) {
        while let Some(event) = self.stream.try_next_event() {
            let SubscriptionEvent::Delta {
                reset,
                added,
                updated,
                removed,
                ..
            } = event
            else {
                panic!("{}: unexpected event {event:?}", self.shape);
            };
            if reset {
                self.rows.clear();
            }
            let moved: BTreeSet<RowUuid> = removed
                .iter()
                .map(|gone| gone.row_uuid)
                .chain(updated.iter().map(|row| row.row_uuid()))
                .collect();
            self.rows.retain(|(id, _)| !moved.contains(id));
            let mut placed: Vec<_> = added.iter().chain(updated.iter()).collect();
            placed.sort_by_key(|row| row.index);
            for row in placed {
                self.rows.insert(row.index, (row.row_uuid(), owned(row)));
            }
        }
    }
}

struct World {
    server: Db,
    clients: Vec<Db>,
    live: Vec<Live>,
    messages: usize,
    memberships: usize,
}

impl World {
    fn open() -> Self {
        let server = block_on(Db::open_history_complete(DbConfig::new(
            schema(),
            DbIdentity {
                node: NodeUuid::from_bytes([1; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )))
        .unwrap();
        for r in 0..ROOMS {
            block_on(server.insert(
                "rooms",
                cells(&[("name", Value::String(format!("r{r}")))]),
                InsertOptions {
                    row_id: Some(room_id(r)),
                    ..Default::default()
                },
            ))
            .unwrap();
        }
        let mut world = World {
            server,
            clients: Vec::new(),
            live: Vec::new(),
            messages: 0,
            memberships: 0,
        };
        for u in 0..USERS {
            let client = block_on(Db::open(DbConfig::new(
                schema(),
                DbIdentity {
                    node: NodeUuid::from_bytes([0x50 + u as u8; 16]),
                    author: user(u),
                },
            )))
            .unwrap();
            let (client_side, server_side) = duplex();
            let _upstream = block_on(client.connect_upstream(client_side));
            world
                .server
                .accept_subscriber(server_side, user(u))
                .unwrap();
            world.clients.push(client);
        }
        for u in 0..USERS {
            for (shape, query) in shapes(u) {
                let on_server = world.server.prepare_query(&query).unwrap();
                let stream = block_on(world.server.subscribe_for_identity(
                    &on_server,
                    ReadOpts::default(),
                    user(u),
                ))
                .unwrap();
                world.live.push(Live {
                    shape,
                    reader: u,
                    client: None,
                    truth: on_server.clone(),
                    stream,
                    rows: Vec::new(),
                });
                let client = &world.clients[u];
                let on_client = client.prepare_query(&query).unwrap();
                let stream = block_on(client.subscribe(&on_client, ReadOpts::default())).unwrap();
                world.live.push(Live {
                    shape,
                    reader: u,
                    client: Some(u),
                    truth: on_server,
                    stream,
                    rows: Vec::new(),
                });
            }
        }
        world.pump();
        world
    }

    fn pump(&self) {
        for _ in 0..12 {
            for client in &self.clients {
                block_on(client.tick()).unwrap();
            }
            block_on(self.server.tick()).unwrap();
        }
        for client in &self.clients {
            block_on(client.tick()).unwrap();
        }
    }

    /// Every maintained result equals a fresh read by its reader.
    fn check(&mut self, context: &str) {
        for live in &mut self.live {
            live.apply();
            // A replica's settled result is the authority's answer for its
            // reader; its local copies may keep rows the reader lost access
            // to (INV-SYNC-14), so a local read is not the reference.
            let fresh = block_on(self.server.all_for_identity(
                &live.truth,
                ReadOpts::default(),
                user(live.reader),
            ))
            .unwrap();
            assert_eq!(
                live.rows,
                snapshot(&fresh),
                "{context}: {} for reader {} on {} diverged",
                live.shape,
                live.reader,
                live.client
                    .map_or("the authority".to_owned(), |c| format!("client {c}")),
            );
        }
    }

    fn step(&mut self, rng: &mut Rng, step: usize) -> usize {
        let pick = rng.next(8);
        // Where it runs: the authority (as a random user), or a client as
        // its own user.
        let who = rng.next(USERS);
        let (db, identity) = if rng.next(2) == 0 {
            (&self.server, WriteIdentity::Session(user(who)))
        } else {
            (&self.clients[who], WriteIdentity::Database)
        };
        let updated = |identity: WriteIdentity| UpdateOptions {
            identity,
            ..Default::default()
        };
        let result = match pick {
            0 | 1 => {
                self.messages += 1;
                block_on(db.insert(
                    "messages",
                    cells(&[
                        ("room", room(rng.next(ROOMS))),
                        ("body", Value::String(format!("m{step}"))),
                        ("sent_at", Value::I64(rng.next(50) as i64)),
                    ]),
                    InsertOptions {
                        row_id: Some(row(3, self.messages)),
                        identity,
                        ..Default::default()
                    },
                ))
                .map(drop)
            }
            // Leaves one watch key and policy scope, enters another.
            2 if self.messages > 0 => block_on(db.update(
                "messages",
                row(3, 1 + rng.next(self.messages)),
                cells(&[
                    ("room", room(rng.next(ROOMS))),
                    ("sent_at", Value::I64(rng.next(50) as i64)),
                ]),
                updated(identity),
            ))
            .map(drop),
            3 => {
                self.memberships += 1;
                block_on(db.insert(
                    "memberships",
                    cells(&[
                        ("room", room(rng.next(ROOMS))),
                        ("member", subject(rng.next(USERS))),
                    ]),
                    InsertOptions {
                        row_id: Some(row(2, self.memberships)),
                        identity,
                        ..Default::default()
                    },
                ))
                .map(drop)
            }
            // Policy evidence moves between readers.
            4 if self.memberships > 0 => block_on(db.update(
                "memberships",
                row(2, 1 + rng.next(self.memberships)),
                cells(&[("member", subject(rng.next(USERS)))]),
                updated(identity),
            ))
            .map(drop),
            5 if self.memberships > 0 => block_on(db.delete(
                "memberships",
                row(2, 1 + rng.next(self.memberships)),
                Default::default(),
            ))
            .map(drop),
            // An included parent changes.
            6 => block_on(db.update(
                "rooms",
                room_id(rng.next(ROOMS)),
                cells(&[("name", Value::String(format!("n{step}")))]),
                updated(identity),
            ))
            .map(drop),
            // A full page that loses a row must refill.
            7 if self.messages > 0 => block_on(db.delete(
                "messages",
                row(3, 1 + rng.next(self.messages)),
                Default::default(),
            ))
            .map(drop),
            _ => Ok(()),
        };
        // A write a client cannot make (a row it cannot see) is refused;
        // the oracle only needs every store to stay consistent.
        let _ = result;
        pick
    }

    /// The aggregate rows every authority-side `stats_by_room` holds.
    fn stats(&self) -> Vec<Rows> {
        self.live
            .iter()
            .filter(|live| live.shape == "stats_by_room" && live.client.is_none())
            .map(|live| live.rows.clone())
            .collect()
    }

    /// Insert `depth` messages into one room, then delete them all, checking
    /// along the way; the aggregates end where they started.
    fn churn(&mut self, depth: usize, seed: u64) {
        let before = self.stats();
        let every = (depth / 10).max(1);
        let first = self.messages + 1;
        for n in 0..depth {
            self.messages += 1;
            block_on(self.server.insert(
                "messages",
                cells(&[
                    ("room", room(0)),
                    ("body", Value::String(format!("churn{n}"))),
                    ("sent_at", Value::I64((n % 97) as i64 - 40)),
                ]),
                InsertOptions {
                    row_id: Some(row(3, self.messages)),
                    identity: WriteIdentity::Session(user(n % USERS)),
                    ..Default::default()
                },
            ))
            .unwrap();
            if n % every == every - 1 {
                self.pump();
                self.check(&format!("seed {seed}: churn {depth} insert {n}"));
            }
        }
        for (n, id) in (first..=self.messages).enumerate() {
            block_on(
                self.server
                    .delete("messages", row(3, id), Default::default()),
            )
            .unwrap();
            if n % every == every - 1 {
                self.pump();
                self.check(&format!("seed {seed}: churn {depth} delete {n}"));
            }
        }
        self.pump();
        self.check(&format!("seed {seed}: churn {depth} done"));
        assert_eq!(
            self.stats(),
            before,
            "seed {seed}: churn {depth} did not cancel out"
        );
    }
}

fn env_list(name: &str) -> Option<Vec<u64>> {
    let value = std::env::var(name).ok()?;
    Some(
        value
            .split(',')
            .map(|part| {
                part.trim()
                    .parse()
                    .unwrap_or_else(|_| panic!("{name}={value}"))
            })
            .collect(),
    )
}

fn run(seed: u64, steps: usize, churn_depths: &[u64]) {
    let mut world = World::open();
    world.check(&format!("seed {seed}: opening"));
    let mut rng = Rng(seed);
    for step in 0..steps {
        let pick = world.step(&mut rng, step);
        world.pump();
        world.check(&format!("seed {seed}: step {step} (op {pick})"));
    }
    for &depth in churn_depths {
        world.churn(depth as usize, seed);
    }
    let stats = world.server.fanout_stats();
    assert!(
        stats.shared_joins > 0,
        "seed {seed}: equal shapes share a group: {stats:?}"
    );
}

#[test]
#[ignore = "#1787: manual randomized differential soak; bounded seed 11 runs in CI"]
fn maintained_one_shot_differential_oracle() {
    let seeds = env_list("JAZZ_SEED").unwrap_or_else(|| vec![7, 11, 23, 42, 99]);
    let steps = env_list("JAZZ_DIFFERENTIAL_STEP_COUNT").map_or(300, |steps| steps[0] as usize);
    let churn = env_list("JAZZ_DIFFERENTIAL_CHURN_DEPTHS").unwrap_or_else(|| vec![10, 1000]);
    for seed in seeds {
        run(seed, steps, &churn);
    }
}

/// The oracle itself catches a divergence: a maintained result that missed
/// a delta fails the check.
#[test]
fn maintained_one_shot_differential_oracle_control() {
    let mut world = World::open();
    world.check("opening");
    block_on(world.server.insert(
        "rooms",
        cells(&[("name", Value::String("a".to_owned()))]),
        Default::default(),
    ))
    .unwrap();
    world.pump();
    // Drop the authority's `rooms_page` deltas instead of applying them.
    let live = world
        .live
        .iter_mut()
        .find(|live| live.shape == "rooms_page" && live.client.is_none())
        .unwrap();
    while live.stream.try_next_event().is_some() {}
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| world.check("control")));
    assert!(caught.is_err(), "the oracle must report a missed delta");
}
