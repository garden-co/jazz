//! Client↔server sync against the authority's truth: several clients, each
//! signed in as its own user, write concurrently (including writes the
//! server's policies reject, writes while offline, and writes that move rows
//! out of other users' reach) while holding window, unbounded and
//! policy-filtered subscriptions. Whenever every client has synced, each
//! maintained result must equal what the server returns for that user, and
//! every client's pending writes must have a fate. This pins the sync layer
//! in `jazz-db` (`sync.rs`).

use crate::common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;
use std::future::Future;

use duplex_transport::duplex;
use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, PreparedQuery, ReadOpts, SubscriptionEvent,
    SubscriptionStream, Upstream, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

use common::{allow_all_policies, compile_schema, exists, outer_eq, session_eq};

const CLIENTS: usize = 4;
const ROOMS: usize = 3;
const ME: [&str; 3] = ["user", "identity", "subject"];

fn user(n: usize) -> AuthorSubject {
    AuthorSubject::for_test_bytes([0x40 + n as u8; 16])
}

fn subject(n: usize) -> Value {
    Value::String(user(n).principal_parts().1)
}

fn room(n: usize) -> Value {
    Value::Uuid(uuid::Uuid::from_bytes([0x10 + n as u8; 16]))
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
    let member_of_room = exists(
        "memberships",
        vec![outer_eq("room", "room"), session_eq("member", &ME)],
    );
    let owner = session_eq("owner", &ME);
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
                    .policies(common::read_and_allow_all_writes(session_eq("member", &ME))),
            )
            .table(
                TableSchemaBuilder::new("messages")
                    .fk_column("room", "rooms")
                    .column("body", ColumnType::Text)
                    .column("sent_at", ColumnType::BigInt)
                    .composite_index(["room", "sent_at"])
                    // Only members read and post; anyone may edit or delete
                    // what they can see.
                    .policies(
                        TablePolicies::new()
                            .with_select(member_of_room.clone())
                            .with_insert(member_of_room)
                            .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                            .with_delete(PolicyExpr::True),
                    ),
            )
            .table(
                TableSchemaBuilder::new("notes")
                    .column("owner", ColumnType::Text)
                    .column("text", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(owner.clone())
                            .with_insert(owner.clone())
                            .with_update(Some(owner.clone()), owner.clone())
                            .with_delete(owner),
                    ),
            )
            .build(),
    )
}

fn open(node: u8, author: AuthorSubject) -> Db {
    let schema = schema();
    block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )))
    .unwrap()
}

type Rows = Vec<(RowUuid, Vec<(String, Option<Value>)>)>;

fn owned(row: &jazz::db::CurrentRow) -> Vec<(String, Option<Value>)> {
    row.cells()
        .map(|(name, value)| (name.to_owned(), value.cloned()))
        .collect()
}

fn snapshot(rows: &[jazz::db::CurrentRow]) -> Rows {
    rows.iter().map(|r| (r.row_uuid(), owned(r))).collect()
}

struct Live {
    query: PreparedQuery,
    stream: SubscriptionStream,
    rows: Rows,
}

fn apply(live: &mut Live) {
    while let Some(event) = live.stream.try_next_event() {
        let SubscriptionEvent::Delta {
            reset,
            added,
            updated,
            removed,
            ..
        } = event
        else {
            panic!("unexpected event {event:?}");
        };
        if reset {
            live.rows.clear();
        }
        let moved: std::collections::BTreeSet<RowUuid> = removed
            .iter()
            .map(|gone| gone.row_uuid)
            .chain(updated.iter().map(|row| row.row_uuid()))
            .collect();
        live.rows.retain(|(id, _)| !moved.contains(id));
        let mut placed: Vec<_> = added.iter().chain(updated.iter()).collect();
        placed.sort_by_key(|row| row.index);
        for row in placed {
            live.rows.insert(row.index, (row.row_uuid(), owned(row)));
        }
    }
}

struct Client {
    db: Db,
    who: usize,
    upstream: Option<Upstream>,
    live: Vec<Live>,
    writes: Vec<u64>,
}

struct Net {
    server: Db,
    clients: Vec<Client>,
}

impl Net {
    fn connect(&mut self, index: usize) {
        let (client_side, server_side) = duplex();
        let client = &mut self.clients[index];
        client.upstream = Some(block_on(client.db.connect_upstream(client_side)));
        self.server
            .accept_subscriber(server_side, user(client.who))
            .unwrap();
    }

    fn pump(&self) {
        for client in &self.clients {
            block_on(client.db.tick()).unwrap();
        }
        block_on(self.server.tick()).unwrap();
        for client in &self.clients {
            block_on(client.db.tick()).unwrap();
        }
    }

    /// Every client's maintained results equal the server's for its user,
    /// and every write it made while connected has a fate.
    fn assert_converged(&mut self, step: usize) -> usize {
        let mut rejected = 0;
        for _ in 0..4 {
            self.pump();
        }
        for client in &mut self.clients {
            for live in &mut client.live {
                apply(live);
                let truth = block_on(self.server.all_for_identity(
                    &live.query,
                    ReadOpts::default(),
                    user(client.who),
                ))
                .unwrap();
                if live.rows != snapshot(&truth) {
                    let all = self
                        .server
                        .prepare_query(&Query::from(live.query.query().table.as_str()))
                        .unwrap();
                    eprintln!(
                        "server rows: {:?}",
                        snapshot(&block_on(self.server.all(&all, ReadOpts::default())).unwrap())
                    );
                    let mem = self
                        .server
                        .prepare_query(&Query::from("memberships"))
                        .unwrap();
                    eprintln!(
                        "server memberships: {:?}",
                        snapshot(&block_on(self.server.all(&mem, ReadOpts::default())).unwrap())
                    );
                    eprintln!("client sync: {}", client.db.sync_debug());
                    eprintln!(
                        "client local: {:?}",
                        snapshot(
                            &block_on(client.db.all(&live.query, ReadOpts::default())).unwrap()
                        )
                    );
                }
                assert_eq!(
                    live.rows,
                    snapshot(&truth),
                    "client {} diverged from the server after step {step} on {:?}",
                    client.who,
                    live.query.query()
                );
                // Included rooms are in the replica, as the server has them.
                if !live.query.query().includes.is_empty() {
                    let rooms = self.server.prepare_query(&Query::from("rooms")).unwrap();
                    let server_rooms =
                        snapshot(&block_on(self.server.all(&rooms, ReadOpts::default())).unwrap());
                    let local = client.db.prepare_query(&Query::from("rooms")).unwrap();
                    let local_opts = ReadOpts {
                        tier: jazz::db::ReadTier::LocalOnly,
                        ..ReadOpts::default()
                    };
                    let local_rooms =
                        snapshot(&block_on(client.db.all(&local, local_opts)).unwrap());
                    for row in &truth {
                        let Some((_, Some(Value::Uuid(room)))) = row
                            .cells()
                            .find(|(name, _)| *name == "room")
                            .map(|(n, v)| (n, v.cloned()))
                        else {
                            continue;
                        };
                        let want = server_rooms.iter().find(|(id, _)| id.0 == room);
                        let have = local_rooms.iter().find(|(id, _)| id.0 == room);

                        assert_eq!(
                            have, want,
                            "client {} lacks or has a stale included room after step {step}",
                            client.who
                        );
                    }
                }
            }
            for tx in client.writes.drain(..) {
                // Resolved either way: accepted or rejected by the server.
                let mut wait =
                    std::pin::pin!(client.db.wait_for_transaction(tx, DurabilityTier::Global));
                let polled = wait
                    .as_mut()
                    .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()));
                assert!(
                    polled.is_ready(),
                    "client {} write {} has no fate after syncing",
                    client.who,
                    tx
                );
                rejected += matches!(polled, std::task::Poll::Ready(Err(_))) as usize;
            }
        }
        rejected
    }
}

/// Knobs for the nightly soak (`dev/gates/run-continuous-simulation-soak.sh`):
/// - `JAZZ_SEED`: run this one seed instead of seeds `1..=SEEDS` (default 8);
/// - `JAZZ_COMMIT_COUNT`: random writes per seed (default 600).
#[test]
fn clients_converge_on_the_servers_truth_under_concurrent_writes() {
    let env = |name: &str| {
        std::env::var(name).ok().map(|value| {
            value
                .parse::<u64>()
                .unwrap_or_else(|_| panic!("{name} must be a number: {value}"))
        })
    };
    let single_seed = env("JAZZ_SEED");
    let commits = env("JAZZ_COMMIT_COUNT");
    let seeds: Vec<u64> = match single_seed {
        Some(seed) => vec![seed],
        None => (1..=env("SEEDS").unwrap_or(8)).collect(),
    };
    let mut rejected = 0;
    for seed in seeds {
        rejected += run(seed, commits.map_or(600, |commits| commits as usize));
    }
    // The default run must exercise rejections; a soak seed is checked
    // against the oracle only.
    if single_seed.is_none() && commits.is_none() {
        assert!(
            rejected > 20,
            "the run exercised server rejections: {rejected}"
        );
    }
}

/// One randomized run of `steps` writes; returns how many the server rejected.
fn run(seed: u64, steps: usize) -> usize {
    let server = block_on(Db::open_history_complete(DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([0xee; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    for r in 0..ROOMS {
        let Value::Uuid(id) = room(r) else {
            unreachable!()
        };
        server
            .seed_settled_mergeable_for_bootstrap(
                "rooms",
                RowUuid(id),
                AuthorSubject::SYSTEM,
                cells(&[("name", Value::String(format!("r{r}")))]),
            )
            .unwrap();
    }
    let mut net = Net {
        server,
        clients: Vec::new(),
    };
    for who in 0..CLIENTS {
        let db = open(0x60 + who as u8, user(who));
        net.clients.push(Client {
            db,
            who,
            upstream: None,
            live: Vec::new(),
            writes: Vec::new(),
        });
        net.connect(who);
        let queries = [
            Query::from("messages")
                .filter(eq(col("room"), lit(room(who % ROOMS))))
                .order_by("sent_at", OrderDirection::Desc)
                .limit(3),
            Query::from("messages").order_by("sent_at", OrderDirection::Asc),
            Query::from("memberships"),
            Query::from("notes"),
            Query::from("rooms")
                .order_by("name", OrderDirection::Asc)
                .limit(2),
            Query::from("messages")
                .order_by("sent_at", OrderDirection::Asc)
                .include("room"),
        ];
        let client = &mut net.clients[who];
        for query in queries {
            let query = client.db.prepare_query(&query).unwrap();
            let stream = block_on(client.db.subscribe(&query, ReadOpts::default())).unwrap();
            client.live.push(Live {
                query,
                stream,
                rows: Vec::new(),
            });
        }
    }
    let mut rng = Rng(seed);
    let mut rejected = 0;
    let (mut messages, mut memberships, mut notes) = (0usize, 0usize, 0usize);
    for step in 0..steps {
        let k = rng.next(CLIENTS);
        let pick = rng.next(10);
        let db = &net.clients[k].db;
        let result = match pick {
            // Post: rejected by the server unless k is a member.
            0 | 1 => {
                messages += 1;
                block_on(db.insert(
                    "messages",
                    cells(&[
                        ("room", room(rng.next(ROOMS))),
                        ("body", Value::String(format!("m{step}"))),
                        ("sent_at", Value::I64(step as i64)),
                    ]),
                    InsertOptions {
                        row_id: Some(row(3, messages)),
                        ..Default::default()
                    },
                ))
                .map(|w| w.mergeable_tx_id())
            }
            // Move or re-time a message k can see (else it fails locally).
            2 if messages > 0 => block_on(db.update(
                "messages",
                row(3, 1 + rng.next(messages)),
                cells(&[
                    ("room", room(rng.next(ROOMS))),
                    ("sent_at", Value::I64(1000 + step as i64)),
                ]),
                Default::default(),
            ))
            .map(|w| w.mergeable_tx_id()),
            // Join, or add someone else.
            3 | 4 => {
                memberships += 1;
                block_on(db.insert(
                    "memberships",
                    cells(&[
                        ("room", room(rng.next(ROOMS))),
                        ("member", subject(rng.next(CLIENTS))),
                    ]),
                    InsertOptions {
                        row_id: Some(row(2, memberships)),
                        ..Default::default()
                    },
                ))
                .map(|w| w.mergeable_tx_id())
            }
            5 if memberships > 0 => block_on(db.delete(
                "memberships",
                row(2, 1 + rng.next(memberships)),
                Default::default(),
            ))
            .map(|w| w.mergeable_tx_id()),
            // A note for oneself, or (rejected) for someone else.
            6 => {
                notes += 1;
                let owner = if rng.next(3) == 0 {
                    rng.next(CLIENTS)
                } else {
                    net.clients[k].who
                };
                block_on(db.insert(
                    "notes",
                    cells(&[
                        ("owner", subject(owner)),
                        ("text", Value::String(format!("n{step}"))),
                    ]),
                    InsertOptions {
                        row_id: Some(row(4, notes)),
                        ..Default::default()
                    },
                ))
                .map(|w| w.mergeable_tx_id())
            }
            7 => {
                let Value::Uuid(id) = room(rng.next(ROOMS)) else {
                    unreachable!()
                };
                block_on(db.update(
                    "rooms",
                    RowUuid(id),
                    cells(&[("name", Value::String(format!("n{step}")))]),
                    Default::default(),
                ))
                .map(|w| w.mergeable_tx_id())
            }
            // Go offline, or come back.
            8 => {
                let client = &mut net.clients[k];
                match client.upstream.take() {
                    Some(upstream) => {
                        assert!(client.db.detach_connection(&upstream));
                    }
                    None => net.connect(k),
                }
                Err(jazz::db::Error::new(jazz::db::ErrorCode::Query, "no write"))
            }
            _ => Err(jazz::db::Error::new(jazz::db::ErrorCode::Query, "no write")),
        };
        if let Ok(tx) = result {
            net.clients[k].writes.push(tx);
        }
        for _ in 0..rng.next(3) {
            net.pump();
        }
        if step % 25 == 24 {
            for index in 0..CLIENTS {
                if net.clients[index].upstream.is_none() {
                    net.connect(index);
                }
            }
            rejected += net.assert_converged(step);
        }
    }
    rejected
}
