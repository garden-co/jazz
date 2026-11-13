//! Fan-out of writes to tables that read policies read: N people, each in
//! K rooms and each subscribed to their room list (`common::member_chat`:
//! memberships ─► room ─► members ─► person ─► handles, plus the room's
//! call, latest seen mark and bots). Every table but the root is reached
//! through relations, and `members` and `people` are read by the read
//! policies too (membership `EXISTS`es, and handles' `INHERITS` of people).
//!
//! Measures four writes, each in three modes, on a SQLite file store:
//! - `off`: every relation and policy table watched whole (main);
//! - `correlate`: correlation watches on, policy-read tables whole;
//! - `pins`: correlation watches, and policy-read tables keyed by what
//!   their policies pin.
//!
//! Reports per write: groups woken, groups re-run, and the median dispatch
//! time (the write's own commit included). A local phase, not an
//! end-to-end claim: compare same-base CodSpeed runs for that.
//!
//! `USERS=100 ROOMS_PER_USER=5 OPS=40 cargo bench -p jazz --bench fanout_policy_tables`

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use jazz::db::{self, Db, DbConfig, DbIdentity, FileOptions, InsertOptions, ReadOpts, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::records::Value;

#[path = "../tests/common/mod.rs"]
mod common;

use common::member_chat::{claims, room_list, schema};

struct Config {
    users: usize,
    rooms_per_user: usize,
    ops: usize,
}

fn id(kind: u8, n: usize) -> RowUuid {
    let mut bytes = [0u8; 16];
    bytes[0] = kind;
    bytes[8..].copy_from_slice(&(n as u64).to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn user(n: usize) -> AuthorSubject {
    let mut bytes = [0u8; 16];
    bytes[0] = 0x56;
    bytes[8..].copy_from_slice(&(n as u64).to_be_bytes());
    AuthorSubject::for_test_bytes(bytes)
}

fn uuid(row: RowUuid) -> Value {
    Value::Uuid(row.0)
}

fn text(s: impl Into<String>) -> Value {
    Value::String(s.into())
}

fn cells(pairs: &[(&str, Value)]) -> BTreeMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect()
}

fn insert(db: &Db, table: &str, row: RowUuid, pairs: &[(&str, Value)]) {
    block_on(db.insert(
        table,
        cells(pairs),
        InsertOptions {
            row_id: Some(row),
            ..Default::default()
        },
    ))
    .unwrap();
}

fn update(db: &Db, table: &str, row: RowUuid, pairs: &[(&str, Value)]) {
    block_on(db.update(table, row, cells(pairs), Default::default())).unwrap();
}

const PERSON: u8 = 1;
const HANDLE: u8 = 2;
const ROOM: u8 = 3;
const MEMBER: u8 = 4;
const CALL: u8 = 5;
const SEEN: u8 = 6;
const BOT: u8 = 7;

/// Room of person `u`'s `k`-th membership: rooms are shared by the people
/// whose windows overlap, as group chats are.
fn room_of(u: usize, k: usize, rooms: usize) -> usize {
    (u + k * 7) % rooms
}

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Off,
    Correlate,
    Pins,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Mode::Off => "off      ",
            Mode::Correlate => "correlate",
            Mode::Pins => "pins     ",
        }
    }
}

struct Measured {
    woken: f64,
    reran: f64,
    median: Duration,
}

const WRITES: [&str; 4] = [
    "member joins a room",
    "member pins a room",
    "person renamed",
    "handle changed",
];

fn run(cfg: &Config, mode: Mode) -> Vec<(&'static str, Measured)> {
    db::set_correlation_filter_for_test(mode != Mode::Off);
    db::set_policy_pins_for_test(mode == Mode::Pins);
    let dir = tempfile::tempdir().unwrap();
    let db = block_on(Db::open_at(
        DbConfig::new(
            schema(),
            DbIdentity {
                node: NodeUuid::from_bytes([1; 16]),
                author: AuthorSubject::SYSTEM,
            },
        ),
        &dir.path().join("store.sqlite"),
        FileOptions::default(),
    ))
    .unwrap();
    let rooms = cfg.users;
    for u in 0..cfg.users {
        insert(
            &db,
            "people",
            id(PERSON, u),
            &[
                ("name", text(format!("p{u}"))),
                ("discoverable", Value::Bool(u % 2 == 0)),
            ],
        );
        insert(
            &db,
            "handles",
            id(HANDLE, u),
            &[
                ("person", uuid(id(PERSON, u))),
                ("handle", text(format!("h{u}"))),
            ],
        );
        db.set_identity_claims(user(u), claims(id(PERSON, u)));
    }
    for r in 0..rooms {
        insert(
            &db,
            "rooms",
            id(ROOM, r),
            &[("title", text(format!("r{r}")))],
        );
        insert(
            &db,
            "calls",
            id(CALL, r),
            &[
                ("room", uuid(id(ROOM, r))),
                ("kind", text("group")),
                ("ended", Value::Bool(r % 2 == 0)),
            ],
        );
        insert(
            &db,
            "bots",
            id(BOT, r),
            &[("room", uuid(id(ROOM, r))), ("name", text("bot"))],
        );
    }
    let mut members = 0;
    for u in 0..cfg.users {
        for k in 0..cfg.rooms_per_user {
            let room = id(ROOM, room_of(u, k, rooms));
            insert(
                &db,
                "members",
                id(MEMBER, members),
                &[
                    ("room", uuid(room)),
                    ("person", uuid(id(PERSON, u))),
                    ("banned", Value::Bool(false)),
                    ("left_at", Value::Nullable(None)),
                    ("pinned_at", Value::Nullable(None)),
                ],
            );
            insert(
                &db,
                "seen",
                id(SEEN, members),
                &[
                    ("room", uuid(room)),
                    ("person", uuid(id(PERSON, u))),
                    ("at", Value::I64(members as i64)),
                ],
            );
            members += 1;
        }
    }
    let mut streams = Vec::new();
    for u in 0..cfg.users {
        let prepared = db.prepare_query(&room_list(id(PERSON, u))).unwrap();
        streams.push(
            block_on(db.subscribe_for_identity(&prepared, ReadOpts::default(), user(u))).unwrap(),
        );
    }
    let drain = |streams: &mut Vec<db::SubscriptionStream>| {
        for stream in streams.iter_mut() {
            while stream.try_next_event().is_some() {}
        }
    };
    drain(&mut streams);
    let mut out = Vec::new();
    let mut joined = members;
    for op in WRITES {
        let mut times = Vec::with_capacity(cfg.ops);
        let before = db.fanout_stats();
        for i in 0..cfg.ops {
            let u = (i * 13) % cfg.users;
            let started = Instant::now();
            match op {
                "member joins a room" => {
                    // A room outside u's window, so it is a new membership.
                    let room = id(ROOM, room_of(u, cfg.rooms_per_user + 1, rooms));
                    insert(
                        &db,
                        "members",
                        id(MEMBER, joined),
                        &[
                            ("room", uuid(room)),
                            ("person", uuid(id(PERSON, u))),
                            ("banned", Value::Bool(false)),
                            ("left_at", Value::Nullable(None)),
                            ("pinned_at", Value::Nullable(None)),
                        ],
                    );
                    joined += 1;
                }
                "member pins a room" => update(
                    &db,
                    "members",
                    id(MEMBER, u * cfg.rooms_per_user),
                    &[("pinned_at", Value::I64(i as i64))],
                ),
                "person renamed" => update(
                    &db,
                    "people",
                    id(PERSON, u),
                    &[("name", text(format!("p{u}.{i}")))],
                ),
                _ => update(
                    &db,
                    "handles",
                    id(HANDLE, u),
                    &[("handle", text(format!("h{u}.{i}")))],
                ),
            }
            times.push(started.elapsed());
            drain(&mut streams);
        }
        let after = db.fanout_stats();
        times.sort();
        let per = |a: u64, b: u64| (a - b) as f64 / cfg.ops as f64;
        out.push((
            op,
            Measured {
                woken: per(after.woken, before.woken),
                reran: per(after.reran, before.reran),
                median: times[times.len() / 2],
            },
        ));
    }
    db::set_correlation_filter_for_test(true);
    db::set_policy_pins_for_test(true);
    out
}

fn main() {
    let env = |k: &str, d: usize| {
        std::env::var(k)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(d)
    };
    let cfg = Config {
        users: env("USERS", 100),
        rooms_per_user: env("ROOMS_PER_USER", 5),
        ops: env("OPS", 40),
    };
    println!(
        "people={} rooms={} rooms/person={} ops={} subscriptions={}",
        cfg.users, cfg.users, cfg.rooms_per_user, cfg.ops, cfg.users
    );
    {
        // ABCCBA.
        let order = [
            Mode::Off,
            Mode::Correlate,
            Mode::Pins,
            Mode::Pins,
            Mode::Correlate,
            Mode::Off,
        ];
        let mut runs = Vec::new();
        let only = std::env::var("MODE").ok();
        for mode in order {
            if only.as_deref().is_some_and(|m| m != mode.name().trim()) {
                continue;
            }
            runs.push((mode, run(&cfg, mode)));
        }
        for op in WRITES {
            for (mode, results) in &runs {
                let Some((_, m)) = results.iter().find(|(o, _)| *o == op) else {
                    continue;
                };
                println!(
                    "  {op:<20} {}  woken/write {:>6.1}  reran/write {:>6.1}  median {:>10.3?}",
                    mode.name(),
                    m.woken,
                    m.reran,
                    m.median
                );
            }
        }
    }
}
