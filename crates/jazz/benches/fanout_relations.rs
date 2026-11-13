//! Fan-out of writes to tables a live result reads only through its array
//! relations: N members, each subscribed to a chat list (chats ─► last
//! message ─► read marks) and to one open thread (messages of a chat ─► the
//! message each replies to). Measures one read-mark write and one message
//! insert, A/B with correlation watches off (every relation table watched
//! whole, as before them) and on, in ABBA order, on a SQLite file store.
//!
//! Reports per write: groups woken, groups re-run, changed rows a watching
//! group skipped by correlation, and the median dispatch time (the write's
//! own commit included). A local phase, not an end-to-end claim: compare
//! same-base CodSpeed runs for that.
//!
//! `USERS=100 CHATS_PER_USER=5 MESSAGES=20 OPS=40 cargo bench -p jazz --bench fanout_relations`

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use jazz::db::{self, Db, DbConfig, DbIdentity, FileOptions, InsertOptions, ReadOpts, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{ArraySubquery, OrderDirection, Query, col, eq, lit};
use jazz::records::Value;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

#[path = "../tests/common/mod.rs"]
#[allow(dead_code)]
mod common;

use common::{
    allow_all_policies, compile_schema, exists, outer_eq, read_and_allow_all_writes, session_eq,
};

struct Config {
    users: usize,
    chats_per_user: usize,
    messages: usize,
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

fn subject(n: usize) -> Value {
    Value::String(user(n).principal_parts().1)
}

fn uuid(row: RowUuid) -> Value {
    Value::Uuid(row.0)
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

const CHAT: u8 = 1;
const MEMBER: u8 = 2;
const MESSAGE: u8 = 3;
const MARK: u8 = 4;

/// Chat `c` of user `u`'s `k`-th membership: chats are shared by the users
/// whose windows overlap, as group chats are.
fn chat_of(u: usize, k: usize, chats: usize) -> usize {
    (u + k * 7) % chats
}

struct Measured {
    woken: f64,
    reran: f64,
    uncorrelated: f64,
    median: Duration,
}

fn run(cfg: &Config, correlate: bool) -> Vec<(&'static str, Measured)> {
    db::set_correlation_filter_for_test(correlate);
    let me = ["identity", "subject"];
    let member_of = |column: &str| {
        exists(
            "members",
            vec![outer_eq("chat", column), session_eq("member", &me)],
        )
    };
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("chats")
                    .column("title", ColumnType::Text)
                    .policies(read_and_allow_all_writes(member_of("id"))),
            )
            .table(
                TableSchemaBuilder::new("members")
                    .fk_column("chat", "chats")
                    .column("member", ColumnType::Text)
                    .index_only(["member"])
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("messages")
                    .fk_column("chat", "chats")
                    .column("body", ColumnType::Text)
                    .column("sent_at", ColumnType::BigInt)
                    .nullable_fk_column("reply_to", "messages")
                    .composite_index(["chat", "sent_at"])
                    .policies(read_and_allow_all_writes(member_of("chat"))),
            )
            .table(
                TableSchemaBuilder::new("read_marks")
                    .fk_column("message", "messages")
                    .column("reader", ColumnType::Text)
                    .column("at", ColumnType::BigInt)
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    let dir = tempfile::tempdir().unwrap();
    let db = block_on(Db::open_at(
        DbConfig::new(
            schema,
            DbIdentity {
                node: NodeUuid::from_bytes([1; 16]),
                author: AuthorSubject::SYSTEM,
            },
        ),
        &dir.path().join("store.sqlite"),
        FileOptions::default(),
    ))
    .unwrap();
    let chats = cfg.users;
    for c in 0..chats {
        insert(
            &db,
            "chats",
            id(CHAT, c),
            &[("title", Value::String(format!("c{c}")))],
        );
    }
    let mut members = 0;
    for u in 0..cfg.users {
        for k in 0..cfg.chats_per_user {
            insert(
                &db,
                "members",
                id(MEMBER, members),
                &[
                    ("chat", uuid(id(CHAT, chat_of(u, k, chats)))),
                    ("member", subject(u)),
                ],
            );
            members += 1;
        }
    }
    let message = |c: usize, n: usize| id(MESSAGE, c * 100_000 + n);
    for c in 0..chats {
        for n in 0..cfg.messages {
            let reply = if n > 0 && n % 3 == 0 {
                uuid(message(c, n - 1))
            } else {
                Value::Nullable(None)
            };
            insert(
                &db,
                "messages",
                message(c, n),
                &[
                    ("chat", uuid(id(CHAT, c))),
                    ("body", Value::String(format!("m{n}"))),
                    ("sent_at", Value::I64(n as i64)),
                    ("reply_to", reply),
                ],
            );
        }
    }
    let mut streams = Vec::new();
    for u in 0..cfg.users {
        let list = Query::from("chats").array_subquery(
            ArraySubquery::new("last", "messages", "chat", "id")
                .order_by("sent_at", OrderDirection::Desc)
                .limit(1)
                .nested(ArraySubquery::new("marks", "read_marks", "message", "id")),
        );
        let thread = Query::from("messages")
            .filter(eq(col("chat"), lit(uuid(id(CHAT, chat_of(u, 0, chats))))))
            .order_by("sent_at", OrderDirection::Asc)
            .array_subquery(ArraySubquery::new("reply", "messages", "id", "reply_to"));
        for query in [list, thread] {
            let prepared = db.prepare_query(&query).unwrap();
            streams.push(
                block_on(db.subscribe_for_identity(&prepared, ReadOpts::default(), user(u)))
                    .unwrap(),
            );
        }
    }
    let drain = |streams: &mut Vec<db::SubscriptionStream>| {
        for stream in streams.iter_mut() {
            while stream.try_next_event().is_some() {}
        }
    };
    drain(&mut streams);
    let mut out = Vec::new();
    let mut last = vec![cfg.messages - 1; chats];
    let mut marks = 0;
    for op in ["read mark on a last message", "message insert"] {
        let mut times = Vec::with_capacity(cfg.ops);
        let before = db.fanout_stats();
        for i in 0..cfg.ops {
            let c = (i * 13) % chats;
            let started = Instant::now();
            match op {
                "read mark on a last message" => {
                    insert(
                        &db,
                        "read_marks",
                        id(MARK, marks),
                        &[
                            ("message", uuid(message(c, last[c]))),
                            ("reader", subject(c)),
                            ("at", Value::I64(i as i64)),
                        ],
                    );
                    marks += 1;
                }
                _ => {
                    last[c] += 1;
                    insert(
                        &db,
                        "messages",
                        message(c, last[c]),
                        &[
                            ("chat", uuid(id(CHAT, c))),
                            ("body", Value::String(format!("new {i}"))),
                            ("sent_at", Value::I64(last[c] as i64)),
                            ("reply_to", uuid(message(c, last[c] - 1))),
                        ],
                    );
                }
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
                uncorrelated: per(after.uncorrelated, before.uncorrelated),
                median: times[times.len() / 2],
            },
        ));
    }
    db::set_correlation_filter_for_test(true);
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
        chats_per_user: env("CHATS_PER_USER", 5),
        messages: env("MESSAGES", 20),
        ops: env("OPS", 40),
    };
    println!(
        "users={} chats={} chats/user={} messages/chat={} ops={} subscriptions={}",
        cfg.users,
        cfg.users,
        cfg.chats_per_user,
        cfg.messages,
        cfg.ops,
        cfg.users * 2
    );
    // ABBA: off, on, on, off.
    let mut runs: Vec<(bool, Vec<(&'static str, Measured)>)> = Vec::new();
    for correlate in [false, true, true, false] {
        runs.push((correlate, run(&cfg, correlate)));
    }
    for (correlate, results) in &runs {
        let mode = if *correlate {
            "correlation on "
        } else {
            "correlation off"
        };
        for (op, m) in results {
            println!(
                "  {mode}  {op:<28} woken/write {:>7.1}  reran/write {:>7.1}  skipped-by-correlation/write {:>7.1}  median {:>10.3?}",
                m.woken, m.reran, m.uncorrelated, m.median
            );
        }
    }
}
