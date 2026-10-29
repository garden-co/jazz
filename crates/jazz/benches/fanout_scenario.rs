// Server fan-out scenario, run by benches/fanout.rs.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use jazz::db;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::records::Value;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use super::common::{
    allow_all_policies, compile_schema, exists, outer_eq, read_and_allow_all_writes, session_eq,
};
use super::{Config, Report, rss_kib};

fn id(kind: u8, n: usize) -> RowUuid {
    let mut bytes = [0u8; 16];
    bytes[0] = kind;
    bytes[8..].copy_from_slice(&(n as u64).to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn user(n: usize) -> AuthorSubject {
    let mut bytes = [0u8; 16];
    bytes[0] = 0x55;
    bytes[8..].copy_from_slice(&(n as u64).to_be_bytes());
    AuthorSubject::for_test_bytes(bytes)
}

fn subject(n: usize) -> String {
    user(n).principal_parts().1
}

fn text(value: impl Into<String>) -> Value {
    Value::String(value.into())
}

fn cells(pairs: &[(&str, Value)]) -> BTreeMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect()
}

/// Rooms are numbered so user `u` belongs to rooms `u, u+1, ..` (mod rooms)
/// and keeps room `u` open.
fn room_of(user: usize, k: usize, cfg: &Config) -> usize {
    (user + k * 7) % cfg.rooms
}

pub fn run(cfg: &Config) -> Report {
    let me = ["user", "identity", "subject"];
    let schema = compile_schema(
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
                    .index_only(["member"])
                    .policies(read_and_allow_all_writes(session_eq("member", &me))),
            )
            .table(
                TableSchemaBuilder::new("messages")
                    .fk_column("room", "rooms")
                    .column("author", ColumnType::Text)
                    .column("body", ColumnType::Text)
                    .column("sent_at", ColumnType::BigInt)
                    .composite_index(["room", "sent_at"])
                    .policies(read_and_allow_all_writes(exists(
                        "memberships",
                        vec![outer_eq("room", "room"), session_eq("member", &me)],
                    ))),
            )
            .table(
                TableSchemaBuilder::new("announcements")
                    .column("body", ColumnType::Text)
                    .column("posted_at", ColumnType::BigInt)
                    .index_only(["posted_at"])
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    let db = db::block_on(db::Db::open(db::DbConfig::new(
        schema,
        db::DbIdentity {
            node: NodeUuid::from_bytes([0x11; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    let insert = |table: &str, row: RowUuid, values: BTreeMap<String, Value>| {
        db::block_on(db.insert(
            table,
            values,
            db::InsertOptions {
                row_id: Some(row),
                ..Default::default()
            },
        ))
        .unwrap();
    };

    // Fixture.
    for r in 0..cfg.rooms {
        insert(
            "rooms",
            id(1, r),
            cells(&[("name", text(format!("room {r}")))]),
        );
    }
    let mut membership = 0;
    for u in 0..cfg.users {
        for k in 0..cfg.rooms_per_user {
            insert(
                "memberships",
                id(2, membership),
                cells(&[
                    ("room", Value::Uuid(id(1, room_of(u, k, cfg)).0)),
                    ("member", text(subject(u))),
                ]),
            );
            membership += 1;
        }
    }
    let mut sent = 0usize;
    for r in 0..cfg.rooms {
        for _ in 0..cfg.messages_per_room {
            insert(
                "messages",
                id(3, sent),
                cells(&[
                    ("room", Value::Uuid(id(1, r).0)),
                    ("author", text(subject(r % cfg.users))),
                    ("body", text(format!("message {sent}"))),
                    ("sent_at", Value::I64(sent as i64)),
                ]),
            );
            sent += 1;
        }
    }
    for a in 0..20 {
        insert(
            "announcements",
            id(4, a),
            cells(&[
                ("body", text(format!("a{a}"))),
                ("posted_at", Value::I64(a as i64)),
            ]),
        );
    }
    db::block_on(db.tick()).unwrap();

    // Subscriptions: per user, the open room's latest page, their room list,
    // and the public announcements feed (identical for everyone).
    let rss_before = rss_kib();
    let started = Instant::now();
    let mut subs = Vec::new();
    for u in 0..cfg.users {
        let room = id(1, room_of(u, 0, cfg));
        let open_room = db
            .prepare_query(
                &Query::from("messages")
                    .filter(eq(col("room"), lit(Value::Uuid(room.0))))
                    .order_by("sent_at", OrderDirection::Desc)
                    .limit(30),
            )
            .unwrap();
        let my_rooms = db.prepare_query(&Query::from("memberships")).unwrap();
        let feed = db
            .prepare_query(
                &Query::from("announcements")
                    .order_by("posted_at", OrderDirection::Desc)
                    .limit(20),
            )
            .unwrap();
        for prepared in [open_room, my_rooms, feed] {
            let mut sub = db::block_on(db.subscribe_for_identity(
                &prepared,
                db::ReadOpts::default(),
                user(u),
            ))
            .unwrap();
            db::block_on(db.tick()).unwrap();
            while sub.try_next_event().is_some() {}
            subs.push(sub);
        }
    }
    let subscribe_time = started.elapsed();
    let rss_after = rss_kib();

    let mut drain = |subs: &mut Vec<db::SubscriptionStream>| -> usize {
        let mut events = 0;
        for sub in subs.iter_mut() {
            while let Some(event) = sub.try_next_event() {
                if matches!(event, db::SubscriptionEvent::Delta { .. }) {
                    events += 1;
                }
            }
        }
        events
    };
    let mut measure = |name: &'static str,
                       n: usize,
                       op: &mut dyn FnMut(usize)|
     -> (&'static str, Duration, usize) {
        let mut times = Vec::new();
        let mut events = 0;
        for i in 0..n {
            let t = Instant::now();
            op(i);
            if std::env::var("NOTICK").is_err() {
                db::block_on(db.tick()).unwrap();
            }
            let delivered = drain(&mut subs);
            times.push(t.elapsed());
            events += delivered;
        }
        times.sort();
        (name, times[times.len() / 2], events / n)
    };

    let mut ops = Vec::new();
    // A message in a room one user has open (and `rooms_per_user` members).
    ops.push(measure("post message", cfg.ops, &mut |i| {
        let r = room_of(i % cfg.users, 0, cfg);
        insert(
            "messages",
            id(5, i),
            cells(&[
                ("room", Value::Uuid(id(1, r).0)),
                ("author", text(subject(i % cfg.users))),
                ("body", text("new")),
                ("sent_at", Value::I64((1_000_000 + i) as i64)),
            ]),
        );
    }));
    // Edit a message that is on its room's open page.
    ops.push(measure("edit visible message", cfg.ops, &mut |i| {
        db::block_on(db.update(
            "messages",
            id(5, i),
            cells(&[("body", text("edited"))]),
            Default::default(),
        ))
        .unwrap();
    }));
    // A user joins a room: their room list and policy evidence change.
    ops.push(measure("join room", cfg.ops, &mut |i| {
        insert(
            "memberships",
            id(6, i),
            cells(&[
                ("room", Value::Uuid(id(1, (i * 13 + 5) % cfg.rooms).0)),
                ("member", text(subject(i % cfg.users))),
            ]),
        );
    }));
    // A public announcement: every user's feed changes.
    ops.push(measure("post announcement", cfg.ops.min(20), &mut |i| {
        insert(
            "announcements",
            id(7, i),
            cells(&[
                ("body", text("news")),
                ("posted_at", Value::I64(1_000 + i as i64)),
            ]),
        );
    }));
    Report {
        subscriptions: subs.len(),
        subscribe_time,
        rss_delta_kib: rss_after.saturating_sub(rss_before),
        ops,
    }
}
