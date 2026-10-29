// Fan-out through the sync path: one server Db, N client Dbs each signed in
// as its own user over an in-memory duplex transport. Run by
// benches/fanout_wire.rs.

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;
use std::time::{Duration, Instant};

use jazz::db;
use jazz::db::SyncMessage as SyncMsg;
use jazz::db::TransportError;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::records::Value;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use super::common::{
    allow_all_policies, compile_schema, exists, outer_eq, read_and_allow_all_writes, session_eq,
};
use super::{Config, WireReport, rss_kib};

type MessageQueue = Rc<RefCell<VecDeque<SyncMsg>>>;

struct Duplex {
    outbound: MessageQueue,
    inbound: MessageQueue,
}

impl db::Transport for Duplex {
    fn send(&mut self, message: SyncMsg) -> Result<(), TransportError> {
        self.outbound.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMsg> {
        self.inbound.borrow_mut().pop_front()
    }
}

/// (client side, server side, server→client queue for counting).
fn duplex() -> (Box<dyn db::Transport>, Box<dyn db::Transport>, MessageQueue) {
    let up = Rc::new(RefCell::new(VecDeque::new()));
    let down = Rc::new(RefCell::new(VecDeque::new()));
    (
        Box::new(Duplex {
            outbound: up.clone(),
            inbound: down.clone(),
        }),
        Box::new(Duplex {
            outbound: down.clone(),
            inbound: up,
        }),
        down,
    )
}

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

fn room_of(user: usize, k: usize, cfg: &Config) -> usize {
    (user + k * 7) % cfg.rooms
}

pub fn run(cfg: &Config) -> WireReport {
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
    let config = |node: [u8; 16], author: AuthorSubject| {
        db::DbConfig::new(
            schema.clone(),
            db::DbIdentity {
                node: NodeUuid::from_bytes(node),
                author,
            },
        )
    };
    let open = |node: [u8; 16], author: AuthorSubject| {
        db::block_on(db::Db::open(config(node, author))).unwrap()
    };
    let server = db::block_on(db::Db::open_history_complete(config(
        [0x11; 16],
        AuthorSubject::SYSTEM,
    )))
    .unwrap();
    let seed = |table: &str, row: RowUuid, values: BTreeMap<String, Value>| {
        server
            .seed_settled_mergeable_for_bootstrap(table, row, AuthorSubject::SYSTEM, values)
            .unwrap();
    };
    for r in 0..cfg.rooms {
        seed(
            "rooms",
            id(1, r),
            cells(&[("name", text(format!("room {r}")))]),
        );
    }
    let mut membership = 0;
    for u in 0..cfg.users {
        for k in 0..cfg.rooms_per_user {
            seed(
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
            seed(
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
        seed(
            "announcements",
            id(4, a),
            cells(&[
                ("body", text(format!("a{a}"))),
                ("posted_at", Value::I64(a as i64)),
            ]),
        );
    }
    db::block_on(server.tick()).unwrap();

    // Clients: each its own Db, connected and subscribed like an app.
    let rss_start = rss_kib();
    let mut clients = Vec::new();
    let mut downs = Vec::new();
    for u in 0..cfg.users {
        let mut node = [0u8; 16];
        node[0] = 0x77;
        node[8..].copy_from_slice(&(u as u64).to_be_bytes());
        let client = open(node, user(u));
        let (client_side, server_side, down) = duplex();
        let _upstream = db::block_on(client.connect_upstream(client_side));
        let _subscriber = server.accept_subscriber(server_side, user(u));
        downs.push(down);
        clients.push(client);
    }
    let rss_clients = rss_kib();
    let started = Instant::now();
    let mut streams = Vec::new();
    for (u, client) in clients.iter().enumerate() {
        let room = id(1, room_of(u, 0, cfg));
        let queries = [
            Query::from("messages")
                .filter(eq(col("room"), lit(Value::Uuid(room.0))))
                .order_by("sent_at", OrderDirection::Desc)
                .limit(30),
            Query::from("memberships"),
            Query::from("announcements")
                .order_by("posted_at", OrderDirection::Desc)
                .limit(20),
        ];
        for query in queries {
            let prepared = client.prepare_query(&query).unwrap();
            streams.push((
                u,
                db::block_on(client.subscribe(&prepared, db::ReadOpts::default())).unwrap(),
            ));
        }
    }
    let pump = |active: &[usize]| -> (Duration, Duration) {
        for &u in active {
            db::block_on(clients[u].tick()).unwrap();
        }
        let t = Instant::now();
        db::block_on(server.tick()).unwrap();
        let server_time = t.elapsed();
        let t = Instant::now();
        for (u, client) in clients.iter().enumerate() {
            if !downs[u].borrow().is_empty() {
                db::block_on(client.tick()).unwrap();
            }
        }
        (server_time, t.elapsed())
    };
    let everyone: Vec<usize> = (0..cfg.users).collect();
    for _ in 0..6 {
        pump(&everyone);
    }
    let subscribe_time = started.elapsed();
    let rss_synced = rss_kib();
    let mut held = 0usize;
    for (u, client) in clients.iter().enumerate() {
        let room = id(1, room_of(u, 0, cfg));
        for query in [
            Query::from("messages")
                .filter(eq(col("room"), lit(Value::Uuid(room.0))))
                .order_by("sent_at", OrderDirection::Desc)
                .limit(30),
            Query::from("announcements")
                .order_by("posted_at", OrderDirection::Desc)
                .limit(20),
        ] {
            let prepared = client.prepare_query(&query).unwrap();
            held += db::block_on(client.all(&prepared, db::ReadOpts::default()))
                .unwrap()
                .len();
        }
    }
    eprintln!("initial rows held across clients (messages + announcements windows): {held}");
    for (_, stream) in streams.iter_mut() {
        while stream.try_next_event().is_some() {}
    }

    let mut ops = Vec::new();
    let mut measure = |name: &'static str, n: usize, op: &mut dyn FnMut(usize) -> usize| {
        let (mut server_times, mut client_times, mut totals) = (Vec::new(), Vec::new(), Vec::new());
        let mut events = 0;
        let mut messages = 0;
        for i in 0..n {
            let t = Instant::now();
            let writer = op(i);
            // Upload, authority, delivery; a second round settles the writer.
            let mut server_time = Duration::ZERO;
            let mut client_time = Duration::ZERO;
            // Pump until nothing is in flight (at least two rounds).
            for round in 0..12 {
                messages += downs.iter().map(|d| d.borrow().len()).sum::<usize>();
                let (s, c) = pump(&[writer]);
                server_time += s;
                client_time += c;
                if round >= 1 && downs.iter().all(|d| d.borrow().is_empty()) {
                    break;
                }
            }
            for (_, stream) in streams.iter_mut() {
                while let Some(event) = stream.try_next_event() {
                    if let db::SubscriptionEvent::Delta {
                        added,
                        updated,
                        removed,
                        ..
                    } = event
                        && !(added.is_empty() && updated.is_empty() && removed.is_empty())
                    {
                        events += 1;
                    }
                }
            }
            totals.push(t.elapsed());
            server_times.push(server_time);
            client_times.push(client_time);
        }
        server_times.sort();
        client_times.sort();
        totals.sort();
        ops.push((
            name,
            server_times[n / 2],
            client_times[n / 2],
            totals[n / 2],
            events / n,
        ));
        let _ = messages;
    };
    let insert = |client: &db::Db, table: &str, row: RowUuid, values: BTreeMap<String, Value>| {
        db::block_on(client.insert(
            table,
            values,
            db::InsertOptions {
                row_id: Some(row),
                ..Default::default()
            },
        ))
        .unwrap();
    };
    measure("post message", cfg.ops, &mut |i| {
        let u = (i * 31) % cfg.users;
        let r = room_of(u, 0, cfg);
        insert(
            &clients[u],
            "messages",
            id(5, i),
            cells(&[
                ("room", Value::Uuid(id(1, r).0)),
                ("author", text(subject(u))),
                ("body", text("new")),
                ("sent_at", Value::I64((1_000_000 + i) as i64)),
            ]),
        );
        u
    });
    measure("edit visible message", cfg.ops, &mut |i| {
        let u = (i * 31) % cfg.users;
        db::block_on(clients[u].update(
            "messages",
            id(5, i),
            cells(&[("body", text("edited"))]),
            Default::default(),
        ))
        .unwrap();
        u
    });
    measure("join room", cfg.ops, &mut |i| {
        let u = (i * 17) % cfg.users;
        insert(
            &clients[u],
            "memberships",
            id(6, i),
            cells(&[
                ("room", Value::Uuid(id(1, (i * 13 + 5) % cfg.rooms).0)),
                ("member", text(subject(u))),
            ]),
        );
        u
    });
    measure("post announcement", cfg.ops.min(20), &mut |i| {
        let u = i % cfg.users;
        insert(
            &clients[u],
            "announcements",
            id(7, i),
            cells(&[
                ("body", text("news")),
                ("posted_at", Value::I64(1_000 + i as i64)),
            ]),
        );
        u
    });
    WireReport {
        clients: cfg.users,
        subscriptions: streams.len(),
        sync_time: subscribe_time,
        rss_clients_kib: rss_clients.saturating_sub(rss_start),
        rss_synced_kib: rss_synced.saturating_sub(rss_clients),
        ops,
    }
}
