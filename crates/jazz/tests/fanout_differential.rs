//! Keyed invalidation and shared execution against one-shot reads: random
//! writes (including ones that move rows between watch keys and change
//! policy evidence) under many identities' subscriptions; after every
//! commit each maintained result must equal a fresh read. Not ported from
//! Jazz: this pins the prototype's fan-out (src/fanout.rs).

use crate::common;

use std::collections::BTreeMap;

use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, SubscriptionEvent, SubscriptionStream,
    block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::records::Value;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{
    allow_all_policies, compile_schema, exists, outer_eq, read_and_allow_all_writes, session_eq,
};

const USERS: usize = 6;
const ROOMS: usize = 4;

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

fn open() -> Db {
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
    );
    block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([1; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap()
}

struct Live {
    query: jazz::db::PreparedQuery,
    identity: AuthorSubject,
    stream: SubscriptionStream,
    rows: Vec<(RowUuid, Vec<(String, Option<Value>)>)>,
    /// Includes `room`: a renamed room must update every row in it.
    includes_room: bool,
}

fn owned(row: &jazz::db::CurrentRow) -> Vec<(String, Option<Value>)> {
    row.cells()
        .map(|(name, value)| (name.to_owned(), value.cloned()))
        .collect()
}

fn snapshot(rows: &[jazz::db::CurrentRow]) -> Vec<(RowUuid, Vec<(String, Option<Value>)>)> {
    rows.iter().map(|r| (r.row_uuid(), owned(r))).collect()
}

/// Applies queued deltas; returns the ids reported as updated.
fn apply(live: &mut Live) -> std::collections::BTreeSet<RowUuid> {
    let mut reported = std::collections::BTreeSet::new();
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
        // Remove what left or changed, then place added and updated rows at
        // their final indices; unchanged rows keep their relative order.
        let moved: std::collections::BTreeSet<RowUuid> = removed
            .iter()
            .map(|gone| gone.row_uuid)
            .chain(updated.iter().map(|row| row.row_uuid()))
            .collect();
        reported.extend(updated.iter().map(|row| row.row_uuid()));
        live.rows.retain(|(id, _)| !moved.contains(id));
        let mut placed: Vec<_> = added.iter().chain(updated.iter()).collect();
        placed.sort_by_key(|row| row.index);
        for row in placed {
            live.rows.insert(row.index, (row.row_uuid(), owned(row)));
        }
    }
    reported
}

#[test]
fn maintained_results_match_one_shot_reads_under_random_writes() {
    for seed in [7, 11, 23, 42, 99] {
        run(seed);
    }
}

fn run(seed: u64) {
    let db = open();
    for r in 0..ROOMS {
        let Value::Uuid(id) = room(r) else {
            unreachable!()
        };
        block_on(db.insert(
            "rooms",
            cells(&[("name", Value::String(format!("r{r}")))]),
            InsertOptions {
                row_id: Some(RowUuid(id)),
                ..Default::default()
            },
        ))
        .unwrap();
    }
    let mut live = Vec::new();
    for u in 0..USERS {
        let queries = [
            Query::from("messages")
                .filter(eq(col("room"), lit(room(u % ROOMS))))
                .order_by("sent_at", OrderDirection::Desc)
                .limit(3),
            Query::from("messages").order_by("sent_at", OrderDirection::Asc),
            // A first page ordered by a column it does not select.
            Query::from("messages")
                .select(["body"])
                .order_by("sent_at", OrderDirection::Asc)
                .limit(4),
            Query::from("memberships"),
            Query::from("rooms")
                .order_by("name", OrderDirection::Asc)
                .limit(2),
            Query::from("messages").include("room"),
        ];
        // Two subscriptions per shape for some users: shared groups.
        for query in queries.iter().chain(queries.iter().take(u % 2)) {
            let query = db.prepare_query(query).unwrap();
            let stream =
                block_on(db.subscribe_for_identity(&query, ReadOpts::default(), user(u))).unwrap();
            let includes_room = !query.query().includes.is_empty();
            live.push(Live {
                query,
                identity: user(u),
                stream,
                rows: Vec::new(),
                includes_room,
            });
        }
    }
    let mut rng = Rng(seed);
    let (mut messages, mut memberships) = (0usize, 0usize);
    for step in 0..400 {
        let pick = rng.next(8);
        let mut renamed = None;
        let result = match pick {
            0 | 1 => {
                messages += 1;
                block_on(db.insert(
                    "messages",
                    cells(&[
                        ("room", room(rng.next(ROOMS))),
                        ("body", Value::String(format!("m{step}"))),
                        ("sent_at", Value::I64(rng.next(50) as i64)),
                    ]),
                    InsertOptions {
                        row_id: Some(row(3, messages)),
                        ..Default::default()
                    },
                ))
                .map(drop)
            }
            // Move a message between rooms: leaves one watch key, enters another.
            2 if messages > 0 => block_on(db.update(
                "messages",
                row(3, 1 + rng.next(messages)),
                cells(&[
                    ("room", room(rng.next(ROOMS))),
                    ("sent_at", Value::I64(rng.next(50) as i64)),
                ]),
                Default::default(),
            ))
            .map(drop),
            3 => {
                memberships += 1;
                block_on(db.insert(
                    "memberships",
                    cells(&[
                        ("room", room(rng.next(ROOMS))),
                        ("member", subject(rng.next(USERS))),
                    ]),
                    InsertOptions {
                        row_id: Some(row(2, memberships)),
                        ..Default::default()
                    },
                ))
                .map(drop)
            }
            // Hand a membership to someone else: policy evidence moves between sessions.
            4 if memberships > 0 => block_on(db.update(
                "memberships",
                row(2, 1 + rng.next(memberships)),
                cells(&[("member", subject(rng.next(USERS)))]),
                Default::default(),
            ))
            .map(drop),
            5 if memberships > 0 => block_on(db.delete(
                "memberships",
                row(2, 1 + rng.next(memberships)),
                Default::default(),
            ))
            .map(drop),
            6 => {
                let Value::Uuid(id) = room(rng.next(ROOMS)) else {
                    unreachable!()
                };
                renamed = Some(id);
                block_on(db.update(
                    "rooms",
                    RowUuid(id),
                    cells(&[("name", Value::String(format!("n{step}")))]),
                    Default::default(),
                ))
                .map(drop)
            }
            // Delete a message: a full page that loses a row must refill.
            7 if messages > 0 => block_on(db.delete(
                "messages",
                row(3, 1 + rng.next(messages)),
                Default::default(),
            ))
            .map(drop),
            _ => Ok(()),
        };
        let _ = result;
        for sub in &mut live {
            let reported = apply(sub);
            let fresh =
                block_on(db.all_for_identity(&sub.query, ReadOpts::default(), sub.identity))
                    .unwrap();
            assert_eq!(
                sub.rows,
                snapshot(&fresh),
                "seed {seed}: diverged after step {step} (op {pick})"
            );
            if let (true, Some(id)) = (sub.includes_room, renamed) {
                for row in fresh.iter().filter(|row| {
                    row.cells()
                        .any(|(name, value)| name == "room" && value == Some(&Value::Uuid(id)))
                }) {
                    assert!(
                        reported.contains(&row.row_uuid()),
                        "seed {seed}: step {step} renamed a room but its message was not updated"
                    );
                }
            }
        }
    }
    let stats = db.fanout_stats();
    assert!(
        stats.shared_joins > 0,
        "duplicate shapes share a group: {stats:?}"
    );
    assert!(
        stats.skipped > stats.woken,
        "keyed invalidation skips most groups: {stats:?}"
    );
}

/// A client's synced subscriptions (rows the server lists) re-check the
/// rows a commit wrote instead of re-running: random writes on either end,
/// and after each round trip every maintained result equals a fresh read.
#[test]
fn synced_results_recheck_written_rows_and_match_one_shot_reads() {
    use jazz::db::{DeleteOptions, ReadTier, UpdateOptions, duplex};
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("tasks")
                    .column("title", ColumnType::Text)
                    .column("done", ColumnType::Boolean)
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    let node = |n: u8| DbIdentity {
        node: NodeUuid::from_bytes([n; 16]),
        author: AuthorSubject::SYSTEM,
    };
    let worker = block_on(Db::open(DbConfig::new(schema.clone(), node(1)))).unwrap();
    let client = block_on(Db::open(DbConfig::new(schema, node(2)))).unwrap();
    let (client_side, server_side) = duplex();
    let _upstream = block_on(client.connect_upstream(client_side));
    worker.accept_subscriber(server_side, AuthorSubject::SYSTEM);
    let pump = || {
        for _ in 0..4 {
            block_on(client.tick()).unwrap();
            block_on(worker.tick()).unwrap();
        }
        block_on(client.tick()).unwrap();
    };
    let task = |n: usize| row(7, n);
    let task_cells = |n: usize, done: bool| {
        cells(&[
            ("title", Value::String(format!("t{:02}", n % 13))),
            ("done", Value::Bool(done)),
        ])
    };
    for n in 0..20 {
        block_on(worker.insert(
            "tasks",
            task_cells(n, n % 3 == 0),
            InsertOptions {
                row_id: Some(task(n)),
                ..Default::default()
            },
        ))
        .unwrap();
    }
    let mut live: Vec<Live> = [
        Query::from("tasks"),
        Query::from("tasks").filter(eq(col("done"), lit(Value::Bool(true)))),
        Query::from("tasks")
            .order_by("title", OrderDirection::Asc)
            .limit(5),
        Query::from("tasks")
            .select(["done"])
            .order_by("title", OrderDirection::Desc),
    ]
    .into_iter()
    .map(|query| {
        let query = client.prepare_query(&query).unwrap();
        let stream = block_on(client.subscribe(&query, ReadOpts::default())).unwrap();
        Live {
            query,
            identity: AuthorSubject::SYSTEM,
            stream,
            rows: Vec::new(),
            includes_room: false,
        }
    })
    .collect();
    pump();
    let local = ReadOpts {
        tier: ReadTier::LocalOnly,
        ..ReadOpts::default()
    };
    let check = |live: &mut Vec<Live>, step: usize| {
        for sub in live.iter_mut() {
            apply(sub);
            let fresh = block_on(client.all(&sub.query, local.clone())).unwrap();
            assert_eq!(sub.rows, snapshot(&fresh), "diverged after step {step}");
        }
    };
    check(&mut live, 0);
    let before = client.fanout_stats();
    let mut rng = Rng(5);
    let mut next = 20;
    for step in 1..=120 {
        let on = if rng.next(2) == 0 { &client } else { &worker };
        let n = rng.next(next);
        match rng.next(4) {
            0 => {
                block_on(on.insert(
                    "tasks",
                    task_cells(next, rng.next(2) == 0),
                    InsertOptions {
                        row_id: Some(task(next)),
                        ..Default::default()
                    },
                ))
                .unwrap();
                next += 1;
            }
            1 => {
                let _ = block_on(on.delete("tasks", task(n), DeleteOptions::default()));
            }
            _ => {
                let _ = block_on(on.update(
                    "tasks",
                    task(n),
                    task_cells(n + rng.next(5), rng.next(2) == 0),
                    UpdateOptions::default(),
                ));
            }
        }
        pump();
        check(&mut live, step);
    }
    let after = client.fanout_stats();
    assert!(
        after.rechecked - before.rechecked > 10 * (after.reran - before.reran),
        "synced groups re-check written rows: {before:?} -> {after:?}"
    );
}
