//! A supporting row's readability can change without the row changing: a
//! room included by a message becomes unreadable when the reader's
//! membership is deleted. The replica must lose it (and get it back when
//! access returns). Not ported from Jazz.

mod common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;

use duplex_transport::duplex;
use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, ReadTier, SubscriptionEvent, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{
    allow_all_policies, compile_schema, exists, outer_eq, read_and_allow_all_writes, session_eq,
};

const ME: [&str; 3] = ["user", "identity", "subject"];

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("rooms")
                    .column("name", ColumnType::Text)
                    .policies(read_and_allow_all_writes(exists(
                        "memberships",
                        vec![outer_eq("room", "id"), session_eq("member", &ME)],
                    ))),
            )
            .table(
                TableSchemaBuilder::new("memberships")
                    .fk_column("room", "rooms")
                    .column("member", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("messages")
                    .fk_column("room", "rooms")
                    .column("body", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([0x55; 16])
}

fn open(node: u8, author: AuthorSubject) -> Db {
    let schema = schema();
    let config = DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    );
    if node == 0 {
        block_on(Db::open_history_complete(config))
    } else {
        block_on(Db::open(config))
    }
    .unwrap()
}

fn id(kind: u8, n: u8) -> RowUuid {
    RowUuid::from_bytes([kind, n, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
}

fn cells(pairs: &[(&str, Value)]) -> BTreeMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect()
}

fn local(db: &Db, table: &str) -> Vec<RowUuid> {
    let prepared = db.prepare_query(&Query::from(table)).unwrap();
    let opts = ReadOpts {
        tier: ReadTier::LocalOnly,
        ..ReadOpts::default()
    };
    let mut ids: Vec<RowUuid> = block_on(db.all(&prepared, opts))
        .unwrap()
        .iter()
        .map(|row| row.row_uuid())
        .collect();
    ids.sort();
    ids
}

fn pump(server: &Db, client: &Db) {
    for _ in 0..4 {
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    block_on(client.tick()).unwrap();
}

#[test]
fn losing_and_regaining_access_to_an_include_target_reaches_the_replica() {
    let server = open(0, AuthorSubject::SYSTEM);
    let seed = |table: &str, row: RowUuid, values: BTreeMap<String, Value>| {
        server
            .seed_settled_mergeable_for_bootstrap(table, row, AuthorSubject::SYSTEM, values)
            .unwrap();
    };
    let me = Value::String(alice().principal_parts().1);
    for n in 1..=2 {
        seed(
            "rooms",
            id(1, n),
            cells(&[("name", Value::String(format!("room {n}")))]),
        );
        seed(
            "memberships",
            id(2, n),
            cells(&[("room", Value::Uuid(id(1, n).0)), ("member", me.clone())]),
        );
        seed(
            "messages",
            id(3, n),
            cells(&[
                ("room", Value::Uuid(id(1, n).0)),
                ("body", Value::String(format!("m{n}"))),
            ]),
        );
    }
    let client = open(1, alice());
    let (client_side, server_side) = duplex();
    let _upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice());
    let prepared = client
        .prepare_query(&Query::from("messages").include("room"))
        .unwrap();
    let _stream = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
    pump(&server, &client);
    assert_eq!(local(&client, "rooms"), [id(1, 1), id(1, 2)]);

    // Alice leaves room 1. Neither the room nor any message changes.
    block_on(server.delete("memberships", id(2, 1), Default::default())).unwrap();
    pump(&server, &client);
    assert_eq!(
        local(&client, "rooms"),
        [id(1, 1), id(1, 2)],
        "the room she can no longer read stays readable locally (INV-SYNC-14)"
    );
    let rooms = client.prepare_query(&Query::from("rooms")).unwrap();
    let mut synced = block_on(client.subscribe(&rooms, ReadOpts::default())).unwrap();
    pump(&server, &client);
    let mut listed = Vec::new();
    while let Some(event) = synced.try_next_event() {
        if let SubscriptionEvent::Delta { added, .. } = event {
            listed.extend(added.iter().map(|row| row.row_uuid()));
        }
    }
    assert_eq!(
        listed,
        [id(1, 2)],
        "a synced read follows the server's results"
    );
    assert_eq!(local(&client, "messages").len(), 2, "public messages stay");

    // She rejoins: the unchanged room comes back.
    seed(
        "memberships",
        id(2, 9),
        cells(&[("room", Value::Uuid(id(1, 1).0)), ("member", me.clone())]),
    );
    pump(&server, &client);
    assert_eq!(
        local(&client, "rooms"),
        [id(1, 1), id(1, 2)],
        "regained access ships the room again"
    );

    // An unrelated membership write changes nothing.
    seed(
        "memberships",
        id(2, 10),
        cells(&[
            ("room", Value::Uuid(id(1, 2).0)),
            ("member", Value::String("someone else".into())),
        ]),
    );
    pump(&server, &client);
    assert_eq!(local(&client, "rooms"), [id(1, 1), id(1, 2)]);
}

/// Timing probe (run with `--release -- --nocapture`): 300 clients each hold
/// the rooms their public feed includes; one membership write makes the
/// server re-check held rooms.
#[test]
fn membership_write_cost_with_many_holders() {
    let server = open(0, AuthorSubject::SYSTEM);
    let seed = |table: &str, row: RowUuid, values: BTreeMap<String, Value>| {
        server
            .seed_settled_mergeable_for_bootstrap(table, row, AuthorSubject::SYSTEM, values)
            .unwrap();
    };
    let users: Vec<AuthorSubject> = (0..300u32)
        .map(|n| {
            let mut bytes = [0x60u8; 16];
            bytes[12..].copy_from_slice(&n.to_be_bytes());
            AuthorSubject::for_test_bytes(bytes)
        })
        .collect();
    for r in 0..20u8 {
        seed(
            "rooms",
            id(1, r),
            cells(&[("name", Value::String(format!("room {r}")))]),
        );
        seed(
            "messages",
            id(3, r),
            cells(&[
                ("room", Value::Uuid(id(1, r).0)),
                ("body", Value::String(format!("m{r}"))),
            ]),
        );
    }
    let membership = |server: &Db, user: &AuthorSubject, room: u8| {
        let row = RowUuid(uuid::Uuid::new_v4());
        server
            .seed_settled_mergeable_for_bootstrap(
                "memberships",
                row,
                AuthorSubject::SYSTEM,
                cells(&[
                    ("room", Value::Uuid(id(1, room).0)),
                    ("member", Value::String(user.principal_parts().1)),
                ]),
            )
            .unwrap();
    };
    let mut clients = Vec::new();
    for (i, user) in users.iter().enumerate() {
        for k in 0..3u8 {
            membership(&server, user, ((i + usize::from(k) * 7) % 20) as u8);
        }
        let client = open(1, *user);
        let (client_side, server_side) = duplex();
        let upstream = block_on(client.connect_upstream(client_side));
        server.accept_subscriber(server_side, *user);
        let prepared = client
            .prepare_query(&Query::from("messages").include("room"))
            .unwrap();
        let stream = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
        clients.push((client, upstream, stream));
    }
    for _ in 0..4 {
        for (client, ..) in &clients {
            block_on(client.tick()).unwrap();
        }
        block_on(server.tick()).unwrap();
    }
    let mut times = Vec::new();
    for round in 0..20usize {
        membership(&server, &users[round], 19);
        let started = std::time::Instant::now();
        block_on(server.tick()).unwrap();
        times.push(started.elapsed());
    }
    times.sort();
    eprintln!(
        "server tick after one membership write, 300 clients holding 3 rooms each: median {:?}",
        times[times.len() / 2]
    );
}
