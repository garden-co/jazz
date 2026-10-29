//! Synced reads that span tables: the server ships a remote query's
//! supporting rows (include targets, array children) with its result, and
//! keeps them coherent on the replica through per-session watches. Shapes
//! that read further still are refused with "not supported yet".

use crate::common;

use std::collections::BTreeMap;

use jazz::db::duplex;
use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, ReadTier, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{ArraySubquery, Query};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("users")
                    .column("name", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("messages")
                    .fk_column("senderId", "users")
                    .column("body", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("comments")
                    .fk_column("messageId", "messages")
                    .column("text", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn open(node: u8) -> Db {
    let schema = schema();
    let config = DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
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

fn text(s: &str) -> Value {
    Value::String(s.to_owned())
}

fn local(db: &Db, table: &str) -> BTreeMap<RowUuid, String> {
    let prepared = db.prepare_query(&Query::from(table)).unwrap();
    let opts = ReadOpts {
        tier: ReadTier::LocalOnly,
        ..ReadOpts::default()
    };
    block_on(db.all(&prepared, opts))
        .unwrap()
        .iter()
        .map(|row| {
            let label = row
                .cells()
                .find_map(|(name, value)| match (name, value) {
                    ("name" | "body" | "text", Some(Value::String(s))) => Some(s.clone()),
                    _ => None,
                })
                .unwrap_or_default();
            (row.row_uuid(), label)
        })
        .collect()
}

fn pump(server: &Db, client: &Db) {
    for _ in 0..4 {
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    block_on(client.tick()).unwrap();
}

fn setup() -> (Db, Db) {
    let server = open(0);
    for (n, name) in [(1, "ada"), (2, "bob"), (3, "cy")] {
        server
            .seed_settled_mergeable_for_bootstrap(
                "users",
                id(1, n),
                AuthorSubject::SYSTEM,
                cells(&[("name", text(name))]),
            )
            .unwrap();
    }
    for (n, sender) in [(1, 1), (2, 2)] {
        server
            .seed_settled_mergeable_for_bootstrap(
                "messages",
                id(2, n),
                AuthorSubject::SYSTEM,
                cells(&[
                    ("senderId", Value::Uuid(id(1, sender).0)),
                    ("body", text(&format!("m{n}"))),
                ]),
            )
            .unwrap();
    }
    server
        .seed_settled_mergeable_for_bootstrap(
            "comments",
            id(3, 1),
            AuthorSubject::SYSTEM,
            cells(&[("messageId", Value::Uuid(id(2, 1).0)), ("text", text("c1"))]),
        )
        .unwrap();
    let client = open(1);
    let (client_side, server_side) = duplex();
    let _ = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, AuthorSubject::SYSTEM);
    (server, client)
}

#[test]
fn include_targets_ship_with_the_result_and_stay_coherent() {
    let (server, client) = setup();
    let prepared = client
        .prepare_query(&Query::from("messages").include("senderId"))
        .unwrap();
    let _stream = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
    pump(&server, &client);
    let users = local(&client, "users");
    assert_eq!(
        users.values().cloned().collect::<Vec<_>>(),
        ["ada", "bob"],
        "only the senders, not every user"
    );

    // A write to a supporting row reaches the replica, though no result row changed.
    block_on(server.update(
        "users",
        id(1, 1),
        cells(&[("name", text("ada lovelace"))]),
        Default::default(),
    ))
    .unwrap();
    pump(&server, &client);
    assert_eq!(local(&client, "users")[&id(1, 1)], "ada lovelace");

    // A message that starts pointing at a new sender ships that sender.
    block_on(server.update(
        "messages",
        id(2, 2),
        cells(&[("senderId", Value::Uuid(id(1, 3).0))]),
        Default::default(),
    ))
    .unwrap();
    pump(&server, &client);
    assert_eq!(local(&client, "users")[&id(1, 3)], "cy");
}

#[test]
fn array_children_ship_and_new_children_follow() {
    let (server, client) = setup();
    let query = Query::from("messages").array_subquery(ArraySubquery::new(
        "comments",
        "comments",
        "messageId",
        "id",
    ));
    let prepared = client.prepare_query(&query).unwrap();
    let _stream = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
    pump(&server, &client);
    assert_eq!(
        local(&client, "comments")
            .values()
            .cloned()
            .collect::<Vec<_>>(),
        ["c1"]
    );

    // A child inserted later matches the correlation watch.
    server
        .seed_settled_mergeable_for_bootstrap(
            "comments",
            id(3, 2),
            AuthorSubject::SYSTEM,
            cells(&[("messageId", Value::Uuid(id(2, 2).0)), ("text", text("c2"))]),
        )
        .unwrap();
    pump(&server, &client);
    assert_eq!(local(&client, "comments").len(), 2);
    // And the client's own tree read sees it under its parent.
    let tree = block_on(client.all(
        &prepared,
        ReadOpts {
            tier: ReadTier::LocalOnly,
            ..ReadOpts::default()
        },
    ))
    .unwrap();
    assert_eq!(tree.len(), 2);
}

#[test]
fn shapes_reading_further_tables_are_refused_explicitly() {
    let (_server, client) = setup();
    let prepared = client.prepare_query(&Query::from("comments").include("messageId.senderId"));
    let error = match prepared {
        Err(error) => error,
        Ok(prepared) => match block_on(client.subscribe(&prepared, ReadOpts::default())) {
            Err(error) => error,
            Ok(_) => panic!("a multi-hop include must not sync silently"),
        },
    };
    assert!(
        error.message.contains("not supported yet"),
        "{}",
        error.message
    );
}

/// Every string inside a cell, nested arrays and records included.
fn strings(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::String(s) => out.push(s.clone()),
        Value::Array(items) => items.iter().for_each(|item| strings(item, out)),
        Value::Record(record) => record
            .to_values()
            .unwrap()
            .iter()
            .for_each(|item| strings(item, out)),
        Value::Nullable(Some(inner)) => strings(inner, out),
        _ => {}
    }
}

/// The nested strings of each root row the subscription last reported,
/// by root id.
fn nested_seen(
    stream: &mut jazz::db::SubscriptionStream,
    seen: &mut BTreeMap<RowUuid, Vec<String>>,
) {
    while let Some(event) = stream.try_next_event() {
        if let jazz::db::SubscriptionEvent::Delta {
            reset,
            added,
            updated,
            ..
        } = event
        {
            if reset {
                seen.clear();
            }
            for row in added.iter().chain(updated.iter()) {
                let mut out = Vec::new();
                for (_, value) in row.row.cells() {
                    if let Some(value) = value {
                        strings(value, &mut out);
                    }
                }
                seen.insert(row.row.row_uuid(), out);
            }
        }
    }
}

/// Nested arrays sync: the server ships every level's children and
/// watches each level's correlation, so children that join later (under a
/// middle row that itself joined later) reach the replica, and the
/// subscription reports them inside its root rows.
#[test]
fn nested_array_children_ship_at_every_level() {
    let (server, client) = setup();
    let query =
        Query::from("users").array_subquery(
            ArraySubquery::new("messages", "messages", "senderId", "id").nested(
                ArraySubquery::new("comments", "comments", "messageId", "id"),
            ),
        );
    let prepared = client.prepare_query(&query).unwrap();
    let mut stream = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
    pump(&server, &client);
    let mut seen = BTreeMap::new();
    nested_seen(&mut stream, &mut seen);
    assert_eq!(seen[&id(1, 1)], ["ada", "m1", "c1"]);
    assert_eq!(seen[&id(1, 2)], ["bob", "m2"]);
    assert_eq!(local(&client, "comments").len(), 1);

    // A grandchild under a shipped child.
    server
        .seed_settled_mergeable_for_bootstrap(
            "comments",
            id(3, 2),
            AuthorSubject::SYSTEM,
            cells(&[("messageId", Value::Uuid(id(2, 2).0)), ("text", text("c2"))]),
        )
        .unwrap();
    pump(&server, &client);
    nested_seen(&mut stream, &mut seen);
    assert_eq!(seen[&id(1, 2)], ["bob", "m2", "c2"]);

    // A child that joins later, then a grandchild under it.
    server
        .seed_settled_mergeable_for_bootstrap(
            "messages",
            id(2, 3),
            AuthorSubject::SYSTEM,
            cells(&[("senderId", Value::Uuid(id(1, 3).0)), ("body", text("m3"))]),
        )
        .unwrap();
    pump(&server, &client);
    server
        .seed_settled_mergeable_for_bootstrap(
            "comments",
            id(3, 3),
            AuthorSubject::SYSTEM,
            cells(&[("messageId", Value::Uuid(id(2, 3).0)), ("text", text("c3"))]),
        )
        .unwrap();
    pump(&server, &client);
    nested_seen(&mut stream, &mut seen);
    assert_eq!(seen[&id(1, 3)], ["cy", "m3", "c3"]);
    assert_eq!(local(&client, "comments").len(), 3);
}
