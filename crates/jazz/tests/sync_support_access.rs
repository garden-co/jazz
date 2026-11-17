//! A supporting row's readability can change without the row changing: a
//! room included by a message becomes unreadable when the reader's
//! membership is deleted. The replica must lose it (and get it back when
//! access returns).

use crate::common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;

use duplex_transport::duplex;
use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, ReadTier, SubscriptionEvent, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{ArraySubquery, OrderDirection, Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    ColumnType, Operation, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder,
};

use common::{
    allow_all_policies, compile_schema, exists, outer_eq, read_and_allow_all_writes, session_eq,
};

const ME: [&str; 2] = ["identity", "subject"];

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
    server.accept_subscriber(server_side, alice()).unwrap();
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
        [id(1, 2)],
        "the room she can no longer read is evicted (INV-SYNC-14)"
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
        server.accept_subscriber(server_side, *user).unwrap();
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

/// A one-shot `Remote` read after access was lost leaves out the cached rows
/// the server's answer no longer lists, in an ordered window or not, though
/// no subscription held them to see them leave: here the reader wrote them.
/// The replica drops them (INV-SYNC-14).
#[test]
fn a_remote_read_after_losing_access_evicts_the_cached_rows() {
    for window in [None, Some((1, 2))] {
        let server = open(0, AuthorSubject::SYSTEM);
        let me = Value::String(alice().principal_parts().1);
        for n in 1..=4 {
            server
                .seed_settled_mergeable_for_bootstrap(
                    "memberships",
                    id(2, n),
                    AuthorSubject::SYSTEM,
                    cells(&[("room", Value::Uuid(id(1, n).0)), ("member", me.clone())]),
                )
                .unwrap();
        }
        let client = open(1, alice());
        let (client_side, server_side) = duplex();
        let _upstream = block_on(client.connect_upstream(client_side));
        server.accept_subscriber(server_side, alice()).unwrap();
        client.set_remote_link_hint(jazz::db::RemoteLinkHint::Live);
        for n in 1..=4 {
            block_on(client.insert(
                "rooms",
                cells(&[("name", Value::String(format!("room {n}")))]),
                InsertOptions {
                    row_id: Some(id(1, n)),
                    ..InsertOptions::default()
                },
            ))
            .unwrap();
        }
        pump(&server, &client);

        let mut query = Query::from("rooms").order_by("name", OrderDirection::Asc);
        if let Some((offset, limit)) = window {
            query.offset = offset;
            query.limit = Some(limit);
        }
        let bytes = postcard::to_allocvec(&query).unwrap();
        let remote = || {
            let read = client.all_serialized_query(
                &bytes,
                ReadOpts::for_read_tier(ReadTier::Remote),
                None,
                None,
                None,
            );
            let mut read = std::pin::pin!(read);
            let mut context = std::task::Context::from_waker(std::task::Waker::noop());
            for _ in 0..50 {
                if let std::task::Poll::Ready(result) = read.as_mut().poll(&mut context) {
                    let jazz::db::SerializedReadResult::Rows(rows) = result.unwrap() else {
                        panic!("the remote read answered with something other than rows");
                    };
                    let mut ids: Vec<RowUuid> = rows.iter().map(|row| row.row_uuid()).collect();
                    ids.sort();
                    return ids;
                }
                pump(&server, &client);
            }
            panic!("the remote read did not answer");
        };

        for n in 1..=4 {
            block_on(server.delete("memberships", id(2, n), Default::default())).unwrap();
        }
        pump(&server, &client);
        assert_eq!(remote(), Vec::<RowUuid>::new(), "window {window:?}");
        assert_eq!(
            local(&client, "rooms"),
            Vec::<RowUuid>::new(),
            "window {window:?}"
        );
    }
}

/// Through a relay, a `Remote` read by id after the reader lost access leaves
/// the row out: the relay serves the upstream's answer, not its cached copy.
#[test]
fn a_remote_read_through_an_edge_after_losing_access_leaves_the_row_out() {
    let server = open(0, AuthorSubject::SYSTEM);
    let me = Value::String(alice().principal_parts().1);
    server
        .seed_settled_mergeable_for_bootstrap(
            "rooms",
            id(1, 1),
            AuthorSubject::SYSTEM,
            cells(&[("name", Value::String("room 1".into()))]),
        )
        .unwrap();
    server
        .seed_settled_mergeable_for_bootstrap(
            "memberships",
            id(2, 1),
            AuthorSubject::SYSTEM,
            cells(&[("room", Value::Uuid(id(1, 1).0)), ("member", me)]),
        )
        .unwrap();
    // The reader's own durable store relays for its foreground, as a
    // browser worker does for its tabs.
    let edge = open(5, alice());
    let (edge_side, server_side) = duplex();
    let _edge_upstream = block_on(edge.connect_upstream(edge_side));
    server.accept_subscriber(server_side, alice()).unwrap();
    edge.set_remote_link_hint(jazz::db::RemoteLinkHint::Live);
    let client = open(1, alice());
    let (client_side, edge_client_side) = duplex();
    let _upstream = block_on(client.connect_upstream(client_side));
    edge.accept_subscriber(edge_client_side, alice()).unwrap();
    client.set_remote_link_hint(jazz::db::RemoteLinkHint::Live);
    let pump_all = || {
        for _ in 0..4 {
            block_on(client.tick()).unwrap();
            block_on(edge.tick()).unwrap();
            block_on(server.tick()).unwrap();
            block_on(edge.tick()).unwrap();
        }
        block_on(client.tick()).unwrap();
    };
    let query = Query::from("rooms").filter(eq(col("id"), lit(Value::Uuid(id(1, 1).0))));
    let bytes = postcard::to_allocvec(&query).unwrap();
    let remote = || {
        let read = client.all_serialized_query(
            &bytes,
            ReadOpts::for_read_tier(ReadTier::Remote),
            None,
            None,
            None,
        );
        let mut read = std::pin::pin!(read);
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        for _ in 0..50 {
            if let std::task::Poll::Ready(result) = read.as_mut().poll(&mut context) {
                let jazz::db::SerializedReadResult::Rows(rows) = result.unwrap() else {
                    panic!("the remote read answered with something other than rows");
                };
                return rows.iter().map(|row| row.row_uuid()).collect::<Vec<_>>();
            }
            pump_all();
        }
        panic!("the remote read did not answer");
    };
    assert_eq!(remote(), [id(1, 1)]);
    block_on(server.delete("memberships", id(2, 1), Default::default())).unwrap();
    pump_all();
    assert_eq!(remote(), Vec::<RowUuid>::new());
}

fn inherited_schema() -> JazzSchema {
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
                    .policies(TablePolicies::new().with_select(PolicyExpr::Inherits {
                        operation: Operation::Select,
                        via_column: "room".to_owned(),
                        max_depth: None,
                    })),
            )
            .build(),
    )
}

#[test]
fn a_replica_holds_the_parent_its_rows_inherit_read_access_from() {
    let open = |node: u8, author: AuthorSubject| {
        let config = DbConfig::new(
            inherited_schema(),
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
    };
    let server = open(0, AuthorSubject::SYSTEM);
    let seed = |table: &str, row: RowUuid, values: BTreeMap<String, Value>| {
        server
            .seed_settled_mergeable_for_bootstrap(table, row, AuthorSubject::SYSTEM, values)
            .unwrap();
    };
    let me = Value::String(alice().principal_parts().1);
    seed(
        "rooms",
        id(1, 1),
        cells(&[("name", Value::String("room".into()))]),
    );
    seed(
        "memberships",
        id(2, 1),
        cells(&[("room", Value::Uuid(id(1, 1).0)), ("member", me)]),
    );
    seed(
        "messages",
        id(3, 1),
        cells(&[
            ("room", Value::Uuid(id(1, 1).0)),
            ("body", Value::String("m".into())),
        ]),
    );
    let client = open(1, alice());
    let (client_side, server_side) = duplex();
    let _upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice()).unwrap();
    let messages = client.prepare_query(&Query::from("messages")).unwrap();
    let _stream = block_on(client.subscribe(&messages, ReadOpts::default())).unwrap();
    pump(&server, &client);
    assert_eq!(local(&client, "messages"), [id(3, 1)]);
    assert_eq!(
        local(&client, "rooms"),
        [id(1, 1)],
        "the room her messages inherit read access from ships with them"
    );

    block_on(server.delete("memberships", id(2, 1), Default::default())).unwrap();
    pump(&server, &client);
    assert_eq!(
        local(&client, "rooms"),
        Vec::<RowUuid>::new(),
        "the room she can no longer read is evicted (INV-SYNC-14)"
    );
}

/// A read of rows that inherit read access from a parent also ships the
/// rows the parent's own read policy consults: a replica (or a backend
/// reading for a request's session) that never read the parent's table
/// still answers the read (alpha.60 adopter pass V1).
#[test]
fn a_cold_remote_read_through_an_inherited_parent_ships_what_the_parent_policy_consults() {
    let open = |node: u8, author: AuthorSubject| {
        let config = DbConfig::new(
            inherited_schema(),
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
    };
    let server = open(0, AuthorSubject::SYSTEM);
    let seed = |table: &str, row: RowUuid, values: BTreeMap<String, Value>| {
        server
            .seed_settled_mergeable_for_bootstrap(table, row, AuthorSubject::SYSTEM, values)
            .unwrap();
    };
    let me = Value::String(alice().principal_parts().1);
    seed(
        "rooms",
        id(1, 1),
        cells(&[("name", Value::String("room".into()))]),
    );
    seed(
        "memberships",
        id(2, 1),
        cells(&[("room", Value::Uuid(id(1, 1).0)), ("member", me)]),
    );
    seed(
        "messages",
        id(3, 1),
        cells(&[
            ("room", Value::Uuid(id(1, 1).0)),
            ("body", Value::String("m".into())),
        ]),
    );
    // A fresh replica that has read nothing yet: no rooms, no memberships.
    let client = open(1, alice());
    let (client_side, server_side) = duplex();
    let _upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice()).unwrap();
    client.set_remote_link_hint(jazz::db::RemoteLinkHint::Live);
    let bytes = postcard::to_allocvec(&Query::from("messages")).unwrap();
    let read = client.all_serialized_query(
        &bytes,
        ReadOpts::for_read_tier(ReadTier::Remote),
        None,
        // Read for her session, as a backend reads for a request's user:
        // the replica evaluates her read policy over what it holds.
        Some((alice(), BTreeMap::new())),
        None,
    );
    let mut read = std::pin::pin!(read);
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    let mut rows = None;
    for _ in 0..50 {
        if let std::task::Poll::Ready(result) = read.as_mut().poll(&mut context) {
            let jazz::db::SerializedReadResult::Rows(answer) = result.unwrap() else {
                panic!("the remote read answered with something other than rows");
            };
            rows = Some(answer.iter().map(|row| row.row_uuid()).collect::<Vec<_>>());
            break;
        }
        pump(&server, &client);
    }
    assert_eq!(
        rows.expect("the remote read did not answer"),
        [id(3, 1)],
        "the message she reads through her room's membership"
    );
    assert_eq!(local(&client, "rooms"), [id(1, 1)]);
    assert_eq!(
        local(&client, "memberships"),
        [id(2, 1)],
        "the membership the room's read policy consults ships too"
    );
}

/// A backend writing for a request's user updates a row it read for that
/// user, whose update policy inherits through a parent the user reaches by
/// a membership: the read brought what the policy consults (alpha.60
/// adopter pass V16).
#[test]
fn a_backend_updates_for_its_user_through_an_inherited_parent_after_reading_the_row() {
    let schema = || {
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
                        .policies({
                            let inherit = PolicyExpr::Inherits {
                                operation: Operation::Select,
                                via_column: "room".to_owned(),
                                max_depth: None,
                            };
                            let mut policies = TablePolicies::new().with_select(inherit.clone());
                            policies.update.using = Some(inherit.clone());
                            policies.update.with_check = Some(inherit);
                            policies
                        }),
                )
                .build(),
        )
    };
    let open = |node: u8| {
        let config = DbConfig::new(
            schema(),
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
    };
    let server = open(0);
    let seed = |table: &str, row: RowUuid, values: BTreeMap<String, Value>| {
        server
            .seed_settled_mergeable_for_bootstrap(table, row, AuthorSubject::SYSTEM, values)
            .unwrap();
    };
    let me = Value::String(alice().principal_parts().1);
    seed(
        "rooms",
        id(1, 1),
        cells(&[("name", Value::String("room".into()))]),
    );
    seed(
        "memberships",
        id(2, 1),
        cells(&[("room", Value::Uuid(id(1, 1).0)), ("member", me)]),
    );
    seed(
        "messages",
        id(3, 1),
        cells(&[
            ("room", Value::Uuid(id(1, 1).0)),
            ("body", Value::String("m".into())),
        ]),
    );
    // A freshly started backend that has read nothing.
    let backend = open(1);
    let (client_side, server_side) = duplex();
    let _upstream = block_on(backend.connect_upstream(client_side));
    server
        .accept_subscriber(server_side, AuthorSubject::SYSTEM)
        .unwrap();
    backend.set_remote_link_hint(jazz::db::RemoteLinkHint::Live);
    // The handler reads the row for its user first, as an update needs the
    // current row; it never reads the room or the membership.
    let bytes = postcard::to_allocvec(&Query::from("messages")).unwrap();
    let read = backend.all_serialized_query(
        &bytes,
        ReadOpts::for_read_tier(ReadTier::Remote),
        None,
        Some((alice(), BTreeMap::new())),
        None,
    );
    let mut read = std::pin::pin!(read);
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    let mut answered = false;
    for _ in 0..50 {
        if let std::task::Poll::Ready(result) = read.as_mut().poll(&mut context) {
            let jazz::db::SerializedReadResult::Rows(rows) = result.unwrap() else {
                panic!("the remote read answered with something other than rows");
            };
            assert_eq!(rows.len(), 1, "she reads the message through her room");
            answered = true;
            break;
        }
        pump(&server, &backend);
    }
    assert!(answered, "the remote read did not answer");
    let update = backend.update(
        "messages",
        id(3, 1),
        cells(&[("body", Value::String("edited".into()))]),
        jazz::db::UpdateOptions {
            identity: jazz::db::WriteIdentity::Session(alice()),
            ..Default::default()
        },
    );
    let mut update = std::pin::pin!(update);
    let mut result = None;
    for _ in 0..50 {
        if let std::task::Poll::Ready(outcome) = update.as_mut().poll(&mut context) {
            result = Some(outcome);
            break;
        }
        pump(&server, &backend);
    }
    result
        .expect("the update did not finish")
        .expect("alice may update the message through her room");
    pump(&server, &backend);
    let prepared = server.prepare_query(&Query::from("messages")).unwrap();
    let rows = block_on(server.all(&prepared, ReadOpts::default())).unwrap();
    assert_eq!(rows[0].cell("body"), Some(Value::String("edited".into())));
}

fn enumeration(variants: &[&str]) -> ColumnType {
    ColumnType::Enum {
        variants: variants.iter().map(|v| (*v).to_owned()).collect(),
    }
}

/// An authority, a relay subscribed to it and a client subscribed to the
/// relay, all as alice (the authority as system), on one schema.
///
/// ```text
/// server ──► relay ──► client
/// ```
struct Relayed {
    server: Db,
    relay: Db,
    client: Db,
    _links: Vec<jazz::db::Upstream>,
}

impl Relayed {
    fn open(schema: impl Fn() -> JazzSchema) -> Self {
        let open = |node: u8, author: AuthorSubject| {
            let config = DbConfig::new(
                schema(),
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
        };
        let server = open(0, AuthorSubject::SYSTEM);
        let relay = open(5, alice());
        let (relay_side, server_side) = duplex();
        let relay_upstream = block_on(relay.connect_upstream(relay_side));
        server.accept_subscriber(server_side, alice()).unwrap();
        relay.set_remote_link_hint(jazz::db::RemoteLinkHint::Live);
        let client = open(1, alice());
        let (client_side, relay_client_side) = duplex();
        let client_upstream = block_on(client.connect_upstream(client_side));
        relay.accept_subscriber(relay_client_side, alice()).unwrap();
        client.set_remote_link_hint(jazz::db::RemoteLinkHint::Live);
        Self {
            server,
            relay,
            client,
            _links: vec![relay_upstream, client_upstream],
        }
    }

    fn seed(&self, table: &str, row: RowUuid, values: BTreeMap<String, Value>) {
        self.server
            .seed_settled_mergeable_for_bootstrap(table, row, AuthorSubject::SYSTEM, values)
            .unwrap();
    }

    fn pump(&self) {
        for _ in 0..4 {
            block_on(self.client.tick()).unwrap();
            block_on(self.relay.tick()).unwrap();
            block_on(self.server.tick()).unwrap();
            block_on(self.relay.tick()).unwrap();
        }
        block_on(self.client.tick()).unwrap();
    }

    /// Subscribe the client to `query`; the subscription lives as long as
    /// the returned stream.
    fn subscribe(&self, query: Query) -> impl Sized {
        let prepared = self.client.prepare_query(&query).unwrap();
        let stream = block_on(self.client.subscribe(&prepared, ReadOpts::default())).unwrap();
        self.pump();
        stream
    }
}

fn level(name: &str) -> Value {
    Value::String(name.to_owned())
}

/// `documents` readable through a matching grant, correlated on `level`
/// (two enum columns declaring their variants in opposite orders, so each
/// stores the same variant as a different tag) and on the `team` they
/// reference. `level_first` puts the enum equality first, which makes it
/// the one support follows.
fn graded_schema(level_first: bool) -> JazzSchema {
    let level = outer_eq("level", "level");
    let team = outer_eq("team", "team");
    let correlation = if level_first {
        vec![level, team, session_eq("user", &ME)]
    } else {
        vec![team, level, session_eq("user", &ME)]
    };
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("teams")
                    .column("name", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("grants")
                    .column("level", enumeration(&["low", "high"]))
                    .fk_column("team", "teams")
                    .column("user", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("documents")
                    .column("level", enumeration(&["high", "low"]))
                    .fk_column("team", "teams")
                    .column("title", ColumnType::Text)
                    .policies(read_and_allow_all_writes(exists("grants", correlation))),
            )
            .build(),
    )
}

/// A team, alice's grant `grants[1]` at `high` and two documents of the
/// team, `documents[1]` at `high` and `documents[2]` at `low`.
fn seed_graded(at: &Relayed) -> (Value, Value) {
    at.seed(
        "teams",
        id(9, 1),
        cells(&[("name", Value::String("team".into()))]),
    );
    let team = Value::Uuid(id(9, 1).0);
    let me = Value::String(alice().principal_parts().1);
    at.seed(
        "grants",
        id(1, 1),
        cells(&[
            ("level", level("high")),
            ("team", team.clone()),
            ("user", me.clone()),
        ]),
    );
    for (n, name) in [(1, "high"), (2, "low")] {
        at.seed(
            "documents",
            id(2, n),
            cells(&[
                ("level", level(name)),
                ("team", team.clone()),
                ("title", Value::String(format!("doc {n}"))),
            ]),
        );
    }
    (team, me)
}

/// A policy whose first correlation compares two enum columns that store
/// the same variant as different tags still ships what it reads: support
/// follows the grant by its variant, not by the document's raw tag, so the
/// relay can evaluate the policy for alice.
///
/// ```text
/// server ──grants + documents──► relay ──documents──► alice
///          (level = "high": tag 1 in grants, tag 0 in documents)
/// ```
#[test]
fn a_correlation_on_differently_stored_enum_columns_follows_its_rows_through_a_relay() {
    for level_first in [true, false] {
        let at = Relayed::open(|| graded_schema(level_first));
        let (team, me) = seed_graded(&at);
        let _documents = at.subscribe(Query::from("documents"));
        assert_eq!(
            local(&at.client, "documents"),
            [id(2, 1)],
            "level_first {level_first}: the document the grant matches"
        );
        assert_eq!(
            local(&at.relay, "grants"),
            [id(1, 1)],
            "level_first {level_first}: the grant the relay evaluates alice's read with"
        );
        assert_eq!(
            local(&at.client, "grants"),
            [id(1, 1)],
            "level_first {level_first}: the grant the document's read rests on"
        );

        // A later grant for the other level opens the other document, which
        // the relay ships with the grant its read rests on.
        at.seed(
            "grants",
            id(1, 2),
            cells(&[("level", level("low")), ("team", team), ("user", me)]),
        );
        at.pump();
        assert_eq!(
            local(&at.client, "documents"),
            [id(2, 1), id(2, 2)],
            "level_first {level_first}: a later grant opens the other document"
        );
    }
}

/// A support row written after the follow reaches the relay through the
/// followed key: alice's grant is replaced by another for the same level,
/// and the relay holds the new one, so the document stays readable there.
///
/// ```text
/// server: grant 1 (high) ──► relay
///         grant 3 (high) ──► relay   (written later, same key)
///         grant 1 deleted ──► relay  holds grant 3, document stays
/// ```
#[test]
fn a_replaced_grant_on_an_enum_correlation_reaches_the_relay() {
    for level_first in [true, false] {
        let at = Relayed::open(|| graded_schema(level_first));
        let (team, me) = seed_graded(&at);
        let _documents = at.subscribe(Query::from("documents"));
        at.seed(
            "grants",
            id(1, 3),
            cells(&[("level", level("high")), ("team", team), ("user", me)]),
        );
        at.pump();
        block_on(at.server.delete("grants", id(1, 1), Default::default())).unwrap();
        at.pump();
        assert_eq!(
            local(&at.relay, "grants"),
            [id(1, 3)],
            "level_first {level_first}: the replacing grant reached the relay"
        );
        assert_eq!(
            local(&at.client, "documents"),
            [id(2, 1)],
            "level_first {level_first}: the document stays readable"
        );
    }
}

/// `staff` readable by a member of the same role: the policy correlates the
/// enum column `role` with itself.
fn staff_schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("staff")
                    .column("role", enumeration(&["admin", "guest"]))
                    .column("user", ColumnType::Text)
                    .policies(read_and_allow_all_writes(exists(
                        "staff",
                        vec![outer_eq("role", "role"), session_eq("user", &ME)],
                    ))),
            )
            .build(),
    )
}

/// An enum column correlated with itself follows its rows too: alice reads
/// bob's admin row through her own, which the relay holds though her query
/// does not select it.
#[test]
fn a_correlation_of_an_enum_column_with_itself_follows_its_rows_through_a_relay() {
    let at = Relayed::open(staff_schema);
    let me = Value::String(alice().principal_parts().1);
    let bob = Value::String("bob".into());
    let row = |role: &str, user: &Value| cells(&[("role", level(role)), ("user", user.clone())]);
    at.seed("staff", id(1, 1), row("admin", &me));
    at.seed("staff", id(1, 2), row("admin", &bob));
    at.seed("staff", id(1, 3), row("guest", &bob));
    let _bobs = at.subscribe(Query::from("staff").filter(eq(col("user"), lit(bob))));
    assert_eq!(local(&at.client, "staff"), [id(1, 1), id(1, 2)]);
    assert_eq!(local(&at.relay, "staff"), [id(1, 1), id(1, 2)]);
}

/// `grants` and `documents` everyone reads, `level` stored with opposite
/// tags in each, and `documents.levels` an array of the same variants.
fn catalog_schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("grants")
                    .column("level", enumeration(&["low", "high"]))
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("documents")
                    .column("level", enumeration(&["high", "low"]))
                    .column(
                        "levels",
                        ColumnType::Array {
                            element: Box::new(enumeration(&["high", "low"])),
                        },
                    )
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

/// `grants[1]` at `high`, `grants[2]` at `low`, and one document at `high`
/// whose `levels` hold `high` only.
fn seed_catalog(at: &Relayed) {
    at.seed("grants", id(1, 1), cells(&[("level", level("high"))]));
    at.seed("grants", id(1, 2), cells(&[("level", level("low"))]));
    at.seed(
        "documents",
        id(2, 1),
        cells(&[
            ("level", level("high")),
            ("levels", Value::Array(vec![level("high")])),
        ]),
    );
}

/// Subscribe the client to `query` over [`seed_catalog`]'s rows through a
/// relay: the relay holds the document and exactly the grant at its level,
/// and so does the client when `at_client`.
fn relation_follows_by_value(query: Query, at_client: bool) {
    let at = Relayed::open(catalog_schema);
    seed_catalog(&at);
    let _documents = at.subscribe(query);
    assert_eq!(local(&at.relay, "documents"), [id(2, 1)]);
    assert_eq!(
        local(&at.relay, "grants"),
        [id(1, 1)],
        "the grant at `high`"
    );
    if at_client {
        assert_eq!(local(&at.client, "documents"), [id(2, 1)]);
        assert_eq!(
            local(&at.client, "grants"),
            [id(1, 1)],
            "the grant at `high`"
        );
    }
}

/// An array subquery correlating differently stored enum columns follows
/// its children by value.
#[test]
fn an_array_subquery_on_differently_stored_enum_columns_follows_its_rows_through_a_relay() {
    relation_follows_by_value(
        Query::from("documents")
            .array_subquery(ArraySubquery::new("grants", "grants", "level", "level")),
        true,
    );
}

/// A forward array relation from an array of enum values follows each
/// element by value.
#[test]
fn a_forward_array_relation_of_enum_values_follows_its_rows_through_a_relay() {
    relation_follows_by_value(
        Query::from("documents")
            .array_subquery(ArraySubquery::new("grants", "grants", "level", "levels")),
        true,
    );
}

/// A junction join correlating differently stored enum columns ships the
/// joined rows by value. The join's own SQL still compares the two
/// columns' raw cells (jazz#6044), so only the support the relay holds is
/// asserted.
#[test]
fn a_junction_join_on_differently_stored_enum_columns_follows_its_rows_through_a_relay() {
    relation_follows_by_value(
        Query::from("documents").join_via_column("grants", "level", "level", []),
        false,
    );
}
