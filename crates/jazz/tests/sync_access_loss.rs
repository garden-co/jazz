//! A replica drops the rows its reader can no longer read (INV-SYNC-14),
//! however the loss reaches it: in the same server tick as the reader's own
//! write of the row, or through the reader's own write made offline, while
//! the rows it lost sit only in its store. Found by the simulation soak
//! (`sync_differential`).
//!
//! Also: the rows a self-referential read policy rests on (members of a
//! band read the band's members) ship in time linear in the band, not
//! exponential in the policy's nesting depth.

use crate::common;

use std::collections::BTreeMap;

use jazz::db::duplex;
use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, ReadTier, Upstream, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema, exists, outer_eq, session_eq};

const ME: [&str; 3] = ["user", "identity", "subject"];

/// Members read and post a room's messages.
fn schema() -> JazzSchema {
    let member = exists(
        "memberships",
        vec![outer_eq("room", "room"), session_eq("member", &ME)],
    );
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
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("messages")
                    .fk_column("room", "rooms")
                    .column("body", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(member.clone())
                            .with_insert(member)
                            .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                            .with_delete(PolicyExpr::True),
                    ),
            )
            .build(),
    )
}

fn user(n: u8) -> AuthorSubject {
    AuthorSubject::for_test_bytes([0x40 + n; 16])
}

fn subject(n: u8) -> Value {
    Value::String(user(n).principal_parts().1)
}

fn open(schema: JazzSchema, node: u8, author: AuthorSubject) -> Db {
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

fn seed(server: &Db, table: &str, row: RowUuid, values: BTreeMap<String, Value>) {
    server
        .seed_settled_mergeable_for_bootstrap(table, row, AuthorSubject::SYSTEM, values)
        .unwrap();
}

fn connect(server: &Db, client: &Db, who: AuthorSubject) -> Upstream {
    let (client_side, server_side) = duplex();
    let upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, who);
    upstream
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

fn pump(server: &Db, clients: &[&Db]) {
    for _ in 0..4 {
        for client in clients {
            block_on(client.tick()).unwrap();
        }
        block_on(server.tick()).unwrap();
    }
    for client in clients {
        block_on(client.tick()).unwrap();
    }
}

/// Ada posts to a room while Bo deletes her membership, and the server
/// applies both in one tick: her subscription's result gains the message
/// and loses it again. The server evicts it, and must not also confirm her
/// commit as holding it, which went out after the eviction and put the
/// message back in her replica.
#[test]
fn an_own_write_that_loses_access_in_the_same_tick_is_evicted() {
    let server = open(schema(), 0, AuthorSubject::SYSTEM);
    seed(&server, "rooms", id(1, 1), cells(&[("name", "r".into())]));
    seed(
        &server,
        "memberships",
        id(2, 1),
        cells(&[("room", Value::Uuid(id(1, 1).0)), ("member", subject(0))]),
    );
    let ada = open(schema(), 1, user(0));
    let bo = open(schema(), 2, user(1));
    let _ada_link = connect(&server, &ada, user(0));
    let _bo_link = connect(&server, &bo, user(1));
    let messages = ada.prepare_query(&Query::from("messages")).unwrap();
    let mut live = block_on(ada.subscribe(&messages, ReadOpts::default())).unwrap();
    let memberships = bo.prepare_query(&Query::from("memberships")).unwrap();
    let _bo_live = block_on(bo.subscribe(&memberships, ReadOpts::default())).unwrap();
    pump(&server, &[&ada, &bo]);

    block_on(ada.insert(
        "messages",
        cells(&[("room", Value::Uuid(id(1, 1).0)), ("body", "hi".into())]),
        InsertOptions {
            row_id: Some(id(3, 1)),
            ..Default::default()
        },
    ))
    .unwrap();
    block_on(bo.delete("memberships", id(2, 1), Default::default())).unwrap();
    // Both commits reach the server before it ticks: it accepts Ada's
    // (she was a member), then Bo's.
    block_on(ada.tick()).unwrap();
    block_on(bo.tick()).unwrap();
    block_on(server.tick()).unwrap();
    pump(&server, &[&ada, &bo]);

    assert_eq!(
        local(&ada, "messages"),
        Vec::<RowUuid>::new(),
        "Ada's replica drops the message she can no longer read"
    );
    let mut rows = Vec::new();
    while let Some(event) = live.try_next_event() {
        if let jazz::db::SubscriptionEvent::Delta {
            reset,
            added,
            removed,
            ..
        } = event
        {
            if reset {
                rows.clear();
            }
            rows.retain(|row| !removed.iter().any(|gone| gone.row_uuid == *row));
            rows.extend(added.iter().map(|row| row.row_uuid()));
        }
    }
    assert_eq!(rows, Vec::<RowUuid>::new(), "and so does her subscription");
}

/// Ada holds a room's messages, goes offline, and leaves the room. When she
/// reconnects, her replica declares the messages it holds before her
/// commit and her resumed subscription reach the server, so no result ever
/// lists them for the commit to take out: the server must re-check the
/// rows she declared when a table their read policy reads is written, as
/// it does the rows of her fetches and ended subscriptions.
#[test]
fn rows_a_reconnecting_replica_holds_are_evicted_by_its_own_offline_write() {
    let server = open(schema(), 0, AuthorSubject::SYSTEM);
    seed(&server, "rooms", id(1, 1), cells(&[("name", "r".into())]));
    seed(
        &server,
        "memberships",
        id(2, 1),
        cells(&[("room", Value::Uuid(id(1, 1).0)), ("member", subject(0))]),
    );
    for n in 1..=2 {
        seed(
            &server,
            "messages",
            id(3, n),
            cells(&[("room", Value::Uuid(id(1, 1).0)), ("body", "m".into())]),
        );
    }
    let ada = open(schema(), 1, user(0));
    let link = connect(&server, &ada, user(0));
    let messages = ada.prepare_query(&Query::from("messages")).unwrap();
    let _live = block_on(ada.subscribe(&messages, ReadOpts::default())).unwrap();
    let memberships = ada.prepare_query(&Query::from("memberships")).unwrap();
    let _mine = block_on(ada.subscribe(&memberships, ReadOpts::default())).unwrap();
    pump(&server, &[&ada]);
    assert_eq!(local(&ada, "messages"), [id(3, 1), id(3, 2)]);

    assert!(ada.detach_connection(&link));
    block_on(ada.delete("memberships", id(2, 1), Default::default())).unwrap();
    let _link = connect(&server, &ada, user(0));
    pump(&server, &[&ada]);

    assert_eq!(
        local(&ada, "messages"),
        Vec::<RowUuid>::new(),
        "the messages of the room she left are evicted (INV-SYNC-14)"
    );
}

/// Members of a band read the band's members: the policy's EXISTS consults
/// its own table, so the rows it rests on nest the same support at every
/// depth (DEFAULT_INHERITS_DEPTH). Following each member's support anew
/// visited (members)^depth rows; BandBook's fourth member took a minute of
/// server CPU to join.
#[test]
fn a_self_referential_read_policy_ships_its_support_in_linear_time() {
    // Exponential, this is 12^10 policy checks per holder and never ends;
    // linear, a few hundred. Run it on its own thread so a regression fails
    // here rather than hanging the suite.
    let (done, finished) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        ship_a_band_roster();
        let _ = done.send(());
    });
    match finished.recv_timeout(std::time::Duration::from_secs(60)) {
        Ok(()) => {}
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            panic!("shipping a 12-member band's roster did not finish in 60 s")
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            panic!("shipping a band's roster failed (see its panic above)")
        }
    }
}

fn ship_a_band_roster() {
    let schema = || {
        compile_schema(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("bands")
                        .column("name", ColumnType::Text)
                        .policies(allow_all_policies()),
                )
                .table(
                    TableSchemaBuilder::new("members")
                        .fk_column("band", "bands")
                        .column("account", ColumnType::Text)
                        .policies(
                            TablePolicies::new()
                                .with_select(PolicyExpr::or(vec![
                                    session_eq("account", &ME),
                                    exists(
                                        "members",
                                        vec![outer_eq("band", "band"), session_eq("account", &ME)],
                                    ),
                                ]))
                                .with_insert(PolicyExpr::True)
                                .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                                .with_delete(PolicyExpr::True),
                        ),
                )
                .build(),
        )
    };
    const MEMBERS: u8 = 12;
    let server = open(schema(), 0, AuthorSubject::SYSTEM);
    seed(&server, "bands", id(1, 1), cells(&[("name", "b".into())]));
    for n in 0..MEMBERS {
        seed(
            &server,
            "members",
            id(2, n),
            cells(&[("band", Value::Uuid(id(1, 1).0)), ("account", subject(n))]),
        );
    }
    let ada = open(schema(), 1, user(0));
    let _link = connect(&server, &ada, user(0));
    let members = ada.prepare_query(&Query::from("members")).unwrap();
    let _live = block_on(ada.subscribe(&members, ReadOpts::default())).unwrap();
    pump(&server, &[&ada]);
    // A newcomer's join re-follows the band's support for every holder.
    block_on(server.insert(
        "members",
        cells(&[
            ("band", Value::Uuid(id(1, 1).0)),
            ("account", subject(MEMBERS)),
        ]),
        InsertOptions {
            row_id: Some(id(2, MEMBERS)),
            ..Default::default()
        },
    ))
    .unwrap();
    pump(&server, &[&ada]);

    assert_eq!(local(&ada, "members").len(), usize::from(MEMBERS) + 1);
}
