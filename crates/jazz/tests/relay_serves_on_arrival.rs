//! A relay ticked only when something arrives (such as the native relay's
//! owner) serves what the upstream sent in the same tick that applies it:
//! its subscribers never wait for an unrelated later tick.

use crate::common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;

use duplex_transport::duplex;
use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, SubscriptionEvent, block_on};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

fn open(node: u8) -> Db {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("title", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap()
}

fn insert(db: &Db, title: &str) {
    block_on(db.insert(
        "todos",
        BTreeMap::from([("title".to_owned(), Value::String(title.to_owned()))]),
        Default::default(),
    ))
    .unwrap();
}

fn added(stream: &mut jazz::db::SubscriptionStream) -> usize {
    let mut rows = 0;
    while let Some(event) = stream.try_next_event() {
        if let SubscriptionEvent::Delta { added, .. } = event {
            rows += added.len();
        }
    }
    rows
}

/// A device's relay (such as the native relay's owner) ticks only
/// when frames arrive. Core's answer to the tab's subscription, and a later
/// live row, must reach the tab after one relay tick each, not wait for a
/// tick nothing asks for. Before, the relay applied the answer after its
/// serving pass and the tab saw nothing until some other event ticked it.
///
/// ```
/// tab ──subscribe──► relay ──subscribe──► core
///                                           │
/// tab ◄──rows── relay (one tick) ◄──rows────┘
/// ```
#[test]
fn a_relay_serves_upstream_rows_in_the_tick_that_applies_them() {
    let core = open(0xc0);
    let relay = open(0x10);
    let tab = open(0x11);
    tab.set_non_durable_client();
    let (client_side, server_side) = duplex();
    block_on(relay.connect_upstream(client_side));
    core.accept_subscriber(server_side, AuthorSubject::SYSTEM)
        .unwrap();
    let (client_side, server_side) = duplex();
    block_on(tab.connect_upstream(client_side));
    relay
        .accept_subscriber(server_side, AuthorSubject::SYSTEM)
        .unwrap();
    insert(&core, "first");

    let query = tab.prepare_query(&Query::from("todos")).unwrap();
    let mut stream = block_on(tab.subscribe(&query, ReadOpts::default())).unwrap();
    // Each node ticks once, when its peer's frames arrive.
    block_on(tab.tick()).unwrap();
    block_on(relay.tick()).unwrap();
    block_on(core.tick()).unwrap();
    block_on(relay.tick()).unwrap();
    block_on(tab.tick()).unwrap();
    assert_eq!(
        added(&mut stream),
        1,
        "the subscription opens with Core's row"
    );

    // A live change takes the same single pass.
    insert(&core, "second");
    block_on(core.tick()).unwrap();
    block_on(relay.tick()).unwrap();
    block_on(tab.tick()).unwrap();
    assert_eq!(added(&mut stream), 1, "Core's next row reaches the tab");
}
